/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.clickhouse;

import static org.apache.beam.sdk.io.clickhouse.ClickHouseReadValidationException.check;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import com.clickhouse.data.ClickHouseFormat;
import java.io.IOException;

/** Worker-side validation of the actual database partition column, independent of output schema. */
final class ClickHousePartitionValidator {
  private ClickHousePartitionValidator() {}

  static void validate(Client client, ClickHouseReadConfiguration configuration)
      throws IOException {
    String table = configuration.table();
    String partitionColumn = configuration.partitionColumn();
    if (table == null || partitionColumn == null) {
      throw new IllegalArgumentException(
          "Table and partition column are required for metadata validation");
    }
    QuerySettings settings =
        new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    try {
      ClickHouseReadResources.checkInterrupted();
      try (QueryResponse response =
              ClickHouseReadResources.await(
                  client.query(ClickHouseReadSql.describeTable(table), settings));
          ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
        boolean found = false;
        while (true) {
          ClickHouseReadResources.checkInterrupted();
          if (!reader.hasNext()) {
            break;
          }
          reader.next();
          String name = reader.getString("name");
          if (partitionColumn.equals(name)) {
            ClickHouseColumn actual = ClickHouseColumn.of(name, reader.getString("type"));
            validateColumn(actual);
            found = true;
          }
        }
        if (!found) {
          throw new ClickHouseReadValidationException(
              "Partition column '" + partitionColumn + "' is missing from ClickHouse table");
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("ClickHouse partition metadata validation interrupted", e);
    } catch (Exception e) {
      throw new IOException("Unable to read ClickHouse partition metadata", e);
    }
  }

  static void validateColumn(ClickHouseColumn column) {
    check(
        !column.isNullable(),
        "Partition column '%s' must not be nullable in ClickHouse",
        column.getColumnName());
    check(
        !column.isLowCardinality()
            && !column.isAggregateFunction()
            && (column.getDataType() == ClickHouseDataType.Int32
                || column.getDataType() == ClickHouseDataType.Int64),
        "Partition column '%s' must be a non-nullable Int32 or Int64 in ClickHouse (actual: %s)",
        column.getColumnName(),
        column.getOriginalTypeName());
  }
}
