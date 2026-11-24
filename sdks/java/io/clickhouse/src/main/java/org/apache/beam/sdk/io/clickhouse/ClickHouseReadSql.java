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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;

/** Worker-local SQL and bound values; never included in descriptor or diagnostic strings. */
final class ClickHouseReadSql {
  private final String sql;
  private final Map<String, Object> parameters;

  private ClickHouseReadSql(String sql, Map<String, Object> parameters) {
    this.sql = sql;
    this.parameters = Collections.unmodifiableMap(new LinkedHashMap<>(parameters));
  }

  String sql() {
    return sql;
  }

  Map<String, Object> parameters() {
    return parameters;
  }

  static ClickHouseReadSql forPartition(
      ClickHouseReadConfiguration configuration, ClickHouseReadPartition partition) {
    ClickHouseReadRange range = partition.range();
    String query = configuration.query();
    if (query != null) {
      checkArgument(range == null, "Query mode requires a single-query work item");
      return new ClickHouseReadSql(query, Collections.emptyMap());
    }
    String table = configuration.table();
    String column = configuration.partitionColumn();
    Schema schema = configuration.schema();
    if (range == null || table == null || column == null || schema == null) {
      throw new IllegalArgumentException("Table mode requires a range work item");
    }
    String fields =
        schema.getFields().stream()
            .map(field -> quoteIdentifier(field.getName()))
            .collect(Collectors.joining(", "));
    String key = quoteIdentifier(column);
    String sql =
        "SELECT "
            + fields
            + " FROM "
            + quoteIdentifier(table)
            + " WHERE "
            + key
            + " >= {beam_lower:Int64} AND "
            + key
            + (range.upperInclusive() ? " <= " : " < ")
            + "{beam_upper:Int64}";
    Map<String, Object> parameters = new LinkedHashMap<>();
    parameters.put("beam_lower", range.lowerBound());
    parameters.put("beam_upper", range.upperBound());
    return new ClickHouseReadSql(sql, parameters);
  }

  @Override
  public String toString() {
    return "ClickHouseReadSql";
  }

  static String describeTable(String table) {
    return "DESCRIBE TABLE " + quoteIdentifier(table);
  }

  private static String quoteIdentifier(String identifier) {
    // ClickHouse identifiers use backslash escapes. Escape literal backslashes before the existing
    // helper escapes quotes; otherwise a trailing backslash can escape the closing quote.
    return ClickHouseIO.quoteIdentifier(identifier.replace("\\", "\\\\"));
  }
}
