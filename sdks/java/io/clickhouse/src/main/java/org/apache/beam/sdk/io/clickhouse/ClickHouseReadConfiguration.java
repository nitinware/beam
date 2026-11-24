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

import java.io.Serializable;
import java.util.Properties;
import org.apache.beam.sdk.schemas.Schema;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Only the immutable settings needed by read workers, without the enclosing transform. */
final class ClickHouseReadConfiguration implements Serializable {
  private final String url;
  private final String database;
  private final Properties properties;
  private final @Nullable String query;
  private final @Nullable String table;
  private final Schema schema;
  private final @Nullable String partitionColumn;

  ClickHouseReadConfiguration(ClickHouseIO.Read read) {
    url = read.clickHouseUrl();
    database = read.database();
    properties = read.properties();
    query = read.query();
    table = read.table();
    Schema suppliedSchema = read.schema();
    if (suppliedSchema == null) {
      throw new IllegalArgumentException("An output schema is required");
    }
    schema = suppliedSchema;
    partitionColumn = read.partitionColumn();
  }

  String clickHouseUrl() {
    return url;
  }

  String database() {
    return database;
  }

  Properties properties() {
    Properties copy = new Properties();
    copy.putAll(properties);
    return copy;
  }

  @Nullable String query() {
    return query;
  }

  @Nullable String table() {
    return table;
  }

  Schema schema() {
    return schema;
  }

  @Nullable String partitionColumn() {
    return partitionColumn;
  }

  @Override
  public String toString() {
    return "ClickHouseReadConfiguration";
  }
}
