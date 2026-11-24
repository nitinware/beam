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

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.checkerframework.checker.nullness.qual.Nullable;

/** A worker input containing only a range, or a marker for the configured single query. */
@AutoValue
abstract class ClickHouseReadPartition implements Serializable {
  abstract @Nullable ClickHouseReadRange range();

  static ClickHouseReadPartition singleQuery() {
    return new AutoValue_ClickHouseReadPartition(null);
  }

  static ClickHouseReadPartition forRange(ClickHouseReadRange range) {
    return new AutoValue_ClickHouseReadPartition(range);
  }
}
