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

import static org.junit.Assert.assertThrows;

import com.clickhouse.data.ClickHouseColumn;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests of the actual database partition-type contract. */
@RunWith(JUnit4.class)
public class ClickHousePartitionValidatorTest {
  @Test
  public void onlyNonNullableInt32AndInt64AreAccepted() {
    ClickHousePartitionValidator.validateColumn(ClickHouseColumn.of("id", "Int32"));
    ClickHousePartitionValidator.validateColumn(ClickHouseColumn.of("id", "Int64"));
  }

  @Test
  public void nullableSignedIntegersAreRejected() {
    for (String type : Arrays.asList("Nullable(Int32)", "Nullable(Int64)")) {
      assertThrows(
          type,
          IllegalArgumentException.class,
          () -> ClickHousePartitionValidator.validateColumn(ClickHouseColumn.of("id", type)));
    }
  }

  @Test
  public void unsupportedTypesAndWrappersAreRejected() {
    for (String type :
        Arrays.asList(
            "Int8",
            "Int16",
            "UInt32",
            "UInt64",
            "String",
            "Date",
            "DateTime",
            "Float64",
            "Array(Int64)",
            "LowCardinality(Int64)",
            "SimpleAggregateFunction(any, Int64)")) {
      assertThrows(
          type,
          IllegalArgumentException.class,
          () -> ClickHousePartitionValidator.validateColumn(ClickHouseColumn.of("id", type)));
    }
  }
}
