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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.clickhouse.data.ClickHouseColumn;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.FixedBytes;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Metadata and value conversion tests independent of a running database. */
@RunWith(JUnit4.class)
public class ClickHouseRowReaderTest {
  private static com.clickhouse.client.api.metadata.TableSchema metadata(String... columns) {
    return new com.clickhouse.client.api.metadata.TableSchema(
        ClickHouseColumn.parse(String.join(", ", columns)));
  }

  @Test
  public void bindsByNameInOutputOrderAndAllowsExtraResultColumns() {
    Schema schema = Schema.builder().addStringField("label").addInt64Field("id").build();
    ClickHouseRowReader reader =
        ClickHouseRowReader.bind(schema, metadata("id Int64", "extra Int32", "label String"));
    Object[] values = {42L, 7, "event"};
    assertEquals(
        Row.withSchema(schema).addValues("event", 42L).build(),
        reader.readRow(index -> values[index - 1]));
  }

  @Test
  public void nullableOutputsAcceptNullAndNonNullValues() {
    Schema schema = Schema.builder().addNullableField("id", Schema.FieldType.INT64).build();
    ClickHouseRowReader reader = ClickHouseRowReader.bind(schema, metadata("id Nullable(Int64)"));
    assertEquals(Row.withSchema(schema).addValue(null).build(), reader.readRow(index -> null));
    assertEquals(Row.withSchema(schema).addValue(42L).build(), reader.readRow(index -> 42L));
    ClickHouseRowReader nonNullableSource = ClickHouseRowReader.bind(schema, metadata("id Int64"));
    assertEquals(
        Row.withSchema(schema).addValue(42L).build(), nonNullableSource.readRow(index -> 42L));
  }

  @Test
  public void missingAndDuplicateColumnsFailDuringBinding() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseRowReader.bind(schema, metadata("other Int64")));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseRowReader.bind(schema, metadata("id Int64", "id Int64")));
  }

  @Test
  public void incompatibleTypesDoNotImplicitlyCastOrWiden() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseRowReader.bind(schema, metadata("id Int32")));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseRowReader.bind(schema, metadata("id String")));
  }

  @Test
  public void nullableSourceCannotBindToNonNullableOutputEvenWithoutRows() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseRowReader.bind(schema, metadata("id Nullable(Int64)")));
    assertTrue(error.getMessage().contains("nullable Beam field"));
  }

  @Test
  public void validationErrorsIdentifyTheAffectedField() {
    Schema schema = Schema.builder().addInt64Field("customer_id").build();
    IllegalArgumentException missing =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseRowReader.bind(schema, metadata("other Int64")));
    assertTrue(missing.getMessage().contains("customer_id"));
    IllegalArgumentException incompatible =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseRowReader.bind(schema, metadata("customer_id String")));
    assertTrue(incompatible.getMessage().contains("customer_id"));
    assertTrue(incompatible.getMessage().contains("String"));
    IllegalArgumentException duplicate =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                ClickHouseRowReader.bind(
                    schema, metadata("customer_id Int64", "customer_id Int64")));
    assertTrue(duplicate.getMessage().contains("customer_id"));
    ClickHouseRowReader reader = ClickHouseRowReader.bind(schema, metadata("customer_id Int64"));
    IllegalArgumentException nullValue =
        assertThrows(IllegalArgumentException.class, () -> reader.readRow(index -> null));
    assertTrue(nullValue.getMessage().contains("customer_id"));
  }

  @Test
  public void unsupportedDatabaseTypesAreRejectedBeforeReadingRows() {
    Schema schema = Schema.builder().addStringField("value").build();
    for (String type :
        Arrays.asList(
            "FixedString(4)",
            "Nullable(FixedString(4))",
            "UInt64",
            "Decimal(10,2)",
            "DateTime64(3)",
            "DateTime32",
            "Array(Int32)",
            "Tuple(Int32, String)",
            "Map(String, Int32)",
            "Enum8('x'=1)",
            "LowCardinality(String)")) {
      assertThrows(
          type,
          IllegalArgumentException.class,
          () -> ClickHouseRowReader.bind(schema, metadata("value " + type)));
    }
  }

  @Test
  public void unsupportedBeamTypesAreRejectedLocally() {
    for (Schema.FieldType type :
        Arrays.asList(
            Schema.FieldType.BYTES,
            Schema.FieldType.DECIMAL,
            Schema.FieldType.array(Schema.FieldType.INT32),
            Schema.FieldType.logicalType(FixedBytes.of(4)))) {
      Schema schema = Schema.builder().addField("value", type).build();
      assertThrows(
          IllegalArgumentException.class,
          () ->
              ClickHouseIO.read("jdbc:clickhouse://unreachable.invalid/default")
                  .withQuery("SELECT value")
                  .withSchema(schema)
                  .validateConfiguration());
    }
  }

  @Test
  public void unexpectedNullOrJavaTypeFailsWithoutPrintingValue() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    ClickHouseRowReader reader = ClickHouseRowReader.bind(schema, metadata("id Int64"));
    assertThrows(IllegalArgumentException.class, () -> reader.readRow(index -> null));
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class, () -> reader.readRow(index -> "sensitive-value"));
    assertFalse(error.getMessage().contains("sensitive-value"));
  }

  @Test
  public void scalarMappingsPreserveBoundaryValuesAndNullableVariants() {
    Object[][] cases = {
      {"Int8", Schema.FieldType.BYTE, Byte.MIN_VALUE, Byte.MAX_VALUE},
      {"Int16", Schema.FieldType.INT16, Short.MIN_VALUE, Short.MAX_VALUE},
      {"Int32", Schema.FieldType.INT32, Integer.MIN_VALUE, Integer.MAX_VALUE},
      {"Int64", Schema.FieldType.INT64, Long.MIN_VALUE, Long.MAX_VALUE},
      {"UInt8", Schema.FieldType.INT16, (short) 0, (short) 255},
      {"UInt16", Schema.FieldType.INT32, 0, 65535},
      {"UInt32", Schema.FieldType.INT64, 0L, 4294967295L},
      {"Float32", Schema.FieldType.FLOAT, Float.NEGATIVE_INFINITY, Float.NaN},
      {"Float64", Schema.FieldType.DOUBLE, Double.POSITIVE_INFINITY, Double.NaN},
      {"String", Schema.FieldType.STRING, "", "event-東京-🚀"},
      {"Bool", Schema.FieldType.BOOLEAN, false, true}
    };
    for (Object[] testCase : cases) {
      String type = (String) testCase[0];
      Schema schema =
          Schema.builder().addNullableField("value", (Schema.FieldType) testCase[1]).build();
      for (String sourceType : Arrays.asList(type, "Nullable(" + type + ")")) {
        ClickHouseRowReader reader =
            ClickHouseRowReader.bind(schema, metadata("value " + sourceType));
        for (Object value : Arrays.asList(testCase[2], testCase[3])) {
          assertEquals(
              sourceType,
              Row.withSchema(schema).addValue(value).build(),
              reader.readRow(index -> value));
        }
        if (sourceType.startsWith("Nullable(")) {
          assertEquals(
              sourceType,
              Row.withSchema(schema).addValue(null).build(),
              reader.readRow(index -> null));
        }
      }
    }
  }

  @Test
  public void subsequentReadsDoNotChangePreviouslyEmittedRows() {
    Schema schema = Schema.builder().addInt64Field("id").addStringField("label").build();
    ClickHouseRowReader reader =
        ClickHouseRowReader.bind(schema, metadata("id Int64", "label String"));
    Object[] current = {1L, "first"};
    Row first = reader.readRow(index -> current[index - 1]);
    current[0] = 2L;
    current[1] = "second";
    Row second = reader.readRow(index -> current[index - 1]);
    assertEquals(Row.withSchema(schema).addValues(1L, "first").build(), first);
    assertEquals(Row.withSchema(schema).addValues(2L, "second").build(), second);
  }

  @Test
  public void clientLookupFailuresAreNotClassifiedAsTrustedValidationErrors() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    ClickHouseRowReader reader = ClickHouseRowReader.bind(schema, metadata("id Int64"));
    IllegalArgumentException clientFailure = new IllegalArgumentException("private-client-detail");
    IllegalArgumentException actual =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                reader.readRow(
                    index -> {
                      throw clientFailure;
                    }));
    org.junit.Assert.assertSame(clientFailure, actual);
    assertFalse(actual instanceof ClickHouseReadValidationException);
  }

  @Test
  public void dateTimePreservesInstantAcrossDisplayZones() {
    Schema schema = Schema.builder().addDateTimeField("timestamp").build();
    ClickHouseRowReader reader =
        ClickHouseRowReader.bind(schema, metadata("timestamp DateTime('America/Los_Angeles')"));
    ZonedDateTime value =
        ZonedDateTime.of(2026, 1, 2, 3, 4, 5, 0, ZoneId.of("America/Los_Angeles"));
    assertEquals(
        Row.withSchema(schema).addValue(Instant.parse("2026-01-02T11:04:05Z")).build(),
        reader.readRow(index -> value));
  }

  @Test
  public void dateUsesCalendarDateAtMidnightUtc() {
    Schema schema = Schema.builder().addDateTimeField("date").build();
    ClickHouseRowReader reader = ClickHouseRowReader.bind(schema, metadata("date Date"));
    ZonedDateTime value = ZonedDateTime.of(2026, 1, 2, 0, 0, 0, 0, ZoneId.of("Asia/Tokyo"));
    assertEquals(
        Row.withSchema(schema).addValue(Instant.parse("2026-01-02T00:00:00Z")).build(),
        reader.readRow(index -> value));
  }
}
