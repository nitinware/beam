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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Client-v2 read-path validation before implementing the Beam reader. */
@RunWith(JUnit4.class)
public class ClickHouseReadClientIT extends BaseClickHouseTest {
  private static final Object[][] CASES = {
    {"Int8", "-128", (byte) -128, Schema.FieldType.BYTE},
    {"Int16", "-32768", (short) -32768, Schema.FieldType.INT16},
    {"Int32", "-2147483648", Integer.MIN_VALUE, Schema.FieldType.INT32},
    {"Int64", "-9223372036854775808", Long.MIN_VALUE, Schema.FieldType.INT64},
    {"UInt8", "255", (short) 255, Schema.FieldType.INT16},
    {"UInt16", "65535", 65535, Schema.FieldType.INT32},
    {"UInt32", "4294967295", 4294967295L, Schema.FieldType.INT64},
    {"Float32", "1.25", 1.25F, Schema.FieldType.FLOAT},
    {"Float64", "1.25", 1.25D, Schema.FieldType.DOUBLE},
    {"String", "'hello'", "hello", Schema.FieldType.STRING},
    {"Bool", "true", true, Schema.FieldType.BOOLEAN},
    {"FixedString(4)", "'ab'", "ab\0\0", null},
    {
      "Date",
      "'2026-01-02'",
      ZonedDateTime.of(2026, 1, 2, 0, 0, 0, 0, ZoneId.of("UTC")),
      Schema.FieldType.DATETIME
    },
    {
      "DateTime('UTC')",
      "'2026-01-02 03:04:05'",
      ZonedDateTime.of(2026, 1, 2, 3, 4, 5, 0, ZoneId.of("UTC")),
      Schema.FieldType.DATETIME
    }
  };

  private Client newClient() {
    return new Client.Builder()
        .addEndpoint(clickHouseUrl)
        .setUsername(clickHouse.getUsername())
        .setPassword(clickHouse.getPassword())
        .setDefaultDatabase(database)
        .build();
  }

  private QuerySettings settings() {
    return new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
  }

  @Test
  public void supportedTypesExposeValuesAndNullableMetadata() throws Exception {
    try (Client client = newClient()) {
      for (Object[] typeCase : CASES) {
        String type = (String) typeCase[0];
        String query =
            "SELECT CAST("
                + typeCase[1]
                + " AS "
                + type
                + ") AS value, "
                + "CAST(NULL AS Nullable("
                + type
                + ")) AS absent, CAST("
                + typeCase[1]
                + " AS Nullable("
                + type
                + ")) AS present";
        try (QueryResponse response =
                client.query(query, settings()).get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
          assertEquals(3, reader.getSchema().getColumns().size());
          assertFalse(reader.getSchema().getColumnByName("value").isNullable());
          assertTrue(reader.getSchema().getColumnByName("absent").isNullable());
          assertTrue(reader.hasNext());
          reader.next();
          Object value = reader.readValue("value");
          assertNotNull(type, value);
          assertNull(reader.readValue("absent"));
          Object present = reader.readValue("present");
          assertNotNull(type, present);
          assertEquals(typeCase[2].getClass(), value.getClass());
          assertEquals(type, typeCase[2], value);
          assertEquals(type, value, present);
          System.out.println(
              "READ_SPIKE "
                  + type
                  + " -> "
                  + value.getClass().getName()
                  + " value="
                  + (value instanceof byte[] ? Arrays.toString((byte[]) value) : value));
          assertFalse(reader.hasNext());
        }
      }
    }
  }

  @Test
  public void fixedStringDefaultDecodingDoesNotPreserveArbitraryBytes() throws Exception {
    // This characterizes a client limitation, not a supported FixedBytes conversion.
    try (Client client = newClient();
        QueryResponse response =
            client
                .query("SELECT CAST(unhex('FF008041') AS FixedString(4)) AS value", settings())
                .get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      assertTrue(reader.hasNext());
      reader.next();
      String decoded = reader.readValue("value");
      assertEquals("\uFFFD\0\uFFFDA", decoded);
      assertFalse(
          Arrays.equals(
              new byte[] {(byte) 0xFF, 0, (byte) 0x80, 0x41},
              decoded.getBytes(StandardCharsets.UTF_8)));
    }
  }

  @Test
  public void emptyResultRetainsColumnNamesAndTypes() throws Exception {
    try (Client client = newClient();
        QueryResponse response =
            client
                .query(
                    "SELECT CAST(1 AS Int64) AS id, CAST(NULL AS Nullable(String)) AS label WHERE 0",
                    settings())
                .get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      assertEquals(2, reader.getSchema().getColumns().size());
      assertEquals("id", reader.getSchema().getColumnByIndex(1).getColumnName());
      assertEquals("Int64", reader.getSchema().getColumnByName("id").getDataType().name());
      assertTrue(reader.getSchema().getColumnByName("label").isNullable());
      assertFalse(reader.hasNext());
    }
  }

  @Test
  public void typedParametersAndStreamingIteration() throws Exception {
    try (Client client = newClient();
        QueryResponse response =
            client
                .query(
                    "SELECT number FROM numbers(10000) WHERE number >= {lo:Int64} ORDER BY number",
                    Collections.<String, Object>singletonMap("lo", 9990L),
                    settings())
                .get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      long expected = 9990;
      while (reader.hasNext()) {
        reader.next();
        assertEquals(expected++, reader.getLong("number"));
      }
      assertEquals(10000, expected);
    }
  }

  @Test
  public void closingPartiallyConsumedResponseAllowsAnotherQuery() throws Exception {
    try (Client client = newClient()) {
      try (QueryResponse response =
              client
                  .query("SELECT number FROM numbers(10000)", settings())
                  .get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
          ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
        assertTrue(reader.hasNext());
        reader.next();
        assertEquals(0, reader.getLong("number"));
      }
      try (QueryResponse response =
              client
                  .query("SELECT 42 AS answer", settings())
                  .get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
          ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
        assertTrue(reader.hasNext());
        reader.next();
        assertEquals(42, reader.getLong("answer"));
      }
    }
  }

  @Test
  public void supportedValuesBecomeRowsInCallerSchemaOrder() throws Exception {
    try (Client client = newClient()) {
      for (Object[] typeCase : CASES) {
        if (typeCase[3] == null) {
          continue; // FixedString is explicitly outside the v1 read contract.
        }
        String type = (String) typeCase[0];
        Schema.FieldType fieldType = (Schema.FieldType) typeCase[3];
        Schema schema =
            Schema.builder()
                .addNullableField("present", fieldType)
                .addNullableField("absent", fieldType)
                .addField("value", fieldType)
                .build();
        String query =
            "SELECT CAST("
                + typeCase[1]
                + " AS "
                + type
                + ") AS value, "
                + "CAST(NULL AS Nullable("
                + type
                + ")) AS absent, CAST("
                + typeCase[1]
                + " AS Nullable("
                + type
                + ")) AS present";
        Object expected = typeCase[2];
        if (expected instanceof ZonedDateTime) {
          expected = new Instant(((ZonedDateTime) expected).toInstant().toEpochMilli());
        }
        try (QueryResponse response =
                client.query(query, settings()).get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
            ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
          ClickHouseRowReader rowReader = ClickHouseRowReader.bind(schema, reader.getSchema());
          assertTrue(reader.hasNext());
          reader.next();
          assertEquals(
              type,
              Row.withSchema(schema).addValues(expected, null, expected).build(),
              rowReader.readRow(reader::readValue));
          assertFalse(reader.hasNext());
        }
      }
    }
  }

  @Test
  public void emptyResultsStillValidateMissingUnsupportedAndIncompatibleColumns() throws Exception {
    try (Client client = newClient();
        QueryResponse response =
            client
                .query(
                    "SELECT CAST('abcd' AS FixedString(4)) AS fixed, "
                        + "CAST(NULL AS Nullable(Int64)) AS nullable, CAST(1 AS Int32) AS number WHERE 0",
                    settings())
                .get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      assertFalse(reader.hasNext());
      for (Schema schema :
          Arrays.asList(
              Schema.builder().addStringField("fixed").build(),
              Schema.builder().addInt64Field("nullable").build(),
              Schema.builder().addInt64Field("number").build(),
              Schema.builder().addInt64Field("missing").build())) {
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseRowReader.bind(schema, reader.getSchema()));
      }
      // A compatible projected schema binds even though no values are returned.
      ClickHouseRowReader.bind(
          Schema.builder().addInt32Field("number").build(), reader.getSchema());
    }
  }

  @Test
  public void dateTimeConversionPreservesInstantWithColumnTimezone() throws Exception {
    Schema schema = Schema.builder().addDateTimeField("timestamp").build();
    try (Client client = newClient();
        QueryResponse response =
            client
                .query(
                    "SELECT toDateTime('2026-01-02 03:04:05', 'America/Los_Angeles') AS timestamp",
                    settings())
                .get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      ClickHouseRowReader rowReader = ClickHouseRowReader.bind(schema, reader.getSchema());
      assertTrue(reader.hasNext());
      reader.next();
      assertEquals(
          Row.withSchema(schema).addValue(Instant.parse("2026-01-02T11:04:05Z")).build(),
          rowReader.readRow(reader::readValue));
    }
  }
}
