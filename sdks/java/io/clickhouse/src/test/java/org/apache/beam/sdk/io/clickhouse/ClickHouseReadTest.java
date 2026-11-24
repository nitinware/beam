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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Configuration tests for the read API; no ClickHouse endpoint is required. */
@RunWith(JUnit4.class)
public class ClickHouseReadTest {
  private static final String URL = "jdbc:clickhouse://unreachable.invalid:8123/events";
  private static final Schema SCHEMA = Schema.builder().addStringField("event_type").build();

  @Test
  public void queryConfigurationDoesNotConnect() {
    ClickHouseIO.Read read =
        ClickHouseIO.read(URL).withQuery("SELECT event_type FROM events").withSchema(SCHEMA);
    read.validateConfiguration();
    assertEquals("http://unreachable.invalid:8123", read.clickHouseUrl());
    assertEquals("events", read.database());
    assertEquals("SELECT event_type FROM events", read.query());
    assertEquals(SCHEMA, read.schema());
    assertNull(read.table());
    assertNull(read.partitionColumn());
    assertNull(read.lowerBound());
    assertNull(read.upperBound());
    assertNull(read.numPartitions());
  }

  @Test
  public void tableConfigurationAllowsPartitionKeyOutsideOutputSchema() {
    ClickHouseIO.Read read =
        ClickHouseIO.read(URL)
            .withTable("events")
            .withSchema(SCHEMA)
            .withPartitionColumn("id")
            .withLowerBound(0L)
            .withUpperBound(100L)
            .withNumPartitions(4);
    read.validateConfiguration();
    assertEquals("events", read.table());
    assertNull(read.query());
    assertEquals(SCHEMA, read.schema());
    assertEquals("id", read.partitionColumn());
    assertEquals(Long.valueOf(0), read.lowerBound());
    assertEquals(Long.valueOf(100), read.upperBound());
    assertEquals(Integer.valueOf(4), read.numPartitions());
  }

  @Test
  public void configurationMethodsDoNotMutateEarlierTransforms() {
    ClickHouseIO.Read base = ClickHouseIO.read(URL);
    ClickHouseIO.Read query = base.withQuery("SELECT 1").withSchema(SCHEMA);
    ClickHouseIO.Read table = base.withTable("events");
    assertNull(base.query());
    assertNull(base.table());
    assertNull(base.schema());
    assertNull(query.table());
    assertNull(table.query());
  }

  @Test
  public void conflictingModesArePreservedForValidationRatherThanSilentlyOverridden() {
    ClickHouseIO.Read read = ClickHouseIO.read(URL).withQuery("SELECT 1").withTable("events");
    assertEquals("SELECT 1", read.query());
    assertEquals("events", read.table());
  }

  @Test
  public void propertyMergingCopiesInputsAndIncludesDefaults() {
    Properties defaults = new Properties();
    defaults.setProperty("socket_timeout", "1000");
    Properties supplied = new Properties(defaults);
    supplied.setProperty("user", "url_user");
    ClickHouseIO.Read base = ClickHouseIO.read(URL + "?user=url_user&password=url_password");
    ClickHouseIO.Read first = base.withProperties(supplied);
    supplied.setProperty("user", "mutated");
    defaults.setProperty("socket_timeout", "2000");
    assertEquals("url_user", base.properties().getProperty("user"));
    assertEquals("url_user", first.properties().getProperty("user"));
    assertEquals("1000", first.properties().getProperty("socket_timeout"));
    assertEquals("url_password", first.properties().getProperty("password"));
    Properties later = new Properties();
    later.setProperty("user", "last");
    assertThrows(IllegalArgumentException.class, () -> first.withProperties(later));
    assertEquals("url_user", first.properties().getProperty("user"));
    Properties returned = first.properties();
    returned.setProperty("user", "changed through getter");
    assertEquals("url_user", first.properties().getProperty("user"));
  }

  @Test
  public void propertyConflictsOmitBothCredentialValues() {
    Properties supplied = new Properties();
    supplied.setProperty("password", "new-secret");
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseIO.read(URL + "?password=old-secret").withProperties(supplied));
    assertFalse(error.getMessage().contains("new-secret"));
    assertFalse(error.getMessage().contains("old-secret"));
    assertTrue(error.getMessage().contains("password"));
  }

  @Test
  public void excessiveEffectivePartitionCountsFailBeforeExpansion() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            tableRead()
                .withLowerBound(0)
                .withUpperBound(Long.MAX_VALUE - 1)
                .withNumPartitions(Integer.MAX_VALUE)
                .validateConfiguration());
    tableRead()
        .withLowerBound(0)
        .withUpperBound(9999)
        .withNumPartitions(10000)
        .validateConfiguration();
  }

  @Test
  public void displayDataContainsSafeReadSettingsButNotCredentialsOrSql() {
    ClickHouseIO.Read read = tableRead().withProperties(secretProperties());
    DisplayData data = DisplayData.from(read);
    Map<String, Object> values = new HashMap<>();
    data.items().forEach(item -> values.put(item.getKey(), item.getValue()));
    assertEquals("http://unreachable.invalid:8123", values.get("url"));
    assertEquals("events", values.get("table"));
    assertEquals("id", values.get("partitionColumn"));
    assertEquals(0L, values.get("lowerBound"));
    assertEquals(100L, values.get("upperBound"));
    assertEquals(4L, values.get("numPartitions"));
    assertFalse(data.toString().contains("private-password"));
    assertFalse(data.toString().contains("password"));
    String queryData =
        DisplayData.from(
                ClickHouseIO.read(URL)
                    .withQuery("SELECT 'private-password'")
                    .withProperties(secretProperties())
                    .withSchema(SCHEMA))
            .toString();
    assertFalse(queryData.contains("SELECT"));
    assertFalse(queryData.contains("private-password"));
  }

  @Test
  public void shortPasswordsDoNotAlterDisplayIdentifiers() {
    Properties properties = new Properties();
    properties.setProperty("password", "a");
    DisplayData data = DisplayData.from(tableRead().withProperties(properties));
    Map<String, Object> values = new HashMap<>();
    data.items().forEach(item -> values.put(item.getKey(), item.getValue()));
    assertEquals("http://unreachable.invalid:8123", values.get("url"));
    assertEquals("events", values.get("database"));
    assertEquals("events", values.get("table"));
    assertEquals("id", values.get("partitionColumn"));
    assertFalse(values.containsKey("password"));
  }

  @Test
  public void allMalformedUrlErrorsOmitTheSuppliedUrlAndCauses() {
    for (String url :
        Arrays.asList(
            "not-a-jdbc-url?password=private-password",
            "jdbc:clickhouse://:8123/default?password=private-password",
            "jdbc:clickhouse:ftp://localhost/default?password=private-password",
            "jdbc:clickhouse://localhost/default?password=private-password%GG")) {
      IllegalArgumentException error =
          assertThrows(IllegalArgumentException.class, () -> ClickHouseIO.read(url));
      assertFalse(error.toString().contains("private-password"));
      assertFalse(error.toString().contains(url));
      assertNull(error.getCause());
    }
  }

  private static Properties secretProperties() {
    Properties properties = new Properties();
    properties.setProperty("password", "private-password");
    return properties;
  }

  @Test
  public void configurationSurvivesSerialization() {
    ClickHouseIO.Read read =
        ClickHouseIO.read(URL)
            .withTable("events")
            .withSchema(SCHEMA)
            .withPartitionColumn("id")
            .withLowerBound(Long.MAX_VALUE)
            .withUpperBound(Long.MAX_VALUE)
            .withNumPartitions(1);
    ClickHouseIO.Read copy = SerializableUtils.clone(read);
    assertEquals(read, copy);
  }

  @Test
  public void diagnosticsDoNotExposeCredentialsOrQueryText() {
    ClickHouseIO.Read read =
        ClickHouseIO.read(URL + "?password=secret_password")
            .withQuery("SELECT 'private_query_text'")
            .withSchema(SCHEMA);
    String diagnostics = read.toString() + DisplayData.from(read);
    assertFalse(diagnostics.contains("secret_password"));
    assertFalse(diagnostics.contains("private_query_text"));
  }

  @Test
  public void malformedUrlErrorsDoNotExposeCredentials() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseIO.read("jdbc:clickhouse://bad host/events?password=secret_password"));
    assertTrue(error.getMessage().startsWith("Invalid ClickHouse connection URL: "));
    assertTrue(error.getMessage().contains("authority"));
    assertFalse(error.getMessage().contains("secret_password"));
    assertNull(error.getCause());
  }

  private static ClickHouseIO.Read queryRead() {
    return ClickHouseIO.read(URL).withQuery("SELECT event_type FROM events").withSchema(SCHEMA);
  }

  private static ClickHouseIO.Read tableRead() {
    return ClickHouseIO.read(URL)
        .withTable("events")
        .withSchema(SCHEMA)
        .withPartitionColumn("id")
        .withLowerBound(0)
        .withUpperBound(100)
        .withNumPartitions(4);
  }

  private static void assertInvalid(ClickHouseIO.Read read, String message) {
    // Exercise expansion so invalid configurations cannot reach reader execution.
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class, () -> read.expand(PBegin.in(Pipeline.create())));
    assertTrue(error.getMessage(), error.getMessage().contains(message));
  }

  @Test
  public void schemaIsRequiredInBothModes() {
    assertInvalid(ClickHouseIO.read(URL).withQuery("SELECT 1"), "schema is required");
    assertInvalid(ClickHouseIO.read(URL).withTable("events"), "schema is required");
    assertInvalid(queryRead().withSchema(Schema.builder().build()), "at least one field");
  }

  @Test
  public void exactlyOneModeIsRequired() {
    assertInvalid(ClickHouseIO.read(URL).withSchema(SCHEMA), "Exactly one");
    assertInvalid(queryRead().withTable("events"), "Exactly one");
    assertInvalid(tableRead().withQuery("SELECT 1"), "Exactly one");
  }

  @Test
  public void everyPartitionOptionIsRejectedInQueryMode() {
    assertInvalid(queryRead().withPartitionColumn("id"), "only with withTable()");
    assertInvalid(queryRead().withLowerBound(0), "only with withTable()");
    assertInvalid(queryRead().withUpperBound(100), "only with withTable()");
    assertInvalid(queryRead().withNumPartitions(4), "only with withTable()");
  }

  @Test
  public void everyIncompleteTablePartitionConfigurationIsRejected() {
    for (int mask = 0; mask < 15; mask++) {
      ClickHouseIO.Read read = ClickHouseIO.read(URL).withSchema(SCHEMA).withTable("events");
      if ((mask & 1) != 0) {
        read = read.withPartitionColumn("id");
      }
      if ((mask & 2) != 0) {
        read = read.withLowerBound(0);
      }
      if ((mask & 4) != 0) {
        read = read.withUpperBound(100);
      }
      if ((mask & 8) != 0) {
        read = read.withNumPartitions(4);
      }
      assertInvalid(read, "Table reads require");
    }
  }

  @Test
  public void blankSqlAndInvalidNamesAreRejectedWithoutEchoingValues() {
    assertInvalid(queryRead().withQuery(" \n\t"), "Read query must not be blank");
    assertInvalid(tableRead().withTable(" "), "Table name must not be blank");
    assertInvalid(tableRead().withPartitionColumn(""), "Partition column name must not be blank");
    assertInvalid(tableRead().withTable("sensitive\0name"), "must not contain a NUL");
    assertInvalid(
        queryRead().withSchema(Schema.builder().addStringField(" ").build()),
        "Output field name must not be blank");
    // Quoting, rather than an alphanumeric allowlist, handles punctuation in real identifiers.
    tableRead().withTable("event-log").withPartitionColumn("event id").validateConfiguration();
  }

  @Test
  public void reversedBoundsAndNonPositivePartitionCountsAreRejected() {
    assertInvalid(tableRead().withLowerBound(101), "Lower bound must be less than or equal");
    assertInvalid(tableRead().withNumPartitions(0), "Partition count must be at least 1");
    assertInvalid(tableRead().withNumPartitions(-1), "Partition count must be at least 1");
  }

  @Test
  public void equalBoundsAtBothLongExtremesAreValidWithoutConnecting() {
    for (long endpoint : new long[] {Long.MIN_VALUE, 0, Long.MAX_VALUE}) {
      ClickHouseIO.Read read = tableRead().withLowerBound(endpoint).withUpperBound(endpoint);
      read.validateConfiguration();
      assertEquals(1, read.inclusiveRangeCardinality());
    }
  }

  @Test
  public void inclusiveCardinalityHandlesNearExtremeBounds() {
    ClickHouseIO.Read lower =
        tableRead().withLowerBound(Long.MIN_VALUE).withUpperBound(Long.MIN_VALUE + 1);
    lower.validateConfiguration();
    assertEquals(2, lower.inclusiveRangeCardinality());
    ClickHouseIO.Read upper =
        tableRead()
            .withLowerBound(Long.MAX_VALUE - 1)
            .withUpperBound(Long.MAX_VALUE)
            .withNumPartitions(1);
    upper.validateConfiguration();
    assertEquals(2, upper.inclusiveRangeCardinality());
    ClickHouseIO.Read largest = tableRead().withLowerBound(1).withUpperBound(Long.MAX_VALUE);
    largest.validateConfiguration();
    assertEquals(Long.MAX_VALUE, largest.inclusiveRangeCardinality());
  }

  @Test
  public void subtractionAndInclusiveAdditionOverflowAreRejected() {
    assertInvalid(
        tableRead().withLowerBound(Long.MIN_VALUE).withUpperBound(Long.MAX_VALUE),
        "v1 implementation restriction");
    assertInvalid(
        tableRead().withLowerBound(0).withUpperBound(Long.MAX_VALUE),
        "v1 implementation restriction");
    assertInvalid(
        tableRead().withLowerBound(Long.MIN_VALUE).withUpperBound(-1),
        "v1 implementation restriction");
  }

  @Test
  public void morePartitionsThanValuesIsValidForLaterCapping() {
    tableRead()
        .withLowerBound(0)
        .withUpperBound(1)
        .withNumPartitions(Integer.MAX_VALUE)
        .validateConfiguration();
  }

  @Test
  public void nullPropertiesFailWithoutLeakingExistingCredentials() {
    IllegalArgumentException error =
        assertThrows(
            IllegalArgumentException.class,
            () -> ClickHouseIO.read(URL + "?password=private_password").withProperties(null));
    assertFalse(error.toString().contains("private_password"));
  }

  @Test
  public void queryExpansionAttachesSchemaWithoutContactingDatabase() {
    Pipeline pipeline = Pipeline.create();
    PCollection<Row> rows = pipeline.apply(queryRead());
    assertTrue(rows.hasSchema());
    assertEquals(SCHEMA, rows.getSchema());
    assertEquals(PCollection.IsBounded.BOUNDED, rows.isBounded());
  }

  @Test
  public void tableExpansionAttachesSchemaWithoutContactingDatabase() {
    Pipeline pipeline = Pipeline.create();
    PCollection<Row> rows = pipeline.apply(tableRead());
    assertTrue(rows.hasSchema());
    assertEquals(SCHEMA, rows.getSchema());
    assertEquals(PCollection.IsBounded.BOUNDED, rows.isBounded());
  }
}
