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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.query.QueryResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowedValues;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Public table-read pipelines and worker-side partition metadata validation. */
@RunWith(JUnit4.class)
public class ClickHouseTableReadIT extends BaseClickHouseTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static Client newClient() {
    return new Client.Builder()
        .addEndpoint(clickHouseUrl)
        .setDefaultDatabase(database)
        .setUsername(clickHouse.getUsername())
        .setPassword(clickHouse.getPassword())
        .build();
  }

  private static void execute(Client client, String sql) throws Exception {
    try (QueryResponse ignored = client.query(sql).get(CLIENT_TIMEOUT, TimeUnit.SECONDS)) {
      // Close all fixture responses, including DDL statements.
    }
  }

  private static ClickHouseIO.Read read(String databaseName) {
    Properties properties = new Properties();
    properties.setProperty("user", clickHouse.getUsername());
    properties.setProperty("password", clickHouse.getPassword());
    properties.setProperty("max_open_connections", "1");
    properties.setProperty("connection_request_timeout", "2000");
    properties.setProperty("compress", "true");
    return ClickHouseIO.read("jdbc:clickhouse:" + clickHouseUrl + "/" + databaseName)
        .withProperties(properties);
  }

  private static ClickHouseIO.Read tableRead(
      String table, Schema schema, long lower, long upper, int count) {
    return read(database)
        .withTable(table)
        .withSchema(schema)
        .withPartitionColumn("id")
        .withLowerBound(lower)
        .withUpperBound(upper)
        .withNumPartitions(count);
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable result = failure;
    while (result.getCause() != null) {
      result = result.getCause();
    }
    return result;
  }

  @Test
  public void parallelAndSingleReadsMatchRowMultisetsForBothPartitionTypes() throws Exception {
    Schema schema =
        Schema.builder()
            .addStringField("label")
            .addNullableField("optional", Schema.FieldType.INT32)
            .build();
    List<Row> expected = new ArrayList<>();
    for (String label :
        Arrays.asList("zero", "zero", "quarter", "half", "three quarters", "last")) {
      expected.add(Row.withSchema(schema).addValues(label, null).build());
    }
    try (Client client = newClient()) {
      for (String type : Arrays.asList("Int32", "Int64")) {
        String table = "beam_table_" + type;
        execute(
            client,
            "CREATE TABLE "
                + table
                + " (id "
                + type
                + ", label String, optional Nullable(Int32), unused UInt64) ENGINE=Memory");
        execute(
            client,
            "INSERT INTO "
                + table
                + " VALUES "
                + "(-1,'before',NULL,0),(0,'zero',NULL,0),(0,'zero',NULL,0),"
                + "(25,'quarter',NULL,0),(50,'half',NULL,0),(75,'three quarters',NULL,0),"
                + "(100,'last',NULL,0),(101,'after',NULL,0)");
        PCollection<Row> single =
            pipeline.apply(
                "single-" + type,
                read(database)
                    .withQuery(
                        "SELECT label, optional FROM " + table + " WHERE id >= 0 AND id <= 100")
                    .withSchema(schema));
        PAssert.that(single).containsInAnyOrder(expected);
        for (int count : new int[] {1, 4, 10}) {
          PCollection<Row> parallel =
              pipeline.apply(
                  "parallel-" + type + "-" + count, tableRead(table, schema, 0, 100, count));
          assertEquals(schema, parallel.getSchema());
          // Matching both outputs to the same multiset preserves duplicates, not merely counts.
          PAssert.that(parallel).containsInAnyOrder(expected);
        }
      }
    }
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void parallelReadsAcceptAnEmptyTableWithValidMetadata() throws Exception {
    try (Client client = newClient()) {
      execute(client, "CREATE TABLE beam_empty_table (id Int64, label String) ENGINE=Memory");
    }
    Schema schema = Schema.builder().addStringField("label").build();
    for (int count : new int[] {1, 4}) {
      PCollection<Row> rows =
          pipeline.apply("empty-" + count, tableRead("beam_empty_table", schema, 0, 100, count));
      assertEquals(schema, rows.getSchema());
      PAssert.that(rows).empty();
    }
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void skewedRowsAndEmptyPartitionsPreserveTheCompleteMultiset() throws Exception {
    try (Client client = newClient()) {
      execute(client, "CREATE TABLE beam_skewed_table (id Int64, label String) ENGINE=Memory");
      // All 257 duplicate rows occupy one partition; the final endpoint occupies another.
      // Interior partitions contain no rows, and records just outside the bounds are excluded.
      execute(client, "INSERT INTO beam_skewed_table SELECT 0, 'repeated' FROM numbers(257)");
      execute(
          client, "INSERT INTO beam_skewed_table VALUES (100,'last'),(-1,'before'),(101,'after')");
    }
    Schema schema = Schema.builder().addStringField("label").build();
    List<Row> expected = new ArrayList<>();
    for (int i = 0; i < 257; i++) {
      expected.add(Row.withSchema(schema).addValue("repeated").build());
    }
    expected.add(Row.withSchema(schema).addValue("last").build());
    PAssert.that(
            pipeline.apply("skewed-parallel", tableRead("beam_skewed_table", schema, 0, 100, 8)))
        .containsInAnyOrder(expected);
    PAssert.that(
            pipeline.apply(
                "skewed-single",
                read(database)
                    .withQuery("SELECT label FROM beam_skewed_table WHERE id >= 0 AND id <= 100")
                    .withSchema(schema)))
        .containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void existingWriterRoundTripsThroughBothReadModes() throws Exception {
    try (Client client = newClient()) {
      execute(
          client,
          "CREATE TABLE beam_read_write_roundtrip "
              + "(id Int64, label String, optional Nullable(Int32)) ENGINE=Memory");
    }
    Schema schema =
        Schema.builder()
            .addInt64Field("id")
            .addStringField("label")
            .addNullableField("optional", Schema.FieldType.INT32)
            .build();
    List<Row> expected =
        Arrays.asList(
            Row.withSchema(schema).addValues(0L, "東京", null).build(),
            Row.withSchema(schema).addValues(0L, "東京", null).build(),
            Row.withSchema(schema).addValues(50L, "", Integer.MIN_VALUE).build(),
            Row.withSchema(schema).addValues(100L, "last", Integer.MAX_VALUE).build());
    Properties properties = new Properties();
    properties.setProperty("user", clickHouse.getUsername());
    properties.setProperty("password", clickHouse.getPassword());
    Pipeline writer = Pipeline.create(pipeline.getOptions());
    writer
        .apply(Create.of(expected).withRowSchema(schema))
        .apply(
            ClickHouseIO.<Row>write(clickHouseUrl, database, "beam_read_write_roundtrip")
                .withProperties(properties)
                .withMaxRetries(0));
    // Separate pipelines guarantee that every write finishes before either read begins.
    writer.run().waitUntilFinish();
    PAssert.that(
            pipeline.apply(
                "roundtrip-single",
                read(database)
                    .withQuery("SELECT * FROM beam_read_write_roundtrip")
                    .withSchema(schema)))
        .containsInAnyOrder(expected);
    PAssert.that(
            pipeline.apply(
                "roundtrip-parallel", tableRead("beam_read_write_roundtrip", schema, 0, 100, 4)))
        .containsInAnyOrder(expected);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void publicTableReadsIncludeExtremeEndpointsAndEqualBounds() throws Exception {
    try (Client client = newClient()) {
      execute(client, "CREATE TABLE beam_table_extremes (id Int64) ENGINE=Memory");
      execute(
          client,
          "INSERT INTO beam_table_extremes VALUES ("
              + Long.MIN_VALUE
              + "),("
              + (Long.MIN_VALUE + 1)
              + "),("
              + (Long.MAX_VALUE - 1)
              + "),("
              + Long.MAX_VALUE
              + ")");
    }
    Schema schema = Schema.builder().addInt64Field("id").build();
    int index = 0;
    for (long[] bounds :
        new long[][] {
          {Long.MIN_VALUE, Long.MIN_VALUE + 1},
          {Long.MAX_VALUE - 1, Long.MAX_VALUE},
          {Long.MIN_VALUE, Long.MIN_VALUE},
          {Long.MAX_VALUE, Long.MAX_VALUE}
        }) {
      PCollection<Row> rows =
          pipeline.apply(
              "extreme-" + index++,
              tableRead("beam_table_extremes", schema, bounds[0], bounds[1], 4));
      List<Row> expected = new ArrayList<>();
      expected.add(Row.withSchema(schema).addValue(bounds[0]).build());
      if (bounds[0] != bounds[1]) {
        expected.add(Row.withSchema(schema).addValue(bounds[1]).build());
      }
      PAssert.that(rows).containsInAnyOrder(expected);
    }
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void setupRejectsMissingNullableAndUnsupportedDatabaseKeysDespiteSuppliedSchema()
      throws Exception {
    Schema schema = Schema.builder().addInt64Field("id").addStringField("label").build();
    try (Client client = newClient()) {
      int index = 0;
      for (String type :
          Arrays.asList(
              "Nullable(Int64)", "Nullable(Int32)", "UInt64", "Int16", "String", "Date", "Int64")) {
        String table = "beam_invalid_key_" + index++;
        execute(client, "CREATE TABLE " + table + " (id " + type + ", label String) ENGINE=Memory");
        ClickHouseIO.Read configuration = tableRead(table, schema, 0, 1, 2);
        if (type.equals("Int64")) {
          configuration = configuration.withPartitionColumn("missing");
        }
        ClickHouseReadFn fn = new ClickHouseReadFn(configuration);
        try {
          IOException error = assertThrows(IOException.class, fn::setup);
          assertTrue(
              rootCause(error).getMessage(),
              rootCause(error).getMessage().contains("Partition column"));
          // Failed setup must not publish a usable client or permit any range query.
          assertThrows(
              IllegalStateException.class,
              () ->
                  fn.process(
                      ClickHouseReadPartition.forRange(ClickHouseReadRange.of(0, 1, true)),
                      value ->
                          WindowedValues.builder(WindowedValues.valueInGlobalWindow(value))
                              .setReceiver(
                                  row -> {
                                    throw new AssertionError("Unexpected row");
                                  })));
        } finally {
          fn.tearDown();
        }
      }
    }
  }

  @Test
  public void nullableKeyHiddenFromOutputStillFailsInWorkerSetup() throws Exception {
    try (Client client = newClient()) {
      execute(
          client,
          "CREATE TABLE beam_hidden_nullable (id Nullable(Int64), label String) ENGINE=Memory");
      execute(client, "INSERT INTO beam_hidden_nullable VALUES (NULL,'null key'),(1,'one')");
    }
    Schema schema = Schema.builder().addStringField("label").build();
    pipeline.apply(tableRead("beam_hidden_nullable", schema, 0, 1, 2));
    RuntimeException error =
        assertThrows(RuntimeException.class, () -> pipeline.run().waitUntilFinish());
    assertTrue(
        rootCause(error).toString(),
        rootCause(error).getMessage().contains("must not be nullable"));
  }

  @Test
  public void metadataUsesConfiguredDatabaseAndEscapedIdentifiers() throws Exception {
    String table = "beam_table_\"\\";
    String key = "key\"\\column";
    String label = "label\"\\column";
    String tableDdl = "`" + table.replace("\\", "\\\\") + "`";
    String keyDdl = "`" + key.replace("\\", "\\\\") + "`";
    String labelDdl = "`" + label.replace("\\", "\\\\") + "`";
    try (Client client = newClient()) {
      execute(client, "CREATE DATABASE beam_metadata_db");
      execute(
          client,
          "CREATE TABLE beam_metadata_db."
              + tableDdl
              + " ("
              + keyDdl
              + " Int64, "
              + labelDdl
              + " String) ENGINE=Memory");
      execute(client, "INSERT INTO beam_metadata_db." + tableDdl + " VALUES (0,'zero'),(1,'one')");
    }
    Schema schema = Schema.builder().addStringField(label).build();
    PCollection<Row> rows =
        pipeline.apply(
            read("beam_metadata_db")
                .withTable(table)
                .withPartitionColumn(key)
                .withSchema(schema)
                .withLowerBound(0)
                .withUpperBound(1)
                .withNumPartitions(2));
    PAssert.that(rows)
        .containsInAnyOrder(
            Row.withSchema(schema).addValue("zero").build(),
            Row.withSchema(schema).addValue("one").build());
    pipeline.run().waitUntilFinish();
  }

  @Test(timeout = 60000)
  public void oneWorkerClientProcessesMultipleRangesAfterSetup() throws Exception {
    try (Client client = newClient()) {
      execute(client, "CREATE TABLE beam_worker_ranges (id Int64) ENGINE=Memory");
      execute(client, "INSERT INTO beam_worker_ranges VALUES (0),(1)");
    }
    Schema schema = Schema.builder().addInt64Field("id").build();
    ClickHouseReadFn fn = new ClickHouseReadFn(tableRead("beam_worker_ranges", schema, 0, 1, 2));
    List<Row> rows = new ArrayList<>();
    fn.setup();
    try {
      for (ClickHouseReadRange range : ClickHouseRangePartitioner.partition(0, 1, 2)) {
        fn.process(
            ClickHouseReadPartition.forRange(range),
            value ->
                WindowedValues.builder(WindowedValues.valueInGlobalWindow(value))
                    .setReceiver(row -> rows.add(row.getValue())));
      }
      assertEquals(
          Arrays.asList(
              Row.withSchema(schema).addValue(0L).build(),
              Row.withSchema(schema).addValue(1L).build()),
          rows);
    } finally {
      fn.tearDown();
    }
  }
}
