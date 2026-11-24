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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.clickhouse.client.api.ServerException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowedValues;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** End-to-end query reads and worker resource lifecycle tests. */
@RunWith(JUnit4.class)
public class ClickHouseReadIT extends BaseClickHouseTest {
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static ClickHouseIO.Read read(String query, Schema schema) {
    Properties properties = new Properties();
    properties.setProperty("user", clickHouse.getUsername());
    properties.setProperty("password", clickHouse.getPassword());
    properties.setProperty("max_open_connections", "1");
    properties.setProperty("connection_request_timeout", "2000");
    return ClickHouseIO.read("jdbc:clickhouse:" + clickHouseUrl + "/" + database)
        .withProperties(properties)
        .withQuery(query)
        .withSchema(schema);
  }

  private static OutputReceiver<Row> receiver(Consumer<Row> consumer) {
    return value ->
        WindowedValues.builder(WindowedValues.valueInGlobalWindow(value))
            .setReceiver(windowed -> consumer.accept(windowed.getValue()));
  }

  private static Throwable rootCause(Throwable failure) {
    Throwable result = failure;
    while (result.getCause() != null) {
      result = result.getCause();
    }
    return result;
  }

  @Test
  public void pipelineReadsNullableRowsInSchemaOrderAndPreservesDuplicates() {
    Schema schema =
        Schema.builder()
            .addStringField("label")
            .addInt64Field("id")
            .addNullableField("optional", Schema.FieldType.INT32)
            .build();
    PCollection<Row> rows =
        pipeline.apply(
            read(
                "SELECT CAST(number % 2 AS Int64) AS id, "
                    + "concat('v', toString(number % 2)) AS label, "
                    + "CAST(NULL AS Nullable(Int32)) AS optional FROM numbers(4)",
                schema));
    assertEquals(schema, rows.getSchema());
    Row zero = Row.withSchema(schema).addValues("v0", 0L, null).build();
    Row one = Row.withSchema(schema).addValues("v1", 1L, null).build();
    PAssert.that(rows).containsInAnyOrder(zero, one, zero, one);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void pipelineAcceptsEmptyResultsWithValidMetadata() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    PCollection<Row> rows = pipeline.apply(read("SELECT CAST(1 AS Int64) AS id WHERE 0", schema));
    PAssert.that(rows).empty();
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void pipelineRejectsFixedStringEvenWithNoRows() {
    Schema schema = Schema.builder().addStringField("value").build();
    pipeline.apply(read("SELECT CAST('abcd' AS FixedString(4)) AS value WHERE 0", schema));
    RuntimeException error =
        assertThrows(RuntimeException.class, () -> pipeline.run().waitUntilFinish());
    assertTrue(rootCause(error).toString(), rootCause(error).getMessage().contains("FixedString"));
  }

  @Test
  public void pipelineRejectsNullableSourceForNonNullableSchemaEvenWithNoRows() {
    Schema schema = Schema.builder().addInt64Field("id").build();
    pipeline.apply(read("SELECT CAST(NULL AS Nullable(Int64)) AS id WHERE 0", schema));
    RuntimeException error =
        assertThrows(RuntimeException.class, () -> pipeline.run().waitUntilFinish());
    assertTrue(
        rootCause(error).toString(), rootCause(error).getMessage().contains("nullable Beam field"));
  }

  @Test(timeout = 60000)
  public void partialOutputFailurePropagatesWithoutReplayAndReleasesResponse() throws Exception {
    Schema schema = Schema.builder().addInt64Field("id").build();
    ClickHouseReadFn fn =
        new ClickHouseReadFn(
            read("SELECT CAST(number AS Int64) AS id FROM numbers(10000)", schema));
    fn.setup();
    AtomicInteger emitted = new AtomicInteger();
    try {
      IllegalStateException injected = new IllegalStateException("injected output failure");
      IllegalStateException actual =
          assertThrows(
              IllegalStateException.class,
              () ->
                  fn.process(
                      ClickHouseReadPartition.singleQuery(),
                      receiver(
                          row -> {
                            if (emitted.incrementAndGet() == 3) {
                              throw injected;
                            }
                          })));
      org.junit.Assert.assertSame(injected, actual);
      assertEquals(3, emitted.get());
      // With a pool of one, a leaked first response would prevent the second request.
      AtomicInteger nextQueryCount = new AtomicInteger();
      fn.process(
          ClickHouseReadPartition.singleQuery(), receiver(row -> nextQueryCount.incrementAndGet()));
      assertEquals(10000, nextQueryCount.get());
    } finally {
      fn.tearDown();
    }
  }

  @Test(timeout = 60000)
  public void metadataFailureBeforeEmissionReleasesResponse() throws Exception {
    Schema schema = Schema.builder().addInt64Field("missing").build();
    ClickHouseReadFn fn = new ClickHouseReadFn(read("SELECT CAST(1 AS Int64) AS id", schema));
    fn.setup();
    List<Row> rows = new ArrayList<>();
    try {
      for (int i = 0; i < 2; i++) {
        IOException error =
            assertThrows(
                IOException.class,
                () -> fn.process(ClickHouseReadPartition.singleQuery(), receiver(rows::add)));
        assertTrue(rootCause(error).getMessage().contains("missing"));
        assertTrue(error.getCause() instanceof ClickHouseReadValidationException);
        assertNotNull(error.getCause());
      }
      assertTrue(rows.isEmpty());
    } finally {
      fn.tearDown();
    }
  }

  @Test(timeout = 60000)
  public void sqlFailurePreservesServerDiagnosticCause() throws Exception {
    Schema schema = Schema.builder().addInt64Field("id").build();
    ClickHouseReadFn fn =
        new ClickHouseReadFn(read("SELECT private_query_marker(1) AS id", schema));
    fn.setup();
    try {
      IOException error =
          assertThrows(
              IOException.class,
              () ->
                  fn.process(
                      ClickHouseReadPartition.singleQuery(),
                      receiver(
                          row -> {
                            throw new AssertionError("Unexpected row");
                          })));
      assertTrue(rootCause(error) instanceof ServerException);
      ServerException serverError = (ServerException) rootCause(error);
      assertEquals(46, serverError.getCode());
      assertTrue(serverError.getMessage().contains("private_query_marker"));
      assertNotNull(error.getCause());
      assertEquals(0, error.getSuppressed().length);
    } finally {
      fn.tearDown();
    }
  }

  @Test(timeout = 60000)
  public void interruptionBetweenRowsStopsWithoutReplayAndReleasesResponse() throws Exception {
    Schema schema = Schema.builder().addInt64Field("id").build();
    ClickHouseReadFn fn =
        new ClickHouseReadFn(
            read("SELECT CAST(number AS Int64) AS id FROM numbers(10000)", schema));
    fn.setup();
    AtomicInteger emitted = new AtomicInteger();
    try {
      IOException error =
          assertThrows(
              IOException.class,
              () ->
                  fn.process(
                      ClickHouseReadPartition.singleQuery(),
                      receiver(
                          row -> {
                            if (emitted.incrementAndGet() == 3) {
                              Thread.currentThread().interrupt();
                            }
                          })));
      assertEquals(3, emitted.get());
      assertTrue(Thread.currentThread().isInterrupted());
      assertTrue(error.getMessage().contains("interrupted"));
      Thread.interrupted(); // Clear the test's interruption before exercising connection reuse.
      AtomicInteger nextQueryCount = new AtomicInteger();
      fn.process(
          ClickHouseReadPartition.singleQuery(), receiver(row -> nextQueryCount.incrementAndGet()));
      assertEquals(10000, nextQueryCount.get());
    } finally {
      Thread.interrupted();
      fn.tearDown();
    }
  }
}
