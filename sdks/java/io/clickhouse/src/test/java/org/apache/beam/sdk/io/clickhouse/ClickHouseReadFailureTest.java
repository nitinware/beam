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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.clickhouse.client.api.ClickHouseException;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.WindowedValues;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** HTTP fault injection using the actual client, without a ClickHouse server dependency. */
@RunWith(JUnit4.class)
public class ClickHouseReadFailureTest {
  private static final Schema SCHEMA =
      Schema.builder().addInt64Field("id").addInt64Field("value").build();

  private static ClickHouseReadFn reader(int port, int retries) {
    Properties properties = new Properties();
    properties.setProperty("retry", Integer.toString(retries));
    properties.setProperty("async", "true");
    properties.setProperty("compress", "false");
    properties.setProperty("max_open_connections", "1");
    properties.setProperty("connection_request_timeout", "2000");
    properties.setProperty("socket_timeout", "2000");
    return new ClickHouseReadFn(
        ClickHouseIO.read("jdbc:clickhouse://127.0.0.1:" + port + "/default")
            .withProperties(properties)
            .withQuery("SELECT private_query_text")
            .withSchema(SCHEMA));
  }

  private static OutputReceiver<Row> receiver(Consumer<Row> consumer) {
    return value ->
        WindowedValues.builder(WindowedValues.valueInGlobalWindow(value))
            .setReceiver(windowed -> consumer.accept(windowed.getValue()));
  }

  private static void consumeRequest(HttpExchange exchange) throws IOException {
    try (InputStream input = exchange.getRequestBody()) {
      byte[] buffer = new byte[4096];
      while (input.read(buffer) != -1) {
        // Drain the request so persistent connections can be reused.
      }
    }
  }

  private static byte[] body(boolean truncated) throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(2); // RowBinaryWithNamesAndTypes: two columns.
    for (String header : new String[] {"id", "value", "Int64", "Int64"}) {
      byte[] bytes = header.getBytes(StandardCharsets.UTF_8);
      output.write(bytes.length);
      output.write(bytes);
    }
    for (long id = 0; id < 2; id++) {
      output.write(
          ByteBuffer.allocate(16)
              .order(ByteOrder.LITTLE_ENDIAN)
              .putLong(id)
              .putLong(id + 100)
              .array());
    }
    if (truncated) {
      // A third row begins but its second column never arrives.
      output.write(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(2).array());
    }
    return output.toByteArray();
  }

  private static void respond(HttpExchange exchange, boolean truncated) throws IOException {
    byte[] bytes = body(truncated);
    exchange.getResponseHeaders().set("X-ClickHouse-Format", "RowBinaryWithNamesAndTypes");
    exchange.getResponseHeaders().set("X-ClickHouse-Timezone", "UTC");
    exchange.sendResponseHeaders(200, bytes.length + (truncated ? 8 : 0));
    try (OutputStream output = exchange.getResponseBody()) {
      output.write(bytes);
    } catch (IOException e) {
      if (!truncated) {
        throw e;
      }
      // Intentionally close before the advertised HTTP content length is satisfied.
    } finally {
      exchange.close();
    }
  }

  @Test(timeout = 30000)
  public void transientFailureBeforeResponseCanRetryWithoutDuplicatingRows() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          consumeRequest(exchange);
          if (requests.incrementAndGet() == 1) {
            exchange.getResponseHeaders().set("X-ClickHouse-Exception-Code", "202");
            exchange.sendResponseHeaders(503, -1);
            exchange.close();
          } else {
            respond(exchange, false);
          }
        });
    server.start();
    ClickHouseReadFn fn = reader(server.getAddress().getPort(), 2);
    try {
      fn.setup();
      List<Row> rows = new ArrayList<>();
      fn.process(ClickHouseReadPartition.singleQuery(), receiver(rows::add));
      assertEquals(2, requests.get());
      assertEquals(2, rows.size());
      assertEquals(Row.withSchema(SCHEMA).addValues(0L, 100L).build(), rows.get(0));
      assertEquals(Row.withSchema(SCHEMA).addValues(1L, 101L).build(), rows.get(1));
    } finally {
      fn.tearDown();
      server.stop(0);
    }
  }

  @Test(timeout = 30000)
  public void brokenResponseAfterPartialOutputDoesNotReplayAndReleasesConnection()
      throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          consumeRequest(exchange);
          respond(exchange, requests.incrementAndGet() == 1);
        });
    server.start();
    ClickHouseReadFn fn = reader(server.getAddress().getPort(), 3);
    try {
      fn.setup();
      List<Row> partial = new ArrayList<>();
      IOException error =
          assertThrows(
              IOException.class,
              () -> fn.process(ClickHouseReadPartition.singleQuery(), receiver(partial::add)));
      assertTrue(partial.size() > 0);
      assertEquals(1, requests.get());
      assertFalse(error.toString().contains("private_query_text"));
      assertTrue(error.getCause() instanceof ClickHouseException);
      List<Row> retryBundle = new ArrayList<>();
      fn.process(ClickHouseReadPartition.singleQuery(), receiver(retryBundle::add));
      assertEquals(2, requests.get());
      assertEquals(2, retryBundle.size());
    } finally {
      fn.tearDown();
      fn.tearDown(); // Teardown is idempotent.
      server.stop(0);
    }
  }

  @Test(timeout = 30000)
  public void downstreamFailureKeepsItsIdentityAndDoesNotReplayTheQuery() throws Exception {
    AtomicInteger requests = new AtomicInteger();
    HttpServer server =
        HttpServer.create(
            new InetSocketAddress(InetAddress.getByAddress(new byte[] {127, 0, 0, 1}), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          consumeRequest(exchange);
          requests.incrementAndGet();
          respond(exchange, false);
        });
    server.start();
    ClickHouseReadFn fn = reader(server.getAddress().getPort(), 3);
    try {
      fn.setup();
      NullPointerException injected = new NullPointerException("downstream failure");
      NullPointerException actual =
          assertThrows(
              NullPointerException.class,
              () ->
                  fn.process(
                      ClickHouseReadPartition.singleQuery(),
                      receiver(
                          row -> {
                            throw injected;
                          })));
      org.junit.Assert.assertSame(injected, actual);
      assertEquals(1, requests.get());
      List<Row> rows = new ArrayList<>();
      fn.process(ClickHouseReadPartition.singleQuery(), receiver(rows::add));
      assertEquals(2, rows.size());
      assertEquals(2, requests.get());
    } finally {
      fn.tearDown();
      server.stop(0);
    }
  }

  @Test
  public void invalidClientConfigurationPreservesOriginalCause() throws Exception {
    Properties properties = new Properties();
    properties.setProperty("password", "private_password");
    properties.setProperty("max_open_connections", "private_option_value");
    ClickHouseReadFn fn =
        new ClickHouseReadFn(
            ClickHouseIO.read("jdbc:clickhouse://127.0.0.1:1/default")
                .withProperties(properties)
                .withQuery("SELECT private_query_text")
                .withSchema(SCHEMA));
    try {
      IOException error = assertThrows(IOException.class, fn::setup);
      assertFalse(error.toString().contains("private_password"));
      assertTrue(error.getCause() instanceof NumberFormatException);
      assertNotNull(error.getCause());
    } finally {
      fn.tearDown();
    }
  }
}
