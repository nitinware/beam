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

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Executes bounded read work items on a worker without replaying partially emitted results. */
final class ClickHouseReadFn extends DoFn<ClickHouseReadPartition, Row> {
  private final ClickHouseReadConfiguration configuration;
  private transient @Nullable Client client;

  ClickHouseReadFn(ClickHouseIO.Read configuration) {
    this.configuration = new ClickHouseReadConfiguration(configuration);
  }

  @Setup
  public void setup() throws IOException {
    Properties properties = configuration.properties();
    Map<String, String> options =
        properties.stringPropertyNames().stream()
            .filter(key -> !key.equals("user") && !key.equals("password"))
            .collect(Collectors.toMap(key -> key, key -> properties.getProperty(key, "")));
    Client created;
    try {
      Client.Builder builder =
          new Client.Builder()
              .addEndpoint(configuration.clickHouseUrl())
              .setDefaultDatabase(configuration.database())
              .setUsername(properties.getProperty("user", "default"))
              .setPassword(properties.getProperty("password", ""))
              .setOptions(options)
              .setClientName(ClickHouseIO.buildClientName(properties));
      if (Boolean.parseBoolean(properties.getProperty("compress", "false"))) {
        builder.compressServerResponse(true).compressClientRequest(true);
      }
      created = builder.build();
    } catch (Exception e) {
      throw new IOException("Unable to create ClickHouse read client", e);
    }
    try {
      if (configuration.table() != null) {
        ClickHousePartitionValidator.validate(created, configuration);
      }
      client = created;
    } catch (Exception e) {
      // Beam need not invoke @Teardown when @Setup fails, so close this client immediately.
      try {
        created.close();
      } catch (Exception closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw new IOException("Unable to validate ClickHouse partition metadata", e);
    }
  }

  @ProcessElement
  public void process(@Element ClickHouseReadPartition partition, OutputReceiver<Row> output)
      throws IOException {
    Client activeClient = client;
    Schema schema = configuration.schema();
    if (activeClient == null || schema == null) {
      throw new IllegalStateException("ClickHouse query reader has not been initialized");
    }
    ClickHouseReadSql statement = ClickHouseReadSql.forPartition(configuration, partition);
    RuntimeException downstreamFailure = null;
    QuerySettings settings =
        new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes);
    try {
      ClickHouseReadResources.checkInterrupted();
      try (QueryResponse response =
              ClickHouseReadResources.await(
                  activeClient.query(statement.sql(), statement.parameters(), settings));
          ClickHouseBinaryFormatReader reader = activeClient.newBinaryFormatReader(response)) {
        ClickHouseRowReader rowReader = ClickHouseRowReader.bind(schema, reader.getSchema());
        while (true) {
          ClickHouseReadResources.checkInterrupted();
          if (!reader.hasNext()) {
            break;
          }
          reader.next();
          Row row = rowReader.readRow(reader::readValue);
          try {
            output.output(row);
          } catch (RuntimeException e) {
            // Fused downstream transforms execute here. Preserve their exception identity and
            // stack, while try-with-resources still closes this query's response.
            downstreamFailure = e;
            throw e;
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("ClickHouse query read interrupted", e);
    } catch (Exception e) {
      if (e == downstreamFailure) {
        throw downstreamFailure;
      }
      // Never restart a partially emitted query; the runner owns bundle retries.
      throw new IOException("ClickHouse query read failed", e);
    }
  }

  @Teardown
  public void tearDown() throws IOException {
    Client activeClient = client;
    client = null;
    if (activeClient != null) {
      try {
        activeClient.close();
      } catch (Exception e) {
        throw new IOException("Unable to close ClickHouse read client", e);
      }
    }
  }
}
