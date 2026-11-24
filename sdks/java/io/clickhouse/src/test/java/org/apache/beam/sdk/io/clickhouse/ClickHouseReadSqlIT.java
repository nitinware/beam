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

import com.clickhouse.client.api.Client;
import com.clickhouse.client.api.data_formats.ClickHouseBinaryFormatReader;
import com.clickhouse.client.api.query.QueryResponse;
import com.clickhouse.client.api.query.QuerySettings;
import com.clickhouse.data.ClickHouseFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Server verification of generated SQL, projection and typed boundaries. */
@RunWith(JUnit4.class)
public class ClickHouseReadSqlIT extends BaseClickHouseTest {
  private static Client newClient() {
    return new Client.Builder()
        .addEndpoint(clickHouseUrl)
        .setDefaultDatabase(database)
        .setUsername(clickHouse.getUsername())
        .setPassword(clickHouse.getPassword())
        .build();
  }

  private static Map<String, Object> identifiers(String table, String key, String value) {
    Map<String, Object> result = new HashMap<>();
    result.put("table_name", table);
    result.put("key_name", key);
    result.put("value_name", value);
    return result;
  }

  private static void execute(Client client, String sql, Map<String, Object> parameters)
      throws Exception {
    try (QueryResponse ignored =
        client.query(sql, parameters).get(CLIENT_TIMEOUT, TimeUnit.SECONDS)) {
      // Close the response even for statements without result rows.
    }
  }

  private static List<Row> rows(
      Client client, String sql, Map<String, Object> parameters, Schema schema) throws Exception {
    List<Row> result = new ArrayList<>();
    try (QueryResponse response =
            client
                .query(
                    sql,
                    parameters,
                    new QuerySettings().setFormat(ClickHouseFormat.RowBinaryWithNamesAndTypes))
                .get(CLIENT_TIMEOUT, TimeUnit.SECONDS);
        ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response)) {
      // The generated table query must project only the schema fields, not the partition key.
      assertEquals(schema.getFieldCount(), reader.getSchema().getColumns().size());
      ClickHouseRowReader rowReader = ClickHouseRowReader.bind(schema, reader.getSchema());
      while (reader.hasNext()) {
        reader.next();
        result.add(rowReader.readRow(reader::readValue));
      }
    }
    return result;
  }

  private static ClickHouseIO.Read configuration(
      String table, String key, Schema schema, long lower, long upper, int count) {
    return ClickHouseIO.read("jdbc:clickhouse://unreachable.invalid/default")
        .withTable(table)
        .withSchema(schema)
        .withPartitionColumn(key)
        .withLowerBound(lower)
        .withUpperBound(upper)
        .withNumPartitions(count);
  }

  private static List<Row> partitionedRows(
      Client client, ClickHouseIO.Read read, Schema schema, long lower, long upper, int count)
      throws Exception {
    List<Row> result = new ArrayList<>();
    for (ClickHouseReadRange range : ClickHouseRangePartitioner.partition(lower, upper, count)) {
      ClickHouseReadSql sql =
          ClickHouseReadSql.forPartition(
              new ClickHouseReadConfiguration(read), ClickHouseReadPartition.forRange(range));
      result.addAll(rows(client, sql.sql(), sql.parameters(), schema));
    }
    return result;
  }

  private static Map<Row, Long> multiset(List<Row> rows) {
    return rows.stream().collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
  }

  private static void verifyProjectedReads(
      Client client, String table, String key, String value, String keyType) throws Exception {
    Map<String, Object> names = identifiers(table, key, value);
    // ClickHouse 23.8 does not accept identifier parameters in column declarations. Use backtick
    // quoting for fixture columns and independent Identifier parameters for the comparison SELECT.
    String keyDeclaration = "`" + key.replace("\\", "\\\\").replace("`", "\\`") + "`";
    String valueDeclaration = "`" + value.replace("\\", "\\\\").replace("`", "\\`") + "`";
    execute(
        client,
        "CREATE TABLE {table_name:Identifier} ("
            + keyDeclaration
            + " "
            + keyType
            + ", "
            + valueDeclaration
            + " String, unused String) ENGINE=Memory",
        names);
    execute(
        client,
        "INSERT INTO {table_name:Identifier} VALUES "
            + "(-1,'before','x'),(0,'zero','x'),(0,'zero','x'),(25,'quarter','x'),"
            + "(50,'half','x'),(75,'three quarters','x'),(100,'last','x'),(101,'after','x')",
        names);
    Schema schema = Schema.builder().addStringField(value).build();
    Map<String, Object> baselineParameters = new HashMap<>(names);
    baselineParameters.put("lo", 0L);
    baselineParameters.put("hi", 100L);
    List<Row> baseline =
        rows(
            client,
            "SELECT {value_name:Identifier} FROM {table_name:Identifier} "
                + "WHERE {key_name:Identifier} >= {lo:Int64} AND {key_name:Identifier} <= {hi:Int64}",
            baselineParameters,
            schema);
    assertEquals(6, baseline.size());
    for (int count : new int[] {1, 4, 10}) {
      ClickHouseIO.Read read = configuration(table, key, schema, 0, 100, count);
      assertEquals(
          multiset(baseline), multiset(partitionedRows(client, read, schema, 0, 100, count)));
    }
  }

  @Test
  public void int32AndInt64RangeQueriesMatchBoundedSingleQueryByMultiset() throws Exception {
    try (Client client = newClient()) {
      verifyProjectedReads(client, "beam_sql_int32", "id", "label", "Int32");
      verifyProjectedReads(client, "beam_sql_int64", "id", "label", "Int64");
    }
  }

  @Test
  public void quotesBackslashesUnicodeAndSqlPunctuationRemainLiteralIdentifiers() throws Exception {
    try (Client client = newClient()) {
      verifyProjectedReads(client, "beam_sql_雪_\"\\; --", "key\"\\; --", "label\"\\tail", "Int64");
    }
  }

  @Test
  public void typedParametersPreserveLongExtremeAndSingletonBounds() throws Exception {
    try (Client client = newClient()) {
      Map<String, Object> parameters = identifiers("beam_sql_extremes", "id", "label");
      execute(client, "CREATE TABLE {table_name:Identifier} (id Int64) ENGINE=Memory", parameters);
      parameters.put("a", Long.MIN_VALUE);
      parameters.put("b", Long.MIN_VALUE + 1);
      parameters.put("c", Long.MAX_VALUE - 1);
      parameters.put("d", Long.MAX_VALUE);
      execute(
          client,
          "INSERT INTO {table_name:Identifier} VALUES "
              + "({a:Int64}),({b:Int64}),({c:Int64}),({d:Int64})",
          parameters);
      Schema schema = Schema.builder().addInt64Field("id").build();
      for (long[] bounds :
          new long[][] {
            {Long.MIN_VALUE, Long.MIN_VALUE + 1}, {Long.MAX_VALUE - 1, Long.MAX_VALUE},
            {Long.MIN_VALUE, Long.MIN_VALUE}, {Long.MAX_VALUE, Long.MAX_VALUE}
          }) {
        parameters.put("lo", bounds[0]);
        parameters.put("hi", bounds[1]);
        List<Row> baseline =
            rows(
                client,
                "SELECT id FROM {table_name:Identifier} WHERE id >= {lo:Int64} AND id <= {hi:Int64}",
                parameters,
                schema);
        assertEquals(bounds[0] == bounds[1] ? 1 : 2, baseline.size());
        ClickHouseIO.Read read =
            configuration("beam_sql_extremes", "id", schema, bounds[0], bounds[1], 4);
        assertEquals(
            multiset(baseline),
            multiset(partitionedRows(client, read, schema, bounds[0], bounds[1], 4)));
      }
    }
  }
}
