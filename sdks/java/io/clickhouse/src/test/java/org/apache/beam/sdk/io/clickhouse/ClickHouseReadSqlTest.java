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

import java.util.HashMap;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** SQL projection, identifier escaping and parameter-binding contract tests. */
@RunWith(JUnit4.class)
public class ClickHouseReadSqlTest {
  private static final Schema SCHEMA =
      Schema.builder().addStringField("event_type").addStringField("label").build();

  private static ClickHouseIO.Read tableRead() {
    return ClickHouseIO.read("jdbc:clickhouse://unreachable.invalid/default")
        .withTable("events")
        .withSchema(SCHEMA)
        .withPartitionColumn("id")
        .withLowerBound(0)
        .withUpperBound(100)
        .withNumPartitions(4);
  }

  private static ClickHouseReadPartition range(long lower, long upper, boolean inclusive) {
    return ClickHouseReadPartition.forRange(ClickHouseReadRange.of(lower, upper, inclusive));
  }

  @Test
  public void projectionUsesSchemaOrderAndOmitsUnselectedPartitionColumn() {
    ClickHouseReadSql sql =
        ClickHouseReadSql.forPartition(
            new ClickHouseReadConfiguration(tableRead()), range(0, 25, false));
    assertEquals(
        "SELECT \"event_type\", \"label\" FROM \"events\" "
            + "WHERE \"id\" >= {beam_lower:Int64} AND \"id\" < {beam_upper:Int64}",
        sql.sql());
    Map<String, Object> expected = new HashMap<>();
    expected.put("beam_lower", 0L);
    expected.put("beam_upper", 25L);
    assertEquals(expected, sql.parameters());
  }

  @Test
  public void finalPartitionIncludesUpperBoundAndParametersRemainLongs() {
    ClickHouseReadSql sql =
        ClickHouseReadSql.forPartition(
            new ClickHouseReadConfiguration(tableRead()), range(75, 100, true));
    assertTrue(sql.sql().endsWith("\"id\" <= {beam_upper:Int64}"));
    assertEquals(Long.valueOf(75), sql.parameters().get("beam_lower"));
    assertEquals(Long.valueOf(100), sql.parameters().get("beam_upper"));
    assertThrows(
        UnsupportedOperationException.class, () -> sql.parameters().put("beam_lower", -1L));
  }

  @Test
  public void equalExtremeBoundsAreBoundWithoutIncrementingOrInterpolating() {
    for (long key : new long[] {Long.MIN_VALUE, Long.MAX_VALUE}) {
      ClickHouseIO.Read read =
          tableRead().withLowerBound(key).withUpperBound(key).withNumPartitions(1);
      ClickHouseReadSql sql =
          ClickHouseReadSql.forPartition(
              new ClickHouseReadConfiguration(read), range(key, key, true));
      assertFalse(sql.sql().contains(Long.toString(key)));
      assertTrue(sql.sql().contains("<= {beam_upper:Int64}"));
      assertEquals(Long.valueOf(key), sql.parameters().get("beam_lower"));
      assertEquals(Long.valueOf(key), sql.parameters().get("beam_upper"));
    }
  }

  @Test
  public void identifiersEscapeQuotesAndLiteralBackslashes() {
    Schema schema = Schema.builder().addStringField("a\"b\\c").build();
    ClickHouseReadSql sql =
        ClickHouseReadSql.forPartition(
            new ClickHouseReadConfiguration(
                tableRead().withTable("events\\").withSchema(schema).withPartitionColumn("order")),
            range(0, 100, true));
    assertEquals(
        "SELECT \"a\\\"b\\\\c\" FROM \"events\\\\\" "
            + "WHERE \"order\" >= {beam_lower:Int64} AND \"order\" <= {beam_upper:Int64}",
        sql.sql());
  }

  @Test
  public void punctuationAndDotsStayInsideOneQuotedIdentifier() {
    ClickHouseReadSql sql =
        ClickHouseReadSql.forPartition(
            new ClickHouseReadConfiguration(tableRead().withTable("events.archive; --")),
            range(0, 100, true));
    assertTrue(sql.sql().contains("FROM \"events.archive; --\" WHERE"));
  }

  @Test
  public void querySqlIsPreservedExactlyWithoutInjectedPredicates() {
    String query = "SELECT 'private text' AS event_type, 'x' AS label WHERE 1\nLIMIT 2";
    ClickHouseIO.Read read =
        ClickHouseIO.read("jdbc:clickhouse://unreachable.invalid/default")
            .withQuery(query)
            .withSchema(SCHEMA);
    ClickHouseReadSql sql =
        ClickHouseReadSql.forPartition(
            new ClickHouseReadConfiguration(read), ClickHouseReadPartition.singleQuery());
    assertEquals(query, sql.sql());
    assertTrue(sql.parameters().isEmpty());
    assertFalse(sql.toString().contains("private text"));
  }

  @Test
  public void modeAndWorkItemMustMatch() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ClickHouseReadSql.forPartition(
                new ClickHouseReadConfiguration(tableRead()),
                ClickHouseReadPartition.singleQuery()));
    ClickHouseIO.Read query =
        ClickHouseIO.read("jdbc:clickhouse://unreachable.invalid/default")
            .withSchema(SCHEMA)
            .withQuery("SELECT event_type, label FROM events");
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ClickHouseReadSql.forPartition(
                new ClickHouseReadConfiguration(query), range(0, 100, true)));
  }
}
