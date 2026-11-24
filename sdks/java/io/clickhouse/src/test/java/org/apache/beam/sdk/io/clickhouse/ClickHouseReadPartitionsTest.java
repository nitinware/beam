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

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Local runner tests for work-item distribution, without a database connection. */
@RunWith(JUnit4.class)
public class ClickHouseReadPartitionsTest {
  private static final Schema SCHEMA = Schema.builder().addStringField("event_type").build();
  @Rule public TestPipeline pipeline = TestPipeline.create();

  private static ClickHouseIO.Read read() {
    return ClickHouseIO.read("jdbc:clickhouse://unreachable.invalid/default").withSchema(SCHEMA);
  }

  private static ClickHouseIO.Read tableRead(long lower, long upper, int count) {
    return read()
        .withTable("events")
        .withPartitionColumn("id")
        .withLowerBound(lower)
        .withUpperBound(upper)
        .withNumPartitions(count);
  }

  private static ClickHouseReadPartition range(long lower, long upper, boolean inclusive) {
    return ClickHouseReadPartition.forRange(ClickHouseReadRange.of(lower, upper, inclusive));
  }

  @Test
  public void queryProducesOneExplicitlyCodedWorkItem() {
    PCollection<ClickHouseReadPartition> work =
        pipeline.apply(
            new ClickHouseReadPartitions(read().withQuery("SELECT event_type FROM events")));
    assertEquals(ClickHouseReadPartitionCoder.of(), work.getCoder());
    PAssert.that(work).containsInAnyOrder(ClickHouseReadPartition.singleQuery());
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void rangesSurviveShuffleAndBecomeIndependentDoFnInputs() {
    PCollection<ClickHouseReadPartition> work =
        pipeline.apply(new ClickHouseReadPartitions(tableRead(0, 100, 4)));
    assertEquals(ClickHouseReadPartitionCoder.of(), work.getCoder());
    PAssert.that(work)
        .containsInAnyOrder(
            range(0, 25, false), range(25, 50, false), range(50, 75, false), range(75, 100, true));
    PCollection<Long> lowerBounds = work.apply(ParDo.of(new LowerBoundFn()));
    PAssert.that(lowerBounds).containsInAnyOrder(0L, 25L, 50L, 75L);
    pipeline.run().waitUntilFinish();
  }

  @Test
  public void cappedRangesRetainInclusiveExtremeUpperBoundAfterShuffle() {
    PCollection<ClickHouseReadPartition> work =
        pipeline.apply(
            new ClickHouseReadPartitions(tableRead(Long.MAX_VALUE - 1, Long.MAX_VALUE, 10)));
    PAssert.that(work)
        .containsInAnyOrder(
            range(Long.MAX_VALUE - 1, Long.MAX_VALUE, false),
            range(Long.MAX_VALUE, Long.MAX_VALUE, true));
    pipeline.run().waitUntilFinish();
  }

  private static final class LowerBoundFn extends DoFn<ClickHouseReadPartition, Long> {
    @ProcessElement
    public void process(@Element ClickHouseReadPartition partition, OutputReceiver<Long> output) {
      ClickHouseReadRange range = partition.range();
      if (range == null) {
        throw new IllegalStateException("Expected a range partition");
      }
      output.output(range.lowerBound());
    }
  }
}
