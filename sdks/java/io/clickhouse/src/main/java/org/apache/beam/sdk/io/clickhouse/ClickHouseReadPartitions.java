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

import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

/** Creates explicitly coded work items and a shuffle boundary for partitioned reads. */
final class ClickHouseReadPartitions
    extends PTransform<PBegin, PCollection<ClickHouseReadPartition>> {
  private final ClickHouseIO.Read configuration;

  ClickHouseReadPartitions(ClickHouseIO.Read configuration) {
    this.configuration = configuration;
  }

  @Override
  public PCollection<ClickHouseReadPartition> expand(PBegin input) {
    if (configuration.query() != null) {
      return input.apply(
          "QueryWorkItem",
          Create.of(ClickHouseReadPartition.singleQuery())
              .withCoder(ClickHouseReadPartitionCoder.of()));
    }
    Long lower = configuration.lowerBound();
    Long upper = configuration.upperBound();
    Integer count = configuration.numPartitions();
    if (lower == null || upper == null || count == null) {
      throw new IllegalArgumentException("Missing partition bounds or count");
    }
    List<ClickHouseReadPartition> partitions =
        ClickHouseRangePartitioner.partition(lower, upper, count).stream()
            .map(ClickHouseReadPartition::forRange)
            .collect(Collectors.toList());
    // Create materializes work items in the graph. Scheduling/concurrency remain runner-dependent.
    return input
        .apply("RangeWorkItems", Create.of(partitions).withCoder(ClickHouseReadPartitionCoder.of()))
        .apply("DistributeRanges", Reshuffle.viaRandomKey());
  }
}
