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

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.List;

/** Generates non-overlapping ranges covering an inclusive numeric interval. */
final class ClickHouseRangePartitioner {
  private ClickHouseRangePartitioner() {}

  static long inclusiveCardinality(long lower, long upper) {
    checkArgument(lower <= upper, "Lower bound must be less than or equal to upper bound");
    try {
      return Math.addExact(Math.subtractExact(upper, lower), 1L);
    } catch (ArithmeticException e) {
      throw new IllegalArgumentException(
          "Inclusive range cardinality must fit in a signed long; this is a v1 implementation restriction");
    }
  }

  static final int MAX_PARTITIONS = 10_000;

  static int effectivePartitionCount(long cardinality, int requestedPartitions) {
    checkArgument(requestedPartitions >= 1, "Partition count must be at least 1");
    int count = (int) Math.min(requestedPartitions, cardinality);
    checkArgument(
        count <= MAX_PARTITIONS,
        "Effective partition count must not exceed %s; partitions are materialized in the pipeline graph",
        MAX_PARTITIONS);
    return count;
  }

  /** Distributes remainder values to the final partitions without computing upper + 1. */
  static List<ClickHouseReadRange> partition(long lower, long upper, int requestedPartitions) {
    long cardinality = inclusiveCardinality(lower, upper);
    int count = effectivePartitionCount(cardinality, requestedPartitions);
    long baseSize = cardinality / count;
    long remainder = cardinality % count;
    List<ClickHouseReadRange> result = new ArrayList<>(count);
    long nextLower = lower;
    for (int index = 0; index < count; index++) {
      if (index == count - 1) {
        result.add(ClickHouseReadRange.of(nextLower, upper, true));
      } else {
        long size = Math.addExact(baseSize, index >= count - remainder ? 1L : 0L);
        long nextBoundary = Math.addExact(nextLower, size);
        result.add(ClickHouseReadRange.of(nextLower, nextBoundary, false));
        nextLower = nextBoundary;
      }
    }
    return result;
  }
}
