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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Range coverage tests with an independent arbitrary-precision arithmetic oracle. */
@RunWith(JUnit4.class)
public class ClickHouseRangePartitionerTest {
  private static List<ClickHouseReadRange> ranges(long lower, long upper, int requested) {
    List<ClickHouseReadRange> result = new ArrayList<>();
    ClickHouseRangePartitioner.partition(lower, upper, requested).forEach(result::add);
    return result;
  }

  @Test
  public void agreedExampleIncludesUpperEndpointOnlyInFinalPartition() {
    assertEquals(
        Arrays.asList(
            ClickHouseReadRange.of(0, 25, false), ClickHouseReadRange.of(25, 50, false),
            ClickHouseReadRange.of(50, 75, false), ClickHouseReadRange.of(75, 100, true)),
        ranges(0, 100, 4));
  }

  @Test
  public void singlePartitionIncludesBothBounds() {
    assertEquals(Arrays.asList(ClickHouseReadRange.of(-5, 100, true)), ranges(-5, 100, 1));
    assertCoverage(1, Long.MAX_VALUE, 1);
  }

  @Test
  public void equalBoundsAtBothExtremesReadExactlyOneKey() {
    for (long key : new long[] {Long.MIN_VALUE, -1, 0, Long.MAX_VALUE}) {
      assertEquals(
          Arrays.asList(ClickHouseReadRange.of(key, key, true)),
          ranges(key, key, Integer.MAX_VALUE));
      assertCoverage(key, key, 1);
    }
  }

  @Test
  public void partitionCountIsCappedAtInclusiveCardinality() {
    assertEquals(
        Arrays.asList(
            ClickHouseReadRange.of(7, 8, false),
            ClickHouseReadRange.of(8, 9, false),
            ClickHouseReadRange.of(9, 9, true)),
        ranges(7, 9, Integer.MAX_VALUE));
  }

  @Test
  public void unevenRemainderGoesToLastPartitions() {
    assertEquals(
        Arrays.asList(
            ClickHouseReadRange.of(0, 2, false),
            ClickHouseReadRange.of(2, 5, false),
            ClickHouseReadRange.of(5, 7, true)),
        ranges(0, 7, 3));
    assertCoverage(0, 7, 3);
  }

  @Test
  public void exhaustiveSmallIntervalsCoverEachKeyExactlyOnceAndExcludeOutsideValues() {
    for (long lower = -8; lower <= 8; lower++) {
      for (long upper = lower; upper <= 8; upper++) {
        for (int requested = 1; requested <= 20; requested++) {
          List<ClickHouseReadRange> partitions = ranges(lower, upper, requested);
          assertCoverage(lower, upper, requested);
          for (long value = lower - 1; value <= upper + 1; value++) {
            int matches = 0;
            for (ClickHouseReadRange range : partitions) {
              if (value >= range.lowerBound()
                  && (range.upperInclusive()
                      ? value <= range.upperBound()
                      : value < range.upperBound())) {
                matches++;
              }
            }
            assertEquals(value >= lower && value <= upper ? 1 : 0, matches);
          }
        }
      }
    }
  }

  @Test
  public void extremeAndCrossZeroIntervalsHaveExactCoverageWithoutOverflow() {
    assertCoverage(Long.MIN_VALUE, Long.MIN_VALUE + 100, 4);
    assertCoverage(Long.MAX_VALUE - 100, Long.MAX_VALUE, 4);
    assertCoverage(Long.MIN_VALUE, -2, 4);
    assertCoverage(1, Long.MAX_VALUE, 3);
    assertCoverage(-4_000_000_000L, 4_000_000_000L, 7);
    assertCoverage(-5, 5, 4);
  }

  @Test
  public void invalidBoundsCountsAndUnrepresentableCardinalityAreRejectedImmediately() {
    assertThrows(
        IllegalArgumentException.class, () -> ClickHouseRangePartitioner.partition(2, 1, 1));
    assertThrows(
        IllegalArgumentException.class, () -> ClickHouseRangePartitioner.partition(0, 1, 0));
    assertThrows(
        IllegalArgumentException.class, () -> ClickHouseRangePartitioner.partition(0, 1, -1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseRangePartitioner.partition(Long.MIN_VALUE, Long.MAX_VALUE, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseRangePartitioner.partition(0, Long.MAX_VALUE, 1));
    assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseRangePartitioner.partition(Long.MIN_VALUE, -1, 1));
  }

  @Test(timeout = 5000)
  public void excessivePartitionCountsAreRejectedBeforeAllocatingDescriptors() {
    assertThrows(
        IllegalArgumentException.class,
        () -> ClickHouseRangePartitioner.partition(1, Long.MAX_VALUE, Integer.MAX_VALUE));
    assertEquals(10000, ClickHouseRangePartitioner.partition(0, 9999, 10000).size());
  }

  @Test
  public void iteratorsAreIndependentAndSignalExhaustion() {
    Iterable<ClickHouseReadRange> iterable = ClickHouseRangePartitioner.partition(0, 100, 4);
    Iterator<ClickHouseReadRange> first = iterable.iterator();
    Iterator<ClickHouseReadRange> second = iterable.iterator();
    assertEquals(first.next(), second.next());
    first.next();
    assertEquals(ClickHouseReadRange.of(25, 50, false), second.next());
    first.next();
    first.next();
    assertFalse(first.hasNext());
    assertThrows(NoSuchElementException.class, first::next);
    List<ClickHouseReadRange> repeated = new ArrayList<>();
    iterable.forEach(repeated::add);
    assertEquals(ranges(0, 100, 4), repeated);
  }

  private static void assertCoverage(long lower, long upper, int requested) {
    List<ClickHouseReadRange> partitions = ranges(lower, upper, requested);
    BigInteger cardinality =
        BigInteger.valueOf(upper).subtract(BigInteger.valueOf(lower)).add(BigInteger.ONE);
    assertEquals(cardinality.min(BigInteger.valueOf(requested)).intValueExact(), partitions.size());
    BigInteger next = BigInteger.valueOf(lower);
    BigInteger total = BigInteger.ZERO;
    BigInteger minSize = cardinality;
    BigInteger maxSize = BigInteger.ZERO;
    BigInteger previousSize = BigInteger.ZERO;
    for (int i = 0; i < partitions.size(); i++) {
      ClickHouseReadRange range = partitions.get(i);
      assertEquals(next, BigInteger.valueOf(range.lowerBound()));
      assertEquals(i == partitions.size() - 1, range.upperInclusive());
      next =
          BigInteger.valueOf(range.upperBound())
              .add(range.upperInclusive() ? BigInteger.ONE : BigInteger.ZERO);
      BigInteger size = next.subtract(BigInteger.valueOf(range.lowerBound()));
      assertTrue(size.signum() > 0);
      assertTrue(size.compareTo(previousSize) >= 0);
      previousSize = size;
      total = total.add(size);
      minSize = minSize.min(size);
      maxSize = maxSize.max(size);
    }
    assertEquals(BigInteger.valueOf(upper).add(BigInteger.ONE), next);
    assertEquals(cardinality, total);
    assertTrue(maxSize.subtract(minSize).compareTo(BigInteger.ONE) <= 0);
  }
}
