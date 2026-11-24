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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.SerializableUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Encoding and serialization tests for work items crossing a Beam shuffle. */
@RunWith(JUnit4.class)
public class ClickHouseReadPartitionCoderTest {
  private static final ClickHouseReadPartitionCoder CODER = ClickHouseReadPartitionCoder.of();
  private static final List<ClickHouseReadPartition> VALUES =
      Arrays.asList(
          ClickHouseReadPartition.singleQuery(),
          ClickHouseReadPartition.forRange(ClickHouseReadRange.of(0, 25, false)),
          ClickHouseReadPartition.forRange(ClickHouseReadRange.of(0, 25, true)),
          ClickHouseReadPartition.forRange(
              ClickHouseReadRange.of(Long.MIN_VALUE, Long.MIN_VALUE, true)),
          ClickHouseReadPartition.forRange(
              ClickHouseReadRange.of(Long.MAX_VALUE, Long.MAX_VALUE, true)),
          ClickHouseReadPartition.forRange(ClickHouseReadRange.of(Long.MIN_VALUE, -2, true)));

  @Test
  public void roundTripsAndSerializesQueryAndRangeDescriptors() throws Exception {
    CoderProperties.coderSerializable(CODER);
    for (ClickHouseReadPartition value : VALUES) {
      CoderProperties.coderDecodeEncodeEqual(CODER, value);
      assertEquals(value, SerializableUtils.clone(value));
    }
  }

  @Test
  public void encodingIsDeterministicAndConsistentWithEquality() throws Exception {
    CODER.verifyDeterministic();
    for (ClickHouseReadPartition first : VALUES) {
      CoderProperties.coderDeterministic(CODER, first, SerializableUtils.clone(first));
      for (ClickHouseReadPartition second : VALUES) {
        CoderProperties.coderConsistentWithEquals(CODER, first, second);
      }
    }
  }

  @Test
  public void sequentialValuesDecodeWithoutConsumingAdjacentRecords() throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    for (ClickHouseReadPartition value : VALUES) {
      CODER.encode(value, output);
    }
    ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
    for (ClickHouseReadPartition value : VALUES) {
      assertEquals(value, CODER.decode(input));
    }
    assertEquals(-1, input.read());
  }

  @Test
  public void nullUnknownTagsAndTruncatedRangesAreRejected() throws Exception {
    assertThrows(CoderException.class, () -> CODER.encode(null, new ByteArrayOutputStream()));
    assertThrows(IOException.class, () -> CODER.decode(new ByteArrayInputStream(new byte[] {2})));
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    CODER.encode(VALUES.get(5), output);
    byte[] complete = output.toByteArray();
    for (int length = 0; length < complete.length; length++) {
      byte[] truncated = Arrays.copyOf(complete, length);
      assertThrows(IOException.class, () -> CODER.decode(new ByteArrayInputStream(truncated)));
    }
  }

  @Test
  public void malformedEmptyRangesAreRejectedOnDecode() throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    output.write(1);
    VarLongCoder.of().encode(10L, output);
    VarLongCoder.of().encode(10L, output);
    output.write(0); // Equal boundaries cannot form a nonempty half-open range.
    assertThrows(
        CoderException.class, () -> CODER.decode(new ByteArrayInputStream(output.toByteArray())));
  }
}
