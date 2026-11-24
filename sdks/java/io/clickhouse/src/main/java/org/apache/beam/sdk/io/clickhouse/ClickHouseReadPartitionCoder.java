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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.BooleanCoder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Deterministic encoding of query markers and numeric range work items. */
final class ClickHouseReadPartitionCoder extends AtomicCoder<ClickHouseReadPartition> {
  private static final ClickHouseReadPartitionCoder INSTANCE = new ClickHouseReadPartitionCoder();
  private static final VarLongCoder LONG_CODER = VarLongCoder.of();
  private static final BooleanCoder BOOLEAN_CODER = BooleanCoder.of();

  private ClickHouseReadPartitionCoder() {}

  static ClickHouseReadPartitionCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(@Nullable ClickHouseReadPartition value, OutputStream output)
      throws IOException {
    if (value == null) {
      throw new CoderException("Cannot encode a null ClickHouse read partition");
    }
    ClickHouseReadRange range = value.range();
    output.write(range == null ? 0 : 1);
    if (range != null) {
      LONG_CODER.encode(range.lowerBound(), output);
      LONG_CODER.encode(range.upperBound(), output);
      BOOLEAN_CODER.encode(range.upperInclusive(), output);
    }
  }

  @Override
  public ClickHouseReadPartition decode(InputStream input) throws IOException {
    int kind = input.read();
    if (kind == 0) {
      return ClickHouseReadPartition.singleQuery();
    }
    if (kind != 1) {
      throw new CoderException("Invalid ClickHouse read partition tag");
    }
    long lower = LONG_CODER.decode(input);
    long upper = LONG_CODER.decode(input);
    boolean inclusive = BOOLEAN_CODER.decode(input);
    try {
      return ClickHouseReadPartition.forRange(ClickHouseReadRange.of(lower, upper, inclusive));
    } catch (IllegalArgumentException e) {
      throw new CoderException("Invalid ClickHouse read partition range");
    }
  }

  @Override
  public boolean consistentWithEquals() {
    return true;
  }
}
