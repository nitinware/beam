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

import com.google.auto.value.AutoValue;
import java.io.Serializable;

/** A numeric read range with an inclusive lower boundary. */
@AutoValue
abstract class ClickHouseReadRange implements Serializable {
  abstract long lowerBound();

  abstract long upperBound();

  abstract boolean upperInclusive();

  static ClickHouseReadRange of(long lower, long upper, boolean upperInclusive) {
    checkArgument(upperInclusive ? lower <= upper : lower < upper, "Read range must not be empty");
    return new AutoValue_ClickHouseReadRange(lower, upper, upperInclusive);
  }
}
