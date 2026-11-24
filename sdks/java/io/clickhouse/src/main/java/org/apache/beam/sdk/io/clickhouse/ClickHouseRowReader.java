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

import static org.apache.beam.sdk.io.clickhouse.ClickHouseReadValidationException.check;

import com.clickhouse.data.ClickHouseColumn;
import com.clickhouse.data.ClickHouseDataType;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.joda.time.Instant;

/** Worker-side binding of result metadata and conversion to the caller's output schema. */
final class ClickHouseRowReader {
  private final Schema schema;
  private final List<Binding> bindings;

  private ClickHouseRowReader(Schema schema, List<Binding> bindings) {
    this.schema = schema;
    this.bindings = bindings;
  }

  /** Reject unsupported Beam output types without requiring database metadata. */
  static void validateSchema(Schema schema) {
    for (Schema.Field field : schema.getFields()) {
      switch (field.getType().getTypeName()) {
        case BYTE:
        case INT16:
        case INT32:
        case INT64:
        case FLOAT:
        case DOUBLE:
        case STRING:
        case BOOLEAN:
        case DATETIME:
          break;
        default:
          throw new ClickHouseReadValidationException(
              "Unsupported Beam output type for ClickHouse read: " + field.getType().getTypeName());
      }
    }
  }

  /** Bind before iterating, so metadata errors are also detected for an empty result. */
  static ClickHouseRowReader bind(
      Schema schema, com.clickhouse.client.api.metadata.TableSchema resultSchema) {
    validateSchema(schema);
    Map<String, Integer> indices = new HashMap<>();
    List<ClickHouseColumn> columns = resultSchema.getColumns();
    for (int i = 0; i < columns.size(); i++) {
      String name = columns.get(i).getColumnName();
      check(indices.put(name, i) == null, "Duplicate result column name '%s'", name);
    }
    List<Binding> bindings = new ArrayList<>();
    for (Schema.Field field : schema.getFields()) {
      Integer index = indices.get(field.getName());
      if (index == null) {
        throw new ClickHouseReadValidationException(
            "Result is missing output schema column '" + field.getName() + "'");
      }
      ClickHouseColumn column = columns.get(index);
      check(
          !column.isLowCardinality() && !column.isAggregateFunction(),
          "Unsupported ClickHouse type wrapper for field '%s'",
          field.getName());
      Schema.FieldType expected = beamType(column.getDataType());
      check(
          field.getType().withNullable(false).equals(expected),
          "ClickHouse result type %s is incompatible with Beam output type %s for field '%s'",
          column.getDataType(),
          field.getType().getTypeName(),
          field.getName());
      check(
          !column.isNullable() || field.getType().getNullable(),
          "Nullable ClickHouse result column '%s' requires a nullable Beam field",
          field.getName());
      // ClickHouseBinaryFormatReader uses one-based column indices.
      bindings.add(new Binding(field, index + 1, column.getDataType()));
    }
    return new ClickHouseRowReader(schema, bindings);
  }

  /** Convert the current row. The lookup must use the bound result's one-based column indices. */
  Row readRow(IntFunction<Object> valueAtIndex) {
    Row.Builder builder = Row.withSchema(schema);
    for (Binding binding : bindings) {
      Object value = valueAtIndex.apply(binding.index);
      if (value == null) {
        check(
            binding.field.getType().getNullable(),
            "Null result value for non-nullable Beam field '%s'",
            binding.field.getName());
        builder.addValue(null);
        continue;
      }
      check(
          valueClass(binding.type).isInstance(value),
          "Unexpected Java value type for ClickHouse result type %s in field '%s'",
          binding.type,
          binding.field.getName());
      if (binding.type == ClickHouseDataType.Date) {
        // A calendar date is not an instant. Use midnight UTC regardless of client display zone.
        ZonedDateTime date = (ZonedDateTime) value;
        value =
            new Instant(date.toLocalDate().atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
      } else if (binding.type == ClickHouseDataType.DateTime) {
        value = new Instant(((ZonedDateTime) value).toInstant().toEpochMilli());
      }
      builder.addValue(value);
    }
    return builder.build();
  }

  private static Schema.FieldType beamType(ClickHouseDataType type) {
    switch (type) {
      case Int8:
        return Schema.FieldType.BYTE;
      case Int16:
      case UInt8:
        return Schema.FieldType.INT16;
      case Int32:
      case UInt16:
        return Schema.FieldType.INT32;
      case Int64:
      case UInt32:
        return Schema.FieldType.INT64;
      case Float32:
        return Schema.FieldType.FLOAT;
      case Float64:
        return Schema.FieldType.DOUBLE;
      case String:
        return Schema.FieldType.STRING;
      case Bool:
        return Schema.FieldType.BOOLEAN;
      case Date:
      case DateTime:
        return Schema.FieldType.DATETIME;
      default:
        throw new ClickHouseReadValidationException(
            "Unsupported ClickHouse result type for read: " + type);
    }
  }

  private static Class<?> valueClass(ClickHouseDataType type) {
    switch (type) {
      case Int8:
        return Byte.class;
      case Int16:
      case UInt8:
        return Short.class;
      case Int32:
      case UInt16:
        return Integer.class;
      case Int64:
      case UInt32:
        return Long.class;
      case Float32:
        return Float.class;
      case Float64:
        return Double.class;
      case String:
        return String.class;
      case Bool:
        return Boolean.class;
      case Date:
      case DateTime:
        return ZonedDateTime.class;
      default:
        throw new ClickHouseReadValidationException(
            "Unsupported ClickHouse result type for read: " + type);
    }
  }

  private static final class Binding {
    private final Schema.Field field;
    private final int index;
    private final ClickHouseDataType type;

    private Binding(Schema.Field field, int index, ClickHouseDataType type) {
      this.field = field;
      this.index = index;
      this.type = type;
    }
  }
}
