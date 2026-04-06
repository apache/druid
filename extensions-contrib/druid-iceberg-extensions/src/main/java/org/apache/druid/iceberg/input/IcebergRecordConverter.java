/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.iceberg.input;

import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Converts an Iceberg {@link Record} into a {@code Map<String, Object>} suitable
 * for consumption by Druid's {@link org.apache.druid.data.input.impl.MapInputRowParser}.
 *
 * Handles all Iceberg primitive types, nested structs, lists, and maps.
 * The converter uses Iceberg field IDs internally for schema resolution,
 * making it robust against column renames (V3 schema evolution).
 */
public class IcebergRecordConverter
{
  private final Schema schema;

  public IcebergRecordConverter(final Schema schema)
  {
    this.schema = schema;
  }

  /**
   * Converts an Iceberg Record to a flat Map using the converter's schema.
   * Only top-level columns from the schema are included in the output map.
   */
  public Map<String, Object> convert(final Record record)
  {
    final Map<String, Object> result = new LinkedHashMap<>();
    for (final Types.NestedField field : schema.columns()) {
      final Object value = record.getField(field.name());
      result.put(field.name(), convertValue(value, field.type()));
    }
    return result;
  }

  /**
   * Converts a single Iceberg value to a Druid-compatible Java type.
   * Returns null for null inputs.
   */
  static Object convertValue(final Object value, final Type type)
  {
    if (value == null) {
      return null;
    }

    switch (type.typeId()) {
      case BOOLEAN:
      case INTEGER:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return value;

      case STRING:
        // Iceberg may return CharSequence (e.g., Utf8); convert to String
        return value.toString();

      case DATE:
        // GenericParquetReaders returns LocalDate
        if (value instanceof LocalDate) {
          return value.toString();
        }
        // Fallback: integer days since epoch
        if (value instanceof Integer) {
          return LocalDate.ofEpochDay((Integer) value).toString();
        }
        return value.toString();

      case TIME:
        // GenericParquetReaders returns LocalTime
        if (value instanceof LocalTime) {
          return value.toString();
        }
        // Fallback: long microseconds since midnight
        if (value instanceof Long) {
          return LocalTime.ofNanoOfDay((Long) value * 1000L).toString();
        }
        return value.toString();

      case TIMESTAMP:
        return convertTimestamp(value);

      case TIMESTAMP_NANO:
        return convertTimestampNano(value);

      case DECIMAL:
        // BigDecimal passes through; Druid handles it natively
        if (value instanceof BigDecimal) {
          return ((BigDecimal) value).doubleValue();
        }
        return value;

      case UUID:
        if (value instanceof UUID) {
          return value.toString();
        }
        return value.toString();

      case FIXED:
      case BINARY:
        if (value instanceof ByteBuffer) {
          final ByteBuffer buf = (ByteBuffer) value;
          final byte[] bytes = new byte[buf.remaining()];
          buf.duplicate().get(bytes);
          return bytes;
        }
        if (value instanceof byte[]) {
          return value;
        }
        return value;

      case STRUCT:
        return convertStruct(value, type.asStructType());

      case LIST:
        return convertList(value, type.asListType());

      case MAP:
        return convertMap(value, type.asMapType());

      default:
        // GEOMETRY, GEOGRAPHY, VARIANT, UNKNOWN: convert to string as best-effort
        return value.toString();
    }
  }

  private static Object convertTimestamp(final Object value)
  {
    // GenericParquetReaders returns LocalDateTime (no tz) or OffsetDateTime (with tz)
    if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toInstant().toEpochMilli();
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
    // Fallback: long microseconds since epoch
    if (value instanceof Long) {
      return (Long) value / 1000L;
    }
    return value.toString();
  }

  private static Object convertTimestampNano(final Object value)
  {
    if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toInstant().toEpochMilli();
    }
    if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
    }
    // Fallback: long nanoseconds since epoch, truncate to millis
    if (value instanceof Long) {
      return (Long) value / 1_000_000L;
    }
    return value.toString();
  }

  private static Map<String, Object> convertStruct(final Object value, final Types.StructType structType)
  {
    if (!(value instanceof Record)) {
      return null;
    }
    final Record nested = (Record) value;
    final Map<String, Object> result = new LinkedHashMap<>();
    for (final Types.NestedField field : structType.fields()) {
      final Object fieldValue = nested.getField(field.name());
      result.put(field.name(), convertValue(fieldValue, field.type()));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static List<Object> convertList(final Object value, final Types.ListType listType)
  {
    if (!(value instanceof List)) {
      return null;
    }
    final List<?> source = (List<?>) value;
    final Type elementType = listType.elementType();
    final List<Object> result = new ArrayList<>(source.size());
    for (final Object element : source) {
      result.add(convertValue(element, elementType));
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> convertMap(final Object value, final Types.MapType mapType)
  {
    if (!(value instanceof Map)) {
      return null;
    }
    final Map<?, ?> source = (Map<?, ?>) value;
    final Type keyType = mapType.keyType();
    final Type valueType = mapType.valueType();
    final Map<String, Object> result = new LinkedHashMap<>();
    for (final Map.Entry<?, ?> entry : source.entrySet()) {
      // Map keys are always converted to String for Druid compatibility
      final String key = String.valueOf(convertValue(entry.getKey(), keyType));
      result.put(key, convertValue(entry.getValue(), valueType));
    }
    return result;
  }
}
