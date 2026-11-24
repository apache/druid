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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.MapInputRowParser;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Static utility class for converting Iceberg {@link Record} objects to Druid {@link InputRow}.
 */
public class IcebergRecordConverter
{
  private static final long MILLIS_PER_DAY = 86_400_000L;

  private IcebergRecordConverter()
  {
  }

  /**
   * Convert an Iceberg {@link Record} to a {@link Map} of field name → Java value.
   * Iceberg primitive types are passed through; temporal and binary types are converted
   * to representations suitable for Druid ingestion.
   *
   * @param binaryAsString mirrors {@code ParquetInputFormat}'s {@code binaryAsString} option:
   *                       when true, binary/fixed values are decoded as UTF-8 strings instead
   *                       of returned as {@code byte[]}.
   */
  public static Map<String, Object> convertToMap(Record record, Schema schema, boolean binaryAsString)
  {
    return convertToMap(record, schema, binaryAsString, true);
  }

  /**
   * Same as {@link #convertToMap(Record, Schema, boolean)}, but when {@code discoverNestedFields} is false, the
   * returned map hides struct, list and map columns from key iteration (and so from schemaless dimension discovery)
   * while still returning them from {@code get}, so explicitly named dimensions and transforms keep working. This
   * mirrors {@code ParquetGroupFlattenerMaker#discoverRootFields}, which only discovers primitive root fields
   * unless schema discovery is enabled.
   *
   * @param discoverNestedFields typically {@code DimensionsSpec#useSchemaDiscovery()}
   */
  public static Map<String, Object> convertToMap(
      Record record,
      Schema schema,
      boolean binaryAsString,
      boolean discoverNestedFields
  )
  {
    Map<String, Object> result = new LinkedHashMap<>();
    Set<String> nonDiscoverable = new HashSet<>();
    for (Types.NestedField field : schema.columns()) {
      String name = field.name();
      Object value = record.getField(name);
      result.put(name, convertValue(value, field.type(), binaryAsString));
      if (!discoverNestedFields && !field.type().isPrimitiveType()) {
        nonDiscoverable.add(name);
      }
    }
    return nonDiscoverable.isEmpty() ? result : new DiscoveryRestrictedMap(result, nonDiscoverable);
  }

  /**
   * A read-through view of a row whose iteration skips {@code hiddenKeys} but whose lookups still resolve them.
   */
  private static class DiscoveryRestrictedMap extends AbstractMap<String, Object>
  {
    private final Map<String, Object> delegate;
    private final Set<Map.Entry<String, Object>> discoverableEntries;

    DiscoveryRestrictedMap(Map<String, Object> delegate, Set<String> hiddenKeys)
    {
      this.delegate = delegate;
      this.discoverableEntries = new LinkedHashSet<>();
      for (Map.Entry<String, Object> entry : delegate.entrySet()) {
        if (!hiddenKeys.contains(entry.getKey())) {
          discoverableEntries.add(entry);
        }
      }
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet()
    {
      return Collections.unmodifiableSet(discoverableEntries);
    }

    @Override
    public Object get(Object key)
    {
      return delegate.get(key);
    }

    @Override
    public boolean containsKey(Object key)
    {
      return delegate.containsKey(key);
    }
  }

  /**
   * Convert a raw-value map plus an {@link InputRowSchema} into an {@link InputRow}, resolving
   * dimensions with the same {@link MapInputRowParser#findDimensions} semantics used by the
   * standard Druid input-format path (schema discovery, include-all, and exclusions all apply).
   */
  public static InputRow toInputRow(Map<String, Object> row, InputRowSchema inputRowSchema)
  {
    return MapInputRowParser.parse(inputRowSchema, row);
  }

  private static Object convertValue(Object value, Type type, boolean binaryAsString)
  {
    if (value == null) {
      return null;
    }
    if (type instanceof Types.TimestampNanoType) {
      // Iceberg V3 nanosecond timestamps; Druid timestamps are millis, as for the microsecond type below.
      if (value instanceof OffsetDateTime) {
        return ((OffsetDateTime) value).toInstant().toEpochMilli();
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
      }
      if (value instanceof Long) {
        return Math.floorDiv((Long) value, 1_000_000L);
      }
      return value;
    }
    if (type instanceof Types.TimestampType) {
      if (value instanceof OffsetDateTime) {
        return ((OffsetDateTime) value).toInstant().toEpochMilli();
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
      }
      if (value instanceof Long) {
        // Iceberg timestamps are stored as microseconds since epoch; Druid expects milliseconds.
        return Math.floorDiv((Long) value, 1000L);
      }
      return value;
    }
    if (type instanceof Types.DateType) {
      // Epoch millis, matching the existing Parquet and Arrow readers.
      if (value instanceof LocalDate) {
        return ((LocalDate) value).toEpochDay() * MILLIS_PER_DAY;
      }
      if (value instanceof Integer) {
        // Iceberg dates are stored as days since epoch.
        return (Integer) value * MILLIS_PER_DAY;
      }
      return value;
    }
    if (type instanceof Types.TimeType) {
      // Millis since midnight, matching the Arrow reader.
      if (value instanceof LocalTime) {
        return ((LocalTime) value).toNanoOfDay() / 1_000_000L;
      }
      if (value instanceof Long) {
        // Iceberg times are stored as microseconds since midnight.
        return Math.floorDiv((Long) value, 1000L);
      }
      return value;
    }
    if (type instanceof Types.BinaryType || type instanceof Types.FixedType) {
      byte[] bytes;
      if (value instanceof ByteBuffer) {
        ByteBuffer buf = (ByteBuffer) value;
        bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
      } else if (value instanceof byte[]) {
        bytes = (byte[]) value;
      } else {
        return value;
      }
      return binaryAsString ? StringUtils.fromUtf8(bytes) : bytes;
    }
    if (type instanceof Types.StructType) {
      if (value instanceof Record) {
        Record nested = (Record) value;
        Map<String, Object> nestedMap = new LinkedHashMap<>();
        Types.StructType structType = (Types.StructType) type;
        for (Types.NestedField field : structType.fields()) {
          nestedMap.put(field.name(), convertValue(nested.getField(field.name()), field.type(), binaryAsString));
        }
        return nestedMap;
      }
      return value;
    }
    if (type instanceof Types.ListType) {
      if (value instanceof List) {
        List<?> listValue = (List<?>) value;
        Types.ListType listType = (Types.ListType) type;
        List<Object> result = new ArrayList<>(listValue.size());
        for (Object item : listValue) {
          result.add(convertValue(item, listType.elementType(), binaryAsString));
        }
        return result;
      }
      return value;
    }
    if (type instanceof Types.MapType) {
      if (value instanceof Map) {
        Map<?, ?> mapValue = (Map<?, ?>) value;
        Types.MapType mapType = (Types.MapType) type;
        Map<Object, Object> result = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : mapValue.entrySet()) {
          result.put(
              convertValue(entry.getKey(), mapType.keyType(), binaryAsString),
              convertValue(entry.getValue(), mapType.valueType(), binaryAsString)
          );
        }
        return result;
      }
      return value;
    }
    return value;
  }
}
