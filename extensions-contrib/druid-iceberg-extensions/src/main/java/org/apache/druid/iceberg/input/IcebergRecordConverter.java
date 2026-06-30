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
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Static utility class for converting Iceberg {@link Record} objects to Druid {@link InputRow}.
 */
public class IcebergRecordConverter
{
  private IcebergRecordConverter()
  {
  }

  /**
   * Convert an Iceberg {@link Record} to a {@link Map} of field name → Java value.
   * Iceberg primitive types are passed through; temporal and binary types are converted
   * to representations suitable for Druid ingestion.
   */
  public static Map<String, Object> convertToMap(Record record, Schema schema)
  {
    Map<String, Object> result = new LinkedHashMap<>();
    for (Types.NestedField field : schema.columns()) {
      String name = field.name();
      Object value = record.getField(name);
      result.put(name, convertValue(value, field.type()));
    }
    return result;
  }

  /**
   * Convert a raw-value map plus an {@link InputRowSchema} into a {@link MapBasedInputRow}.
   * If the dimensions list is empty (auto mode), all columns except the timestamp column are used.
   */
  public static InputRow toInputRow(Map<String, Object> row, InputRowSchema inputRowSchema)
  {
    DateTime timestamp = inputRowSchema.getTimestampSpec().extractTimestamp(row);

    List<String> dimensions = inputRowSchema.getDimensionsSpec().getDimensionNames();
    if (dimensions.isEmpty()) {
      String tsColumn = inputRowSchema.getTimestampSpec().getTimestampColumn();
      dimensions = row.keySet().stream()
                      .filter(key -> !key.equals(tsColumn))
                      .collect(Collectors.toList());
    }

    return new MapBasedInputRow(timestamp, dimensions, row);
  }

  private static Object convertValue(Object value, Type type)
  {
    if (value == null) {
      return null;
    }
    if (type instanceof Types.TimestampType) {
      if (value instanceof OffsetDateTime) {
        return ((OffsetDateTime) value).toInstant().toEpochMilli();
      }
      if (value instanceof LocalDateTime) {
        return ((LocalDateTime) value).toInstant(ZoneOffset.UTC).toEpochMilli();
      }
      return value;
    }
    if (type instanceof Types.DateType) {
      if (value instanceof LocalDate) {
        return value.toString();
      }
      return value;
    }
    if (type instanceof Types.TimeType) {
      if (value instanceof LocalTime) {
        return value.toString();
      }
      return value;
    }
    if (type instanceof Types.BinaryType || type instanceof Types.FixedType) {
      if (value instanceof ByteBuffer) {
        ByteBuffer buf = (ByteBuffer) value;
        byte[] bytes = new byte[buf.remaining()];
        buf.duplicate().get(bytes);
        return bytes;
      }
      return value;
    }
    if (type instanceof Types.StructType) {
      if (value instanceof Record) {
        Record nested = (Record) value;
        Map<String, Object> nestedMap = new LinkedHashMap<>();
        Types.StructType structType = (Types.StructType) type;
        for (Types.NestedField field : structType.fields()) {
          nestedMap.put(field.name(), convertValue(nested.getField(field.name()), field.type()));
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
          result.add(convertValue(item, listType.elementType()));
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
              convertValue(entry.getKey(), mapType.keyType()),
              convertValue(entry.getValue(), mapType.valueType())
          );
        }
        return result;
      }
      return value;
    }
    return value;
  }
}
