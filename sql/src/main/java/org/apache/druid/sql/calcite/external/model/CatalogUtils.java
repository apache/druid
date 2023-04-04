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

package org.apache.druid.sql.calcite.external.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CatalogUtils
{
  public static List<String> columnNames(List<ColumnSpec> columns)
  {
    return columns
           .stream()
           .map(col -> col.name())
           .collect(Collectors.toList());
  }

  /**
   * {@code String}-to-{@code List<String>} conversion. The string can contain zero items,
   * one items, or a list. The list items are separated by a comma and optional
   * whitespace.
   */
  public static List<String> stringToList(String value)
  {
    if (value == null) {
      return null;
    }
    return Arrays.asList(value.split(",\\s*"));
  }

  public static <T> T safeCast(Object value, Class<T> type, String key)
  {
    if (value == null) {
      return null;
    }
    try {
      return type.cast(value);
    }
    catch (ClassCastException e) {
      throw new IAE("Value [%s] is not valid for property %s, expected type %s",
          value,
          key,
          type.getSimpleName()
      );
    }
  }

  public static <T> T safeGet(Map<String, Object> map, String key, Class<T> type)
  {
    return safeCast(map.get(key), type, key);
  }

  public static long getLong(Map<String, Object> map, String key)
  {
    Object value = map.get(key);
    if (value == null) {
      return 0;
    }

    // Jackson may deserialize the value as either Integer or Long.
    if (value instanceof Integer) {
      return (Integer) value;
    }
    return safeCast(value, Long.class, key);
  }

  public static String getString(Map<String, Object> map, String key)
  {
    return safeGet(map, key, String.class);
  }

  public static List<String> getStringList(Map<String, Object> map, String key)
  {
    return stringToList(getString(map, key));
  }

  /**
   * Get the value of a {@code VARCHAR ARRAY} parameter. Though the type is
   * called {@code ARRAY}, Calcite provides the actual value as a {@link List}
   * of {@code String}s.
   */
  @SuppressWarnings("unchecked")
  public static List<String> getStringArray(Map<String, Object> map, String key)
  {
    final Object value = map.get(key);
    if (value == null) {
      return null;
    }
    return (List<String>) safeCast(value, List.class, key);
  }

  public static String stringListToLines(List<String> lines)
  {
    if (lines.isEmpty()) {
      return "";
    }
    return String.join("\n", lines) + "\n";
  }

  /**
   * Catalog-specific quick & easy implementation of {@code toString()} for objects
   * which are primarily representations of JSON objects. Use only for cases where the
   * {@code toString()} is for debugging. Also, assumes that the
   * type can serialized using the default mapper: this trick doesn't work for types that
   * require custom Jackson extensions. The catalog, however, has a simple type
   * hierarchy, which is not extended via extensions, and so the default object mapper is
   * fine.
   */
  public static String toString(Object obj)
  {
    try {
      return DefaultObjectMapper.INSTANCE.writerWithDefaultPrettyPrinter().writeValueAsString(obj);
    }
    catch (JsonProcessingException e) {
      throw new ISE("Failed to serialize TableDefn");
    }
  }

  public static <T> List<T> concatLists(
      @Nullable final List<T> base,
      @Nullable final List<T> additions
  )
  {
    return Stream
        .of(base, additions)
        .filter(Objects::nonNull)
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  /**
   * Get a string parameter that can either be null or non-blank.
   */
  public static String getNonBlankString(Map<String, Object> args, String parameter)
  {
    String value = CatalogUtils.getString(args, parameter);
    if (value != null) {
      value = value.trim();
      if (value.isEmpty()) {
        throw new IAE("%s parameter cannot be a blank string", parameter);
      }
    }
    return value;
  }

  public static int findColumn(List<ColumnSpec> columns, String colName)
  {
    for (int i = 0; i < columns.size(); i++) {
      if (columns.get(i).name().equals(colName)) {
        return i;
      }
    }
    return -1;
  }
}
