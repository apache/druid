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

package org.apache.druid.catalog.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.Period;

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
   * Convert a catalog granularity string to the Druid form. Catalog granularities
   * are either the usual descriptive strings (in any case), or an ISO period.
   * For the odd interval, the interval name is also accepted (for the other
   * intervals, the interval name is the descriptive string).
   */
  public static Granularity asDruidGranularity(String value)
  {
    if (Strings.isNullOrEmpty(value)) {
      return Granularities.ALL;
    }
    try {
      return new PeriodGranularity(new Period(value), null, null);
    }
    catch (IllegalArgumentException e) {
      throw new IAE(StringUtils.format("%s is an invalid period string", value));
    }
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

  public static <T> T safeCast(Object value, Class<T> type, String propertyName)
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
          propertyName,
          type.getSimpleName()
      );
    }
  }

  public static <T> T safeGet(Map<String, Object> map, String propertyName, Class<T> type)
  {
    return safeCast(map.get(propertyName), type, propertyName);
  }

  public static String stringListToLines(List<String> lines)
  {
    if (lines.isEmpty()) {
      return "";
    }
    return String.join("\n", lines) + "\n";
  }

  /**
   * Catalog-specific 1uick & easy implementation of {@code toString()} for objects
   * which are primarily representations of JSON objects. Use only for cases where the
   * {@code toString()} is for debugging: the cost of creating an object mapper
   * every time is undesirable for production code. Also, assumes that the
   * type can serialized using the default mapper: doesn't work for types that
   * require custom Jackson extensions. The catalog, however, has a simple type
   * hierarchy, which is not extended via extensions, and so the default object mapper is
   * fine.
   */
  public static String toString(Object obj)
  {
    ObjectMapper jsonMapper = new ObjectMapper();
    try {
      return jsonMapper.writeValueAsString(obj);
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
}
