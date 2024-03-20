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

package org.apache.druid.sql.calcite.run;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Holder for the utility method {@link #coerce(ObjectMapper, Context, Object, SqlTypeName, String)}.
 */
public class SqlResults
{
  public static Object coerce(
      final ObjectMapper jsonMapper,
      final Context context,
      final Object value,
      final SqlTypeName sqlTypeName,
      final String fieldName
  )
  {
    final Object coercedValue;

    if (SqlTypeName.CHAR_TYPES.contains(sqlTypeName)) {
      if (value == null || value instanceof String) {
        coercedValue = NullHandling.nullToEmptyIfNeeded((String) value);
      } else if (value instanceof NlsString) {
        coercedValue = ((NlsString) value).getValue();
      } else if (value instanceof Number) {
        coercedValue = String.valueOf(value);
      } else if (value instanceof Boolean) {
        coercedValue = String.valueOf(value);
      } else {
        final Object maybeList = coerceArrayToList(value, false);

        // Check if "maybeList" was originally a Collection of some kind, or was able to be coerced to one.
        // Then Iterate through the collection, coercing each value. Useful for handling multi-value dimensions.
        if (maybeList instanceof Collection) {
          final List<String> valueStrings =
              ((Collection<?>) maybeList)
                  .stream()
                  .map(v -> (String) coerce(jsonMapper, context, v, sqlTypeName, fieldName))
                  .collect(Collectors.toList());

          // Must stringify since the caller is expecting CHAR_TYPES.
          coercedValue = coerceUsingObjectMapper(jsonMapper, valueStrings, sqlTypeName, fieldName);
        } else {
          throw cannotCoerce(value, sqlTypeName, fieldName);
        }
      }
    } else if (value == null) {
      coercedValue = null;
    } else if (sqlTypeName == SqlTypeName.DATE) {
      return Calcites.jodaToCalciteDate(coerceDateTime(value, sqlTypeName, fieldName), context.getTimeZone());
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      return Calcites.jodaToCalciteTimestamp(coerceDateTime(value, sqlTypeName, fieldName), context.getTimeZone());
    } else if (sqlTypeName == SqlTypeName.BOOLEAN) {
      if (value instanceof Boolean) {
        coercedValue = value;
      } else if (value instanceof String) {
        coercedValue = Evals.asBoolean(((String) value));
      } else if (value instanceof Number) {
        coercedValue = Evals.asBoolean(((Number) value).longValue());
      } else {
        throw cannotCoerce(value, sqlTypeName, fieldName);
      }
    } else if (sqlTypeName == SqlTypeName.INTEGER) {
      if (value instanceof String) {
        coercedValue = Ints.tryParse((String) value);
      } else if (value instanceof Number) {
        coercedValue = ((Number) value).intValue();
      } else {
        throw cannotCoerce(value, sqlTypeName, fieldName);
      }
    } else if (sqlTypeName == SqlTypeName.BIGINT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToLong(value);
      }
      catch (Exception e) {
        throw cannotCoerce(value, sqlTypeName, fieldName);
      }
    } else if (sqlTypeName == SqlTypeName.FLOAT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToFloat(value);
      }
      catch (Exception e) {
        throw cannotCoerce(value, sqlTypeName, fieldName);
      }
    } else if (SqlTypeName.FRACTIONAL_TYPES.contains(sqlTypeName)) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToDouble(value);
      }
      catch (Exception e) {
        throw cannotCoerce(value, sqlTypeName, fieldName);
      }
    } else if (sqlTypeName == SqlTypeName.OTHER) {
      // Complex type, try to serialize if we should, else print class name
      if (context.isSerializeComplexValues()) {
        coercedValue = coerceUsingObjectMapper(jsonMapper, value, sqlTypeName, fieldName);
      } else {
        coercedValue = value.getClass().getName();
      }
    } else if (sqlTypeName == SqlTypeName.ARRAY) {
      if (context.isStringifyArrays()) {
        if (value instanceof String) {
          coercedValue = NullHandling.nullToEmptyIfNeeded((String) value);
        } else if (value instanceof NlsString) {
          coercedValue = ((NlsString) value).getValue();
        } else {
          coercedValue = coerceUsingObjectMapper(jsonMapper, value, sqlTypeName, fieldName);
        }
      } else {
        // the protobuf jdbc handler prefers lists (it actually can't handle java arrays as sql arrays, only java lists)
        // the json handler could handle this just fine, but it handles lists as sql arrays as well so just convert
        // here if needed
        coercedValue = coerceArrayToList(value, true);
      }
    } else {
      throw cannotCoerce(value, sqlTypeName, fieldName);
    }

    return coercedValue;
  }

  /**
   * Attempt to coerce a value to {@link List}. If it cannot be coerced, either return the original value (if mustCoerce
   * is false) or return the value as a single element list (if mustCoerce is true).
   */
  @VisibleForTesting
  @Nullable
  static Object coerceArrayToList(Object value, boolean mustCoerce)
  {
    if (value instanceof List) {
      return value;
    } else if (value instanceof String[]) {
      return Arrays.asList((String[]) value);
    } else if (value instanceof Long[]) {
      return Arrays.asList((Long[]) value);
    } else if (value instanceof Double[]) {
      return Arrays.asList((Double[]) value);
    } else if (value instanceof Object[]) {
      final Object[] array = (Object[]) value;
      final ArrayList<Object> lst = new ArrayList<>(array.length);
      for (Object o : array) {
        lst.add(coerceArrayToList(o, false));
      }
      return lst;
    } else if (value instanceof long[]) {
      return Arrays.stream((long[]) value).boxed().collect(Collectors.toList());
    } else if (value instanceof double[]) {
      return Arrays.stream((double[]) value).boxed().collect(Collectors.toList());
    } else if (value instanceof float[]) {
      final float[] array = (float[]) value;
      final ArrayList<Object> lst = new ArrayList<>(array.length);
      for (float f : array) {
        lst.add(f);
      }
      return lst;
    } else if (mustCoerce) {
      return Collections.singletonList(value);
    }
    return value;
  }

  private static DateTime coerceDateTime(final Object value, final SqlTypeName sqlType, final String fieldName)
  {
    final DateTime dateTime;

    if (value instanceof Number) {
      dateTime = DateTimes.utc(((Number) value).longValue());
    } else if (value instanceof String) {
      dateTime = DateTimes.utc(Long.parseLong((String) value));
    } else if (value instanceof DateTime) {
      dateTime = (DateTime) value;
    } else {
      throw cannotCoerce(value, sqlType, fieldName);
    }
    return dateTime;
  }

  private static String coerceUsingObjectMapper(
      final ObjectMapper jsonMapper,
      final Object value,
      final SqlTypeName sqlTypeName,
      final String fieldName
  )
  {
    try {
      return jsonMapper.writeValueAsString(value);
    }
    catch (JsonProcessingException e) {
      throw cannotCoerce(e, value, sqlTypeName, fieldName);
    }
  }

  private static DruidException cannotCoerce(final Throwable t, final Object value, final SqlTypeName sqlTypeName, final String fieldName)
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.INVALID_INPUT)
                         .build(
                             t,
                             "Cannot coerce field [%s] from type [%s] to type [%s]",
                             fieldName,
                             value == null
                             ? "unknown"
                             : mapPriveArrayClassNameToReadableStrings(value.getClass().getName()),
                             sqlTypeName
                         );
  }

  private static String mapPriveArrayClassNameToReadableStrings(String name)
  {
    switch (name) {
      case "[B":
        return "Byte Array";
      case "[Z":
        return "Boolean Array";
      default:
        return name;
    }
  }

  private static DruidException cannotCoerce(final Object value, final SqlTypeName sqlTypeName, final String fieldName)
  {
    return cannotCoerce(null, value, sqlTypeName, fieldName);
  }

  /**
   * Context for {@link #coerce(ObjectMapper, Context, Object, SqlTypeName, String)}
   */
  public static class Context
  {
    private final DateTimeZone timeZone;
    private final boolean serializeComplexValues;
    private final boolean stringifyArrays;

    @JsonCreator
    public Context(
        @JsonProperty("timeZone") final DateTimeZone timeZone,
        @JsonProperty("serializeComplexValues") final boolean serializeComplexValues,
        @JsonProperty("stringifyArrays") final boolean stringifyArrays
    )
    {
      this.timeZone = timeZone;
      this.serializeComplexValues = serializeComplexValues;
      this.stringifyArrays = stringifyArrays;
    }

    public static Context fromPlannerContext(final PlannerContext plannerContext)
    {
      return new Context(
          plannerContext.getTimeZone(),
          plannerContext.getPlannerConfig().shouldSerializeComplexValues(),
          plannerContext.isStringifyArrays()
      );
    }

    @JsonProperty
    public DateTimeZone getTimeZone()
    {
      return timeZone;
    }

    @JsonProperty
    public boolean isSerializeComplexValues()
    {
      return serializeComplexValues;
    }

    @JsonProperty
    public boolean isStringifyArrays()
    {
      return stringifyArrays;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Context context = (Context) o;
      return serializeComplexValues == context.serializeComplexValues
             && stringifyArrays == context.stringifyArrays
             && Objects.equals(timeZone, context.timeZone);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(timeZone, serializeComplexValues, stringifyArrays);
    }

    @Override
    public String toString()
    {
      return "Context{" +
             "timeZone=" + timeZone +
             ", serializeComplexValues=" + serializeComplexValues +
             ", stringifyArrays=" + stringifyArrays +
             '}';
    }
  }
}
