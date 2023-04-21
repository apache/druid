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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Holder for the utility method {@link #coerce(ObjectMapper, Context, Object, SqlTypeName)}.
 */
public class SqlResults
{
  public static Object coerce(
      final ObjectMapper jsonMapper,
      final Context context,
      final Object value,
      final SqlTypeName sqlTypeName
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
      } else if (value instanceof Collection) {
        // Iterate through the collection, coercing each value. Useful for handling selects of multi-value dimensions.
        final List<String> valueStrings =
            ((Collection<?>) value).stream()
                                   .map(v -> (String) coerce(jsonMapper, context, v, sqlTypeName))
                                   .collect(Collectors.toList());

        try {
          coercedValue = jsonMapper.writeValueAsString(valueStrings);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
      }
    } else if (value == null) {
      coercedValue = null;
    } else if (sqlTypeName == SqlTypeName.DATE) {
      return Calcites.jodaToCalciteDate(coerceDateTime(value, sqlTypeName), context.getTimeZone());
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      return Calcites.jodaToCalciteTimestamp(coerceDateTime(value, sqlTypeName), context.getTimeZone());
    } else if (sqlTypeName == SqlTypeName.BOOLEAN) {
      if (value instanceof String) {
        coercedValue = Evals.asBoolean(((String) value));
      } else if (value instanceof Number) {
        coercedValue = Evals.asBoolean(((Number) value).longValue());
      } else {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
      }
    } else if (sqlTypeName == SqlTypeName.INTEGER) {
      if (value instanceof String) {
        coercedValue = Ints.tryParse((String) value);
      } else if (value instanceof Number) {
        coercedValue = ((Number) value).intValue();
      } else {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
      }
    } else if (sqlTypeName == SqlTypeName.BIGINT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToLong(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
      }
    } else if (sqlTypeName == SqlTypeName.FLOAT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToFloat(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
      }
    } else if (SqlTypeName.FRACTIONAL_TYPES.contains(sqlTypeName)) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToDouble(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
      }
    } else if (sqlTypeName == SqlTypeName.OTHER) {
      // Complex type, try to serialize if we should, else print class name
      if (context.isSerializeComplexValues()) {
        try {
          coercedValue = jsonMapper.writeValueAsString(value);
        }
        catch (JsonProcessingException jex) {
          throw new ISE(jex, "Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
        }
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
          try {
            coercedValue = jsonMapper.writeValueAsString(value);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
      } else {
        // the protobuf jdbc handler prefers lists (it actually can't handle java arrays as sql arrays, only java lists)
        // the json handler could handle this just fine, but it handles lists as sql arrays as well so just convert
        // here if needed
        coercedValue = maybeCoerceArrayToList(value, true);
        if (coercedValue == null) {
          throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
        }
      }
    } else {
      throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlTypeName);
    }

    return coercedValue;
  }


  @VisibleForTesting
  static Object maybeCoerceArrayToList(Object value, boolean mustCoerce)
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
        lst.add(maybeCoerceArrayToList(o, false));
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
    } else if (value instanceof ComparableStringArray) {
      return Arrays.asList(((ComparableStringArray) value).getDelegate());
    } else if (value instanceof ComparableList) {
      return ((ComparableList) value).getDelegate();
    } else if (mustCoerce) {
      return null;
    }
    return value;
  }

  private static DateTime coerceDateTime(Object value, SqlTypeName sqlType)
  {
    final DateTime dateTime;

    if (value instanceof Number) {
      dateTime = DateTimes.utc(((Number) value).longValue());
    } else if (value instanceof String) {
      dateTime = DateTimes.utc(Long.parseLong((String) value));
    } else if (value instanceof DateTime) {
      dateTime = (DateTime) value;
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }
    return dateTime;
  }

  /**
   * Context for {@link #coerce(ObjectMapper, Context, Object, SqlTypeName)}
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
