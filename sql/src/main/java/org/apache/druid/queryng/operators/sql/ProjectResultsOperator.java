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

package org.apache.druid.queryng.operators.sql;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.primitives.Ints;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.MappingOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.ResultIterator;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Projection operator to reorder and change types of the values from a native
 * query to match the expected output for a SQL query.
 *
 * This version is almost a literal copy of the {@code NativeQueryMaker} code,
 * to demonstrate that the operator did not change semantics. It uses the
 * "interpreted" style of conversion, and acts as a temporary reference
 * point for the cached {@link ProjectResultsOperatorEx} version.
 *
 * @See {@link org.apache.druid.sql.calcite.run.NativeQueryMaker}
 */
public class ProjectResultsOperator extends MappingOperator<Object[], Object[]>
{
  private final int[] mapping;
  private final List<SqlTypeName> newTypes;
  private final ObjectMapper jsonMapper;
  private final DateTimeZone timeZone;
  private final boolean serializeComplexValues;
  private final boolean stringifyArrays;
  private int rowCount;

  public ProjectResultsOperator(
      final FragmentContext context,
      final Operator<Object[]> input,
      final int[] mapping,
      final List<SqlTypeName> newTypes,
      final ObjectMapper jsonMapper,
      final DateTimeZone timeZone,
      final boolean serializeComplexValues,
      final boolean stringifyArrays)
  {
    super(context, input);
    this.mapping = mapping;
    this.newTypes = newTypes;
    this.jsonMapper = jsonMapper;
    this.timeZone = timeZone;
    this.serializeComplexValues = serializeComplexValues;
    this.stringifyArrays = stringifyArrays;
  }

  @Override
  public Object[] next() throws ResultIterator.EofException
  {
    final Object[] array = inputIter.next();
    final Object[] newArray = new Object[mapping.length];
    for (int i = 0; i < mapping.length; i++) {
      newArray[i] = coerce(array[mapping[i]], newTypes.get(i));
    }
    rowCount++;
    return newArray;
  }

  private Object coerce(final Object value, final SqlTypeName sqlType)
  {
    final Object coercedValue;

    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      if (value == null || value instanceof String) {
        coercedValue = NullHandling.nullToEmptyIfNeeded((String) value);
      } else if (value instanceof NlsString) {
        coercedValue = ((NlsString) value).getValue();
      } else if (value instanceof Number) {
        coercedValue = String.valueOf(value);
      } else if (value instanceof Collection) {
        // Iterate through the collection, coercing each value. Useful for handling selects of multi-value dimensions.
        final List<String> valueStrings = ((Collection<?>) value).stream()
                                                                 .map(v -> (String) coerce(v, sqlType))
                                                                 .collect(Collectors.toList());

        try {
          coercedValue = jsonMapper.writeValueAsString(valueStrings);
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (value == null) {
      coercedValue = null;
    } else if (sqlType == SqlTypeName.DATE) {
      return Calcites.jodaToCalciteDate(coerceDateTime(value, sqlType), timeZone);
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return Calcites.jodaToCalciteTimestamp(coerceDateTime(value, sqlType), timeZone);
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      if (value instanceof String) {
        coercedValue = Evals.asBoolean(((String) value));
      } else if (value instanceof Number) {
        coercedValue = Evals.asBoolean(((Number) value).longValue());
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.INTEGER) {
      if (value instanceof String) {
        coercedValue = Ints.tryParse((String) value);
      } else if (value instanceof Number) {
        coercedValue = ((Number) value).intValue();
      } else {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.BIGINT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToLong(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.FLOAT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToFloat(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (SqlTypeName.FRACTIONAL_TYPES.contains(sqlType)) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToDouble(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.OTHER) {
      // Complex type, try to serialize if we should, else print class name
      if (serializeComplexValues) {
        try {
          coercedValue = jsonMapper.writeValueAsString(value);
        }
        catch (JsonProcessingException jex) {
          throw new ISE(jex, "Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
        }
      } else {
        coercedValue = value.getClass().getName();
      }
    } else if (sqlType == SqlTypeName.ARRAY) {
      if (stringifyArrays) {
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
          throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
        }
      }
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }

    return coercedValue;
  }

  private static Object maybeCoerceArrayToList(Object value, boolean mustCoerce)
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
      Object[] array = (Object[]) value;
      ArrayList<Object> lst = new ArrayList<>(array.length);
      for (Object o : array) {
        lst.add(maybeCoerceArrayToList(o, false));
      }
      return lst;
    } else if (value instanceof ComparableStringArray) {
      return Arrays.asList(((ComparableStringArray) value).getDelegate());
    } else if (value instanceof ComparableList) {
      return ((ComparableList<?>) value).getDelegate();
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
}
