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
import org.apache.druid.queryng.operators.OperatorProfile;
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
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Projection operator to reorder and change types of the values from a native
 * query to match the expected output for a SQL query.
 *
 * This version caches the conversions for performance. The destination types
 * are the same for every row, so we pre-compute the conversion function, then
 * invoke it for each column of each row. Unfortunately, Druid native rows can
 * have varying types, so we have to dynamically pick out the input type for
 * each row, which is a bit slow.
 *
 * @See {@link org.apache.druid.sql.calcite.run.NativeQueryMaker}
 */
public class ProjectResultsOperatorEx extends MappingOperator<Object[], Object[]>
{
  private final int[] mapping;
  private final ObjectMapper jsonMapper;
  private final DateTimeZone timeZone;
  private final boolean serializeComplexValues;
  private final boolean stringifyArrays;
  private final Function<Object, Object>[] conversions;
  private int rowCount;

  @SuppressWarnings("unchecked")
  public ProjectResultsOperatorEx(
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
    this.jsonMapper = jsonMapper;
    this.timeZone = timeZone;
    this.serializeComplexValues = serializeComplexValues;
    this.stringifyArrays = stringifyArrays;

    this.conversions = new Function[newTypes.size()];
    for (int i = 0; i < newTypes.size(); i++) {
      conversions[i] = createCast(newTypes.get(i));
    }
  }

  @Override
  public Object[] next() throws ResultIterator.EofException
  {
    final Object[] array = inputIter.next();
    rowCount++;
    final Object[] newArray = new Object[mapping.length];
    for (int i = 0; i < mapping.length; i++) {
      newArray[i] = conversions[i].apply(array[mapping[i]]);
    }
    return newArray;
  }

  private Function<Object, Object> createCast(SqlTypeName sqlType)
  {
    switch (sqlType) {
      case CHAR:
      case VARCHAR:
        return new CastToString(sqlType, jsonMapper);
      case DATE:
        return value -> value == null
              ? null
              : Calcites.jodaToCalciteDate(coerceDateTime(value, sqlType), timeZone);
      case TIMESTAMP:
        return value -> value == null
              ? null
              : Calcites.jodaToCalciteTimestamp(coerceDateTime(value, sqlType), timeZone);
      case BOOLEAN:
        return value -> {
          if (value == null) {
            return null;
          } else if (value instanceof String) {
            return Evals.asBoolean(((String) value));
          } else if (value instanceof Number) {
            return Evals.asBoolean(((Number) value).longValue());
          } else {
            throw conversionFailed(value, sqlType);
          }
        };
      case INTEGER:
        return value -> {
          if (value == null) {
            return null;
          } else if (value instanceof Number) {
            return ((Number) value).intValue();
          } else if (value instanceof String) {
            return Ints.tryParse((String) value);
          } else {
            throw conversionFailed(value, sqlType);
          }
        };
      case BIGINT:
        return value -> {
          try {
            return value == null ? null : DimensionHandlerUtils.convertObjectToLong(value);
          }
          catch (Exception e) {
            throw conversionFailed(value, sqlType);
          }
        };
      case FLOAT:
        return value -> {
          try {
            return value == null ? null : DimensionHandlerUtils.convertObjectToFloat(value);
          }
          catch (Exception e) {
            throw conversionFailed(value, sqlType);
          }
        };
      case DOUBLE: // FRACTIONAL_TYPES - FLOAT
      case REAL:
      case DECIMAL:
        return value -> {
          try {
            return value == null ? null : DimensionHandlerUtils.convertObjectToDouble(value);
          }
          catch (Exception e) {
            throw conversionFailed(value, sqlType);
          }
        };
      case OTHER:
        // Complex type, try to serialize if we should, else print class name
        if (serializeComplexValues) {
          return value -> {
            try {
              return value == null ? null : jsonMapper.writeValueAsString(value);
            }
            catch (JsonProcessingException jex) {
              throw conversionFailed(value, sqlType);
            }
          };
        } else {
          return value ->
              value == null ? null : value.getClass().getName();
        }
      case ARRAY:
        if (stringifyArrays) {
          return value -> {
            if (value == null) {
              return null;
            } else if (value instanceof String) {
              return NullHandling.nullToEmptyIfNeeded((String) value);
            } else if (value instanceof NlsString) {
              return ((NlsString) value).getValue();
            } else {
              try {
                return jsonMapper.writeValueAsString(value);
              }
              catch (IOException e) {
                throw conversionFailed(value, sqlType);
              }
            }
          };
        } else {
          // The Protobuf JDBC handler prefers lists (it actually can't handle Java
          // arrays as SQL arrays, only Java lists).
          // The JSON handler could handle this just fine, but it handles lists as
          // SQL arrays as well so just convert here if needed.
          return value -> value == null
              ? value
              : maybeCoerceArrayToList(value, true);
        }
      default:
        throw new ISE("Unsupported SQL type: %s", sqlType);
    }
  }

  private static ISE conversionFailed(Object value, SqlTypeName sqlType)
  {
    return new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
  }

  private static class CastToString implements Function<Object, Object>
  {
    private final SqlTypeName sqlType;
    private final ObjectMapper jsonMapper;

    CastToString(SqlTypeName sqlType, ObjectMapper jsonMapper)
    {
      this.sqlType = sqlType;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public Object apply(Object value)
    {
      if (value == null) {
        return NullHandling.nullToEmptyIfNeeded((String) value);
      } else if (value instanceof String) {
        return value;
      } else if (value instanceof Number) {
        return String.valueOf(value);
      } else if (value instanceof NlsString) {
        return ((NlsString) value).getValue();
      } else if (value instanceof Collection) {
        // Iterate through the collection, coercing each value. Useful for handling selects of multi-value dimensions.
        final List<String> valueStrings = ((Collection<?>) value).stream()
                                                                 .map(v -> (String) apply(v))
                                                                 .collect(Collectors.toList());

        try {
          return jsonMapper.writeValueAsString(valueStrings);
        }
        catch (IOException e) {
          throw conversionFailed(value, sqlType);
        }
      } else {
        throw conversionFailed(value, sqlType);
      }
    }
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
      throw conversionFailed(value, SqlTypeName.ARRAY);
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
      throw conversionFailed(value, SqlTypeName.ARRAY);
    }
    return dateTime;
  }

  @Override
  public void close(boolean cascade)
  {
    if (state == State.RUN) {
      OperatorProfile profile = new OperatorProfile("project-sql-results");
      profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
      context.updateProfile(this, profile);
    }
    super.close(cascade);
  }
}
