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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.CannotBuildQueryException;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class NativeQueryMaker implements QueryMaker
{
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final PlannerContext plannerContext;
  private final ObjectMapper jsonMapper;
  private final List<Pair<Integer, String>> fieldMapping;
  private final RelDataType resultType;

  public NativeQueryMaker(
      final QueryLifecycleFactory queryLifecycleFactory,
      final PlannerContext plannerContext,
      final ObjectMapper jsonMapper,
      final List<Pair<Integer, String>> fieldMapping,
      final RelDataType resultType
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.plannerContext = plannerContext;
    this.jsonMapper = jsonMapper;
    this.fieldMapping = fieldMapping;
    this.resultType = resultType;
  }

  @Override
  public RelDataType getResultType()
  {
    return resultType;
  }

  @Override
  public boolean feature(QueryFeature feature)
  {
    switch (feature) {
      case CAN_RUN_TIMESERIES:
      case CAN_RUN_TOPN:
        return true;
      case CAN_READ_EXTERNAL_DATA:
      case SCAN_CAN_ORDER_BY_NON_TIME:
        return false;
      default:
        throw new IAE("Unrecognized feature: %s", feature);
    }
  }

  @Override
  public Sequence<Object[]> runQuery(final DruidQuery druidQuery)
  {
    final Query<?> query = druidQuery.getQuery();

    if (plannerContext.getPlannerConfig().isRequireTimeCondition()) {
      if (Intervals.ONLY_ETERNITY.equals(findBaseDataSourceIntervals(query))) {
        throw new CannotBuildQueryException(
            "requireTimeCondition is enabled, all queries must include a filter condition on the __time column"
        );
      }
    }

    final List<String> rowOrder;
    if (query instanceof TimeseriesQuery && !druidQuery.getGrouping().getDimensions().isEmpty()) {
      // Hack for timeseries queries: when generating them, DruidQuery.toTimeseriesQuery translates a dimension
      // based on a timestamp_floor expression into a 'granularity'. This is not reflected in the druidQuery's
      // output row signature, so we have to account for it here.
      // TODO: We can remove this once https://github.com/apache/druid/issues/9974 is done.
      final String timeDimension = Iterables.getOnlyElement(druidQuery.getGrouping().getDimensions()).getOutputName();
      rowOrder = druidQuery.getOutputRowSignature().getColumnNames().stream()
                           .map(f -> timeDimension.equals(f) ? ColumnHolder.TIME_COLUMN_NAME : f)
                           .collect(Collectors.toList());
    } else {
      rowOrder = druidQuery.getOutputRowSignature().getColumnNames();
    }

    final List<SqlTypeName> columnTypes =
        druidQuery.getOutputRowType()
                  .getFieldList()
                  .stream()
                  .map(f -> f.getType().getSqlTypeName())
                  .collect(Collectors.toList());

    return execute(
        query,
        mapColumnList(rowOrder, fieldMapping),
        mapColumnList(columnTypes, fieldMapping)
    );
  }

  private List<Interval> findBaseDataSourceIntervals(Query<?> query)
  {
    return DataSourceAnalysis.forDataSource(query.getDataSource())
                             .getBaseQuerySegmentSpec()
                             .map(QuerySegmentSpec::getIntervals)
                             .orElseGet(query::getIntervals);
  }

  private <T> Sequence<Object[]> execute(Query<T> query, final List<String> newFields, final List<SqlTypeName> newTypes)
  {
    Hook.QUERY_PLAN.run(query);

    if (query.getId() == null) {
      final String queryId = UUID.randomUUID().toString();
      plannerContext.addNativeQueryId(queryId);
      query = query.withId(queryId);
    }

    query = query.withSqlQueryId(plannerContext.getSqlQueryId());

    final AuthenticationResult authenticationResult = plannerContext.getAuthenticationResult();
    final Access authorizationResult = plannerContext.getAuthorizationResult();
    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();

    // After calling "runSimple" the query will start running. We need to do this before reading the toolChest, since
    // otherwise it won't yet be initialized. (A bummer, since ideally, we'd verify the toolChest exists and can do
    // array-based results before starting the query; but in practice we don't expect this to happen since we keep
    // tight control over which query types we generate in the SQL layer. They all support array-based results.)
    final Sequence<T> results = queryLifecycle.runSimple(query, authenticationResult, authorizationResult);

    //noinspection unchecked
    final QueryToolChest<T, Query<T>> toolChest = queryLifecycle.getToolChest();
    final List<String> resultArrayFields = toolChest.resultArraySignature(query).getColumnNames();
    final Sequence<Object[]> resultArrays = toolChest.resultsAsArrays(query, results);

    return mapResultSequence(resultArrays, resultArrayFields, newFields, newTypes);
  }

  private Sequence<Object[]> mapResultSequence(
      final Sequence<Object[]> sequence,
      final List<String> originalFields,
      final List<String> newFields,
      final List<SqlTypeName> newTypes
  )
  {
    // Build hash map for looking up original field positions, in case the number of fields is super high.
    final Object2IntMap<String> originalFieldsLookup = new Object2IntOpenHashMap<>();
    originalFieldsLookup.defaultReturnValue(-1);
    for (int i = 0; i < originalFields.size(); i++) {
      originalFieldsLookup.put(originalFields.get(i), i);
    }

    // Build "mapping" array of new field index -> old field index.
    final int[] mapping = new int[newFields.size()];
    for (int i = 0; i < newFields.size(); i++) {
      final String newField = newFields.get(i);
      final int idx = originalFieldsLookup.getInt(newField);
      if (idx < 0) {
        throw new ISE(
            "newField[%s] not contained in originalFields[%s]",
            newField,
            String.join(", ", originalFields)
        );
      }

      mapping[i] = idx;
    }

    return Sequences.map(
        sequence,
        array -> {
          final Object[] newArray = new Object[mapping.length];
          for (int i = 0; i < mapping.length; i++) {
            newArray[i] = coerce(array[mapping[i]], newTypes.get(i));
          }
          return newArray;
        }
    );
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
      return Calcites.jodaToCalciteDate(coerceDateTime(value, sqlType), plannerContext.getTimeZone());
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return Calcites.jodaToCalciteTimestamp(coerceDateTime(value, sqlType), plannerContext.getTimeZone());
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
      if (plannerContext.getPlannerConfig().shouldSerializeComplexValues()) {
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
      if (plannerContext.isStringifyArrays()) {
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
        if (value instanceof List) {
          coercedValue = value;
        } else if (value instanceof String[]) {
          coercedValue = Arrays.asList((String[]) value);
        } else if (value instanceof Long[]) {
          coercedValue = Arrays.asList((Long[]) value);
        } else if (value instanceof Double[]) {
          coercedValue = Arrays.asList((Double[]) value);
        } else if (value instanceof Object[]) {
          coercedValue = Arrays.asList((Object[]) value);
        } else {
          throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
        }
      }
    } else {
      throw new ISE("Cannot coerce[%s] to %s", value.getClass().getName(), sqlType);
    }

    return coercedValue;
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

  private static <T> List<T> mapColumnList(final List<T> in, final List<Pair<Integer, String>> fieldMapping)
  {
    final List<T> out = new ArrayList<>(fieldMapping.size());

    for (final Pair<Integer, String> entry : fieldMapping) {
      out.add(in.get(entry.getKey()));
    }

    return out;
  }
}
