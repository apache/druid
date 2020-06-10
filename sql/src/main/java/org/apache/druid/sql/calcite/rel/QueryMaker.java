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

package org.apache.druid.sql.calcite.rel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
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
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class QueryMaker
{
  private final QueryLifecycleFactory queryLifecycleFactory;
  private final PlannerContext plannerContext;
  private final ObjectMapper jsonMapper;

  public QueryMaker(
      final QueryLifecycleFactory queryLifecycleFactory,
      final PlannerContext plannerContext,
      final ObjectMapper jsonMapper
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.plannerContext = plannerContext;
    this.jsonMapper = jsonMapper;
  }

  public PlannerContext getPlannerContext()
  {
    return plannerContext;
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

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

    return execute(
        query,
        rowOrder,
        druidQuery.getOutputRowType()
                  .getFieldList()
                  .stream()
                  .map(f -> f.getType().getSqlTypeName())
                  .collect(Collectors.toList())
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
    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();

    // After calling "runSimple" the query will start running. We need to do this before reading the toolChest, since
    // otherwise it won't yet be initialized. (A bummer, since ideally, we'd verify the toolChest exists and can do
    // array-based results before starting the query; but in practice we don't expect this to happen since we keep
    // tight control over which query types we generate in the SQL layer. They all support array-based results.)
    final Sequence<T> results = queryLifecycle.runSimple(query, authenticationResult, null);

    //noinspection unchecked
    final QueryToolChest<T, Query<T>> toolChest = queryLifecycle.getToolChest();
    final List<String> resultArrayFields = toolChest.resultArraySignature(query).getColumnNames();
    final Sequence<Object[]> resultArrays = toolChest.resultsAsArrays(query, results);

    return remapFields(resultArrays, resultArrayFields, newFields, newTypes);
  }

  private Sequence<Object[]> remapFields(
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

  public static ColumnMetaData.Rep rep(final SqlTypeName sqlType)
  {
    if (SqlTypeName.CHAR_TYPES.contains(sqlType)) {
      return ColumnMetaData.Rep.of(String.class);
    } else if (sqlType == SqlTypeName.TIMESTAMP) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.DATE) {
      return ColumnMetaData.Rep.of(Integer.class);
    } else if (sqlType == SqlTypeName.INTEGER) {
      return ColumnMetaData.Rep.of(Integer.class);
    } else if (sqlType == SqlTypeName.BIGINT) {
      return ColumnMetaData.Rep.of(Long.class);
    } else if (sqlType == SqlTypeName.FLOAT) {
      return ColumnMetaData.Rep.of(Float.class);
    } else if (sqlType == SqlTypeName.DOUBLE || sqlType == SqlTypeName.DECIMAL) {
      return ColumnMetaData.Rep.of(Double.class);
    } else if (sqlType == SqlTypeName.BOOLEAN) {
      return ColumnMetaData.Rep.of(Boolean.class);
    } else if (sqlType == SqlTypeName.OTHER) {
      return ColumnMetaData.Rep.of(Object.class);
    } else {
      throw new ISE("No rep for SQL type[%s]", sqlType);
    }
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
      try {
        coercedValue = jsonMapper.writeValueAsString(value);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
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
}
