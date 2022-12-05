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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Ints;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Pair;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
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

  public NativeQueryMaker(
      final QueryLifecycleFactory queryLifecycleFactory,
      final PlannerContext plannerContext,
      final ObjectMapper jsonMapper,
      final List<Pair<Integer, String>> fieldMapping
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.plannerContext = plannerContext;
    this.jsonMapper = jsonMapper;
    this.fieldMapping = fieldMapping;
  }

  @Override
  public QueryResponse<Object[]> runQuery(final DruidQuery druidQuery)
  {
    final Query<?> query = druidQuery.getQuery();

    if (plannerContext.getPlannerConfig().isRequireTimeCondition()
        && !(druidQuery.getDataSource() instanceof InlineDataSource)) {
      if (Intervals.ONLY_ETERNITY.equals(findBaseDataSourceIntervals(query))) {
        throw new CannotBuildQueryException(
            "requireTimeCondition is enabled, all queries must include a filter condition on the __time column"
        );
      }
    }
    int numFilters = plannerContext.getPlannerConfig().getMaxNumericInFilters();

    // special corner case handling for numeric IN filters
    // in case of query containing IN (v1, v2, v3,...) where Vi is numeric
    // a BoundFilter is created internally for each of the values
    // whereas when Vi s are String the Filters are converted as BoundFilter to SelectorFilter to InFilter
    // which takes lesser processing for bitmaps
    // So in a case where user executes a query with multiple numeric INs, flame graph shows BoundFilter.getBitmapColumnIndex
    // and BoundFilter.match predicate eating up processing time which stalls a historical for a query with large number
    // of numeric INs (> 10K). In such cases user should change the query to specify the IN clauses as String
    // Instead of IN(v1,v2,v3) user should specify IN('v1','v2','v3')
    if (numFilters != PlannerConfig.NUM_FILTER_NOT_USED) {
      if (query.getFilter() instanceof OrDimFilter) {
        OrDimFilter orDimFilter = (OrDimFilter) query.getFilter();
        int numBoundFilters = 0;
        for (DimFilter filter : orDimFilter.getFields()) {
          numBoundFilters += filter instanceof BoundDimFilter ? 1 : 0;
        }
        if (numBoundFilters > numFilters) {
          String dimension = ((BoundDimFilter) (orDimFilter.getFields().get(0))).getDimension();
          throw new UOE(StringUtils.format(
              "The number of values in the IN clause for [%s] in query exceeds configured maxNumericFilter limit of [%s] for INs. Cast [%s] values of IN clause to String",
              dimension,
              numFilters,
              orDimFilter.getFields().size()
          ));
        }
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

  @SuppressWarnings("unchecked")
  private <T> QueryResponse<Object[]> execute(Query<?> query, final List<String> newFields, final List<SqlTypeName> newTypes)
  {
    Hook.QUERY_PLAN.run(query);

    if (query.getId() == null) {
      final String queryId = UUID.randomUUID().toString();
      plannerContext.addNativeQueryId(queryId);
      query = query.withId(queryId);
    } else {
      plannerContext.addNativeQueryId(query.getId());
    }

    query = query.withSqlQueryId(plannerContext.getSqlQueryId());

    final AuthenticationResult authenticationResult = plannerContext.getAuthenticationResult();
    final Access authorizationResult = plannerContext.getAuthorizationResult();
    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();

    // After calling "runSimple" the query will start running. We need to do this before reading the toolChest, since
    // otherwise it won't yet be initialized. (A bummer, since ideally, we'd verify the toolChest exists and can do
    // array-based results before starting the query; but in practice we don't expect this to happen since we keep
    // tight control over which query types we generate in the SQL layer. They all support array-based results.)
    final QueryResponse<T> results = queryLifecycle.runSimple((Query<T>) query, authenticationResult, authorizationResult);

    return mapResultSequence(
        results,
        (QueryToolChest<T, Query<T>>) queryLifecycle.getToolChest(),
        (Query<T>) query,
        newFields,
        newTypes
    );
  }

  private <T> QueryResponse<Object[]> mapResultSequence(
      final QueryResponse<T> results,
      final QueryToolChest<T, Query<T>> toolChest,
      final Query<T> query,
      final List<String> newFields,
      final List<SqlTypeName> newTypes
  )
  {
    final List<String> originalFields = toolChest.resultArraySignature(query).getColumnNames();

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

    //noinspection unchecked
    final Sequence<Object[]> sequence = toolChest.resultsAsArrays(query, results.getResults());
    return new QueryResponse(
        Sequences.map(
            sequence,
            array -> {
              final Object[] newArray = new Object[mapping.length];
              for (int i = 0; i < mapping.length; i++) {
                newArray[i] = coerce(array[mapping[i]], newTypes.get(i));
              }
              return newArray;
            }
        ),
        results.getResponseContext()
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
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
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
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.INTEGER) {
      if (value instanceof String) {
        coercedValue = Ints.tryParse((String) value);
      } else if (value instanceof Number) {
        coercedValue = ((Number) value).intValue();
      } else {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.BIGINT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToLong(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.FLOAT) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToFloat(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (SqlTypeName.FRACTIONAL_TYPES.contains(sqlType)) {
      try {
        coercedValue = DimensionHandlerUtils.convertObjectToDouble(value);
      }
      catch (Exception e) {
        throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
      }
    } else if (sqlType == SqlTypeName.OTHER) {
      // Complex type, try to serialize if we should, else print class name
      if (plannerContext.getPlannerConfig().shouldSerializeComplexValues()) {
        try {
          coercedValue = jsonMapper.writeValueAsString(value);
        }
        catch (JsonProcessingException jex) {
          throw new ISE(jex, "Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
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
        coercedValue = maybeCoerceArrayToList(value, true);
        if (coercedValue == null) {
          throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
        }
      }
    } else {
      throw new ISE("Cannot coerce [%s] to %s", value.getClass().getName(), sqlType);
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

  private static <T> List<T> mapColumnList(final List<T> in, final List<Pair<Integer, String>> fieldMapping)
  {
    final List<T> out = new ArrayList<>(fieldMapping.size());

    for (final Pair<Integer, String> entry : fieldMapping) {
      out.add(in.get(entry.getKey()));
    }

    return out;
  }
}
