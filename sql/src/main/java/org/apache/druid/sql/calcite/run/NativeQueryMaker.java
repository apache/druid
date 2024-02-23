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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.util.Pair;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.server.QueryLifecycle;
import org.apache.druid.server.QueryLifecycleFactory;
import org.apache.druid.server.QueryResponse;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.CannotBuildQueryException;
import org.apache.druid.sql.calcite.rel.DruidQuery;
import org.joda.time.Interval;

import java.util.ArrayList;
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
          if (filter instanceof BoundDimFilter) {
            final BoundDimFilter bound = (BoundDimFilter) filter;
            if (StringComparators.NUMERIC.equals(bound.getOrdering())) {
              numBoundFilters++;
              if (numBoundFilters > numFilters) {
                throw new UOE(StringUtils.format(
                    "The number of values in the IN clause for [%s] in query exceeds configured maxNumericFilter limit of [%s] for INs. Cast [%s] values of IN clause to String",
                    bound.getDimension(),
                    numFilters,
                    orDimFilter.getFields().size()
                ));
              }
            }
          }
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

    final List<RelDataType> columnTypes =
        druidQuery.getOutputRowType()
                  .getFieldList()
                  .stream()
                  .map(RelDataTypeField::getType)
                  .collect(Collectors.toList());

    return execute(
        query,
        mapColumnList(rowOrder, fieldMapping),
        mapColumnList(columnTypes, fieldMapping)
    );
  }

  private List<Interval> findBaseDataSourceIntervals(Query<?> query)
  {
    return query.getDataSource().getAnalysis()
                .getBaseQuerySegmentSpec()
                .map(QuerySegmentSpec::getIntervals)
                .orElseGet(query::getIntervals);
  }

  @SuppressWarnings("unchecked")
  private <T> QueryResponse<Object[]> execute(
      Query<?> query, // Not final: may be reassigned with query ID added
      final List<String> newFields,
      final List<RelDataType> newTypes
  )
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
      final List<RelDataType> newTypes
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

    final Sequence<Object[]> sequence = toolChest.resultsAsArrays(query, results.getResults());
    final SqlResults.Context sqlResultsContext = SqlResults.Context.fromPlannerContext(plannerContext);
    return new QueryResponse<>(
        Sequences.map(
            sequence,
            array -> {
              final Object[] newArray = new Object[mapping.length];
              for (int i = 0; i < mapping.length; i++) {
                newArray[i] = SqlResults.coerce(
                    jsonMapper,
                    sqlResultsContext,
                    array[mapping[i]],
                    newTypes.get(i).getSqlTypeName(),
                    originalFields.get(mapping[i])
                );
              }
              return newArray;
            }
        ),
        results.getResponseContext()
    );
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
