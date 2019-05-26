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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.MappedSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.IntervalChunkingQueryRunnerDecorator;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SubqueryQueryRunner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.query.groupby.strategy.GroupByStrategy;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 */
public class GroupByQueryQueryToolChest extends QueryToolChest<Row, GroupByQuery>
{
  private static final byte GROUPBY_QUERY = 0x14;
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>()
  {
  };
  public static final String GROUP_BY_MERGE_KEY = "groupByMerge";

  private final GroupByStrategySelector strategySelector;
  @Deprecated
  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;
  private final GroupByQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public GroupByQueryQueryToolChest(
      GroupByStrategySelector strategySelector,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this(strategySelector, intervalChunkingQueryRunnerDecorator, DefaultGroupByQueryMetricsFactory.instance());
  }

  @Inject
  public GroupByQueryQueryToolChest(
      GroupByStrategySelector strategySelector,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator,
      GroupByQueryMetricsFactory queryMetricsFactory
  )
  {
    this.strategySelector = strategySelector;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<Row> mergeResults(final QueryRunner<Row> runner)
  {
    return new QueryRunner<Row>()
    {
      @Override
      public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
      {
        if (QueryContexts.isBySegment(queryPlus.getQuery())) {
          return runner.run(queryPlus, responseContext);
        }

        final GroupByQuery groupByQuery = (GroupByQuery) queryPlus.getQuery();
        if (strategySelector.strategize(groupByQuery).doMergeResults(groupByQuery)) {
          return initAndMergeGroupByResults(groupByQuery, runner, responseContext);
        }
        return runner.run(queryPlus, responseContext);
      }
    };
  }

  private Sequence<Row> initAndMergeGroupByResults(
      final GroupByQuery query,
      QueryRunner<Row> runner,
      Map<String, Object> context
  )
  {
    final GroupByStrategy groupByStrategy = strategySelector.strategize(query);
    final GroupByQueryResource resource = groupByStrategy.prepareResource(query, false);

    return Sequences.withBaggage(mergeGroupByResults(groupByStrategy, query, resource, runner, context), resource);
  }

  private Sequence<Row> mergeGroupByResults(
      GroupByStrategy groupByStrategy,
      final GroupByQuery query,
      GroupByQueryResource resource,
      QueryRunner<Row> runner,
      Map<String, Object> context
  )
  {
    if (isNestedQueryPushDown(query, groupByStrategy)) {
      return mergeResultsWithNestedQueryPushDown(groupByStrategy, query, resource, runner, context);
    }
    return mergeGroupByResultsWithoutPushDown(groupByStrategy, query, resource, runner, context);
  }

  private Sequence<Row> mergeGroupByResultsWithoutPushDown(
      GroupByStrategy groupByStrategy,
      GroupByQuery query,
      GroupByQueryResource resource,
      QueryRunner<Row> runner,
      Map<String, Object> context
  )
  {
    // If there's a subquery, merge subquery results and then apply the aggregator

    final DataSource dataSource = query.getDataSource();

    if (dataSource instanceof QueryDataSource) {
      final GroupByQuery subquery;
      try {
        // Inject outer query context keys into subquery if they don't already exist in the subquery context.
        // Unlike withOverriddenContext's normal behavior, we want keys present in the subquery to win.
        final Map<String, Object> subqueryContext = new TreeMap<>();
        if (query.getContext() != null) {
          for (Map.Entry<String, Object> entry : query.getContext().entrySet()) {
            if (entry.getValue() != null) {
              subqueryContext.put(entry.getKey(), entry.getValue());
            }
          }
        }
        if (((QueryDataSource) dataSource).getQuery().getContext() != null) {
          subqueryContext.putAll(((QueryDataSource) dataSource).getQuery().getContext());
        }
        subqueryContext.put(GroupByQuery.CTX_KEY_SORT_BY_DIMS_FIRST, false);
        subquery = (GroupByQuery) ((QueryDataSource) dataSource).getQuery().withOverriddenContext(subqueryContext);
      }
      catch (ClassCastException e) {
        throw new UnsupportedOperationException("Subqueries must be of type 'group by'");
      }

      final Sequence<Row> subqueryResult = mergeGroupByResults(
          groupByStrategy,
          subquery.withOverriddenContext(
              ImmutableMap.of(
                  //setting sort to false avoids unnecessary sorting while merging results. we only need to sort
                  //in the end when returning results to user. (note this is only respected by groupBy v1)
                  GroupByQueryHelper.CTX_KEY_SORT_RESULTS,
                  false
              )
          ),
          resource,
          runner,
          context
      );

      final Sequence<Row> finalizingResults = finalizeSubqueryResults(subqueryResult, subquery);

      if (query.getSubtotalsSpec() != null) {
        return groupByStrategy.processSubtotalsSpec(
            query,
            resource,
            groupByStrategy.processSubqueryResult(subquery, query, resource, finalizingResults, false)
        );
      } else {
        return groupByStrategy.applyPostProcessing(groupByStrategy.processSubqueryResult(
            subquery,
            query,
            resource,
            finalizingResults,
            false
        ), query);
      }

    } else {
      if (query.getSubtotalsSpec() != null) {
        return groupByStrategy.processSubtotalsSpec(
            query,
            resource,
            groupByStrategy.mergeResults(runner, query.withSubtotalsSpec(null), context)
        );
      } else {
        return groupByStrategy.applyPostProcessing(groupByStrategy.mergeResults(runner, query, context), query);
      }
    }
  }

  private Sequence<Row> mergeResultsWithNestedQueryPushDown(
      GroupByStrategy groupByStrategy,
      GroupByQuery query,
      GroupByQueryResource resource,
      QueryRunner<Row> runner,
      Map<String, Object> context
  )
  {
    Sequence<Row> pushDownQueryResults = groupByStrategy.mergeResults(runner, query, context);
    final Sequence<Row> finalizedResults = finalizeSubqueryResults(pushDownQueryResults, query);
    GroupByQuery rewrittenQuery = rewriteNestedQueryForPushDown(query);
    return groupByStrategy.applyPostProcessing(groupByStrategy.processSubqueryResult(
        query,
        rewrittenQuery,
        resource,
        finalizedResults,
        true
    ), query);
  }

  /**
   * Rewrite the aggregator and dimension specs since the push down nested query will return
   * results with dimension and aggregation specs of the original nested query.
   */
  @VisibleForTesting
  GroupByQuery rewriteNestedQueryForPushDown(GroupByQuery query)
  {
    return query.withAggregatorSpecs(Lists.transform(query.getAggregatorSpecs(), (agg) -> agg.getCombiningFactory()))
                .withDimensionSpecs(Lists.transform(
                    query.getDimensions(),
                    (dim) -> new DefaultDimensionSpec(
                        dim.getOutputName(),
                        dim.getOutputName(),
                        dim.getOutputType()
                    )
                ));
  }

  private Sequence<Row> finalizeSubqueryResults(Sequence<Row> subqueryResult, GroupByQuery subquery)
  {
    final Sequence<Row> finalizingResults;
    if (QueryContexts.isFinalize(subquery, false)) {
      finalizingResults = new MappedSequence<>(
          subqueryResult,
          makePreComputeManipulatorFn(
              subquery,
              MetricManipulatorFns.finalizing()
          )::apply
      );
    } else {
      finalizingResults = subqueryResult;
    }
    return finalizingResults;
  }

  public static boolean isNestedQueryPushDown(GroupByQuery q, GroupByStrategy strategy)
  {
    return q.getDataSource() instanceof QueryDataSource
           && q.getContextBoolean(GroupByQueryConfig.CTX_KEY_FORCE_PUSH_DOWN_NESTED_QUERY, false)
           && q.getSubtotalsSpec() == null
           && strategy.supportsNestedQueryPushDown();
  }

  @Override
  public GroupByQueryMetrics makeMetrics(GroupByQuery query)
  {
    GroupByQueryMetrics queryMetrics = queryMetricsFactory.makeMetrics();
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  public Function<Row, Row> makePreComputeManipulatorFn(
      final GroupByQuery query,
      final MetricManipulationFn fn
  )
  {
    if (MetricManipulatorFns.identity().equals(fn)) {
      return Functions.identity();
    }

    return new Function<Row, Row>()
    {
      @Override
      public Row apply(Row input)
      {
        if (input instanceof MapBasedRow) {
          final MapBasedRow inputRow = (MapBasedRow) input;
          final Map<String, Object> values = new HashMap<>(inputRow.getEvent());
          for (AggregatorFactory agg : query.getAggregatorSpecs()) {
            values.put(agg.getName(), fn.manipulate(agg, inputRow.getEvent().get(agg.getName())));
          }
          return new MapBasedRow(inputRow.getTimestamp(), values);
        }
        return input;
      }
    };
  }

  @Override
  public Function<Row, Row> makePostComputeManipulatorFn(
      final GroupByQuery query,
      final MetricManipulationFn fn
  )
  {
    final Set<String> optimizedDims = ImmutableSet.copyOf(
        Iterables.transform(
            extractionsToRewrite(query),
            new Function<DimensionSpec, String>()
            {
              @Override
              public String apply(DimensionSpec input)
              {
                return input.getOutputName();
              }
            }
        )
    );
    final Function<Row, Row> preCompute = makePreComputeManipulatorFn(query, fn);
    if (optimizedDims.isEmpty()) {
      return preCompute;
    }

    // If we have optimizations that can be done at this level, we apply them here

    final Map<String, ExtractionFn> extractionFnMap = new HashMap<>();
    for (DimensionSpec dimensionSpec : query.getDimensions()) {
      final String dimension = dimensionSpec.getOutputName();
      if (optimizedDims.contains(dimension)) {
        extractionFnMap.put(dimension, dimensionSpec.getExtractionFn());
      }
    }

    return new Function<Row, Row>()
    {
      @Nullable
      @Override
      public Row apply(Row input)
      {
        Row preRow = preCompute.apply(input);
        if (preRow instanceof MapBasedRow) {
          MapBasedRow preMapRow = (MapBasedRow) preRow;
          Map<String, Object> event = new HashMap<>(preMapRow.getEvent());
          for (String dim : optimizedDims) {
            final Object eventVal = event.get(dim);
            event.put(dim, extractionFnMap.get(dim).apply(eventVal));
          }
          return new MapBasedRow(preMapRow.getTimestamp(), event);
        } else {
          return preRow;
        }
      }
    };
  }

  @Override
  public TypeReference<Row> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public QueryRunner<Row> preMergeQueryDecoration(final QueryRunner<Row> runner)
  {
    return new SubqueryQueryRunner<>(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            GroupByQuery groupByQuery = (GroupByQuery) queryPlus.getQuery();
            if (groupByQuery.getDimFilter() != null) {
              groupByQuery = groupByQuery.withDimFilter(groupByQuery.getDimFilter().optimize());
            }
            final GroupByQuery delegateGroupByQuery = groupByQuery;
            ArrayList<DimensionSpec> dimensionSpecs = new ArrayList<>();
            Set<String> optimizedDimensions = ImmutableSet.copyOf(
                Iterables.transform(
                    extractionsToRewrite(delegateGroupByQuery),
                    new Function<DimensionSpec, String>()
                    {
                      @Override
                      public String apply(DimensionSpec input)
                      {
                        return input.getDimension();
                      }
                    }
                )
            );
            for (DimensionSpec dimensionSpec : delegateGroupByQuery.getDimensions()) {
              if (optimizedDimensions.contains(dimensionSpec.getDimension())) {
                dimensionSpecs.add(
                    new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionSpec.getOutputName())
                );
              } else {
                dimensionSpecs.add(dimensionSpec);
              }
            }

            return strategySelector.strategize(delegateGroupByQuery)
                                   .createIntervalChunkingRunner(
                                       intervalChunkingQueryRunnerDecorator,
                                       runner,
                                       GroupByQueryQueryToolChest.this
                                   )
                                   .run(
                                       queryPlus.withQuery(delegateGroupByQuery.withDimensionSpecs(dimensionSpecs)),
                                       responseContext
                                   );
          }
        }
    );
  }

  @Override
  public CacheStrategy<Row, Object, GroupByQuery> getCacheStrategy(final GroupByQuery query)
  {
    return new CacheStrategy<Row, Object, GroupByQuery>()
    {
      private static final byte CACHE_STRATEGY_VERSION = 0x1;
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();
      private final List<DimensionSpec> dims = query.getDimensions();

      @Override
      public boolean isCacheable(GroupByQuery query, boolean willMergeRunners)
      {
        return strategySelector.strategize(query).isCacheable(willMergeRunners);
      }

      @Override
      public byte[] computeCacheKey(GroupByQuery query)
      {
        return new CacheKeyBuilder(GROUPBY_QUERY)
            .appendByte(CACHE_STRATEGY_VERSION)
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheables(query.getDimensions())
            .appendCacheable(query.getVirtualColumns())
            .build();
      }

      @Override
      public byte[] computeResultLevelCacheKey(GroupByQuery query)
      {
        final CacheKeyBuilder builder = new CacheKeyBuilder(GROUPBY_QUERY)
            .appendByte(CACHE_STRATEGY_VERSION)
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheables(query.getDimensions())
            .appendCacheable(query.getVirtualColumns())
            .appendCacheable(query.getHavingSpec())
            .appendCacheable(query.getLimitSpec())
            .appendCacheables(query.getPostAggregatorSpecs());

        if (query.getSubtotalsSpec() != null && !query.getSubtotalsSpec().isEmpty()) {
          for (List<String> subTotalSpec : query.getSubtotalsSpec()) {
            builder.appendStrings(subTotalSpec);
          }
        }
        return builder.build();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Row, Object> prepareForCache(boolean isResultLevelCache)
      {
        return new Function<Row, Object>()
        {
          @Override
          public Object apply(Row input)
          {
            if (input instanceof MapBasedRow) {
              final MapBasedRow row = (MapBasedRow) input;
              final List<Object> retVal = Lists.newArrayListWithCapacity(1 + dims.size() + aggs.size());
              retVal.add(row.getTimestamp().getMillis());
              Map<String, Object> event = row.getEvent();
              for (DimensionSpec dim : dims) {
                retVal.add(event.get(dim.getOutputName()));
              }
              for (AggregatorFactory agg : aggs) {
                retVal.add(event.get(agg.getName()));
              }
              if (isResultLevelCache) {
                for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
                  retVal.add(event.get(postAgg.getName()));
                }
              }
              return retVal;
            }

            throw new ISE("Don't know how to cache input rows of type[%s]", input.getClass());
          }
        };
      }

      @Override
      public Function<Object, Row> pullFromCache(boolean isResultLevelCache)
      {
        return new Function<Object, Row>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          public Row apply(Object input)
          {
            Iterator<Object> results = ((List<Object>) input).iterator();

            DateTime timestamp = granularity.toDateTime(((Number) results.next()).longValue());

            final Map<String, Object> event = Maps.newLinkedHashMap();
            Iterator<DimensionSpec> dimsIter = dims.iterator();
            while (dimsIter.hasNext() && results.hasNext()) {
              final DimensionSpec dimensionSpec = dimsIter.next();

              // Must convert generic Jackson-deserialized type into the proper type.
              event.put(
                  dimensionSpec.getOutputName(),
                  DimensionHandlerUtils.convertObjectToType(results.next(), dimensionSpec.getOutputType())
              );
            }
            Iterator<AggregatorFactory> aggsIter = aggs.iterator();

            CacheStrategy.fetchAggregatorsFromCache(
                aggsIter,
                results,
                isResultLevelCache,
                (aggName, aggValueObject) -> {
                  event.put(aggName, aggValueObject);
                  return null;
                }
            );

            if (isResultLevelCache) {
              Iterator<PostAggregator> postItr = query.getPostAggregatorSpecs().iterator();
              while (postItr.hasNext() && results.hasNext()) {
                event.put(postItr.next().getName(), results.next());
              }
            }
            if (dimsIter.hasNext() || aggsIter.hasNext() || results.hasNext()) {
              throw new ISE(
                  "Found left over objects while reading from cache!! dimsIter[%s] aggsIter[%s] results[%s]",
                  dimsIter.hasNext(),
                  aggsIter.hasNext(),
                  results.hasNext()
              );
            }

            return new MapBasedRow(
                timestamp,
                event
            );
          }
        };
      }
    };
  }


  /**
   * This function checks the query for dimensions which can be optimized by applying the dimension extraction
   * as the final step of the query instead of on every event.
   *
   * @param query The query to check for optimizations
   *
   * @return A collection of DimensionsSpec which can be extracted at the last second upon query completion.
   */
  public static Collection<DimensionSpec> extractionsToRewrite(GroupByQuery query)
  {
    return Collections2.filter(
        query.getDimensions(), new Predicate<DimensionSpec>()
        {
          @Override
          public boolean apply(DimensionSpec input)
          {
            return input.getExtractionFn() != null
                   && ExtractionFn.ExtractionType.ONE_TO_ONE.equals(
                input.getExtractionFn().getExtractionType()
            );
          }
        }
    );
  }
}
