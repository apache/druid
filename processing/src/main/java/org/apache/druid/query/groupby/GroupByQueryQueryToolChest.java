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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.MappedSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
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
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.groupby.resource.GroupByQueryResource;
import org.apache.druid.query.groupby.strategy.GroupByStrategy;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BinaryOperator;

/**
 *
 */
public class GroupByQueryQueryToolChest extends QueryToolChest<ResultRow, GroupByQuery>
{
  private static final byte GROUPBY_QUERY = 0x14;
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<ResultRow> TYPE_REFERENCE = new TypeReference<ResultRow>()
  {
  };
  public static final String GROUP_BY_MERGE_KEY = "groupByMerge";

  private final GroupByStrategySelector strategySelector;
  private final GroupByQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public GroupByQueryQueryToolChest(GroupByStrategySelector strategySelector)
  {
    this(strategySelector, DefaultGroupByQueryMetricsFactory.instance());
  }

  @Inject
  public GroupByQueryQueryToolChest(
      GroupByStrategySelector strategySelector,
      GroupByQueryMetricsFactory queryMetricsFactory
  )
  {
    this.strategySelector = strategySelector;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<ResultRow> mergeResults(final QueryRunner<ResultRow> runner)
  {
    return (queryPlus, responseContext) -> {
      if (QueryContexts.isBySegment(queryPlus.getQuery())) {
        return runner.run(queryPlus, responseContext);
      }

      final GroupByQuery groupByQuery = (GroupByQuery) queryPlus.getQuery();
      if (strategySelector.strategize(groupByQuery).doMergeResults(groupByQuery)) {
        return initAndMergeGroupByResults(groupByQuery, runner, responseContext);
      }
      return runner.run(queryPlus, responseContext);
    };
  }

  @Override
  public BinaryOperator<ResultRow> createMergeFn(Query<ResultRow> query)
  {
    return strategySelector.strategize((GroupByQuery) query).createMergeFn(query);
  }

  @Override
  public Comparator<ResultRow> createResultComparator(Query<ResultRow> query)
  {
    return strategySelector.strategize((GroupByQuery) query).createResultComparator(query);
  }

  private Sequence<ResultRow> initAndMergeGroupByResults(
      final GroupByQuery query,
      QueryRunner<ResultRow> runner,
      ResponseContext context
  )
  {
    final GroupByStrategy groupByStrategy = strategySelector.strategize(query);
    final GroupByQueryResource resource = groupByStrategy.prepareResource(query);
    try {
      final Sequence<ResultRow> mergedSequence = mergeGroupByResults(
          groupByStrategy,
          query,
          resource,
          runner,
          context
      );

      return Sequences.withBaggage(mergedSequence, resource);
    }
    catch (Exception e) {
      // Error creating the Sequence; release resources.
      resource.close();
      throw e;
    }
  }

  private Sequence<ResultRow> mergeGroupByResults(
      GroupByStrategy groupByStrategy,
      final GroupByQuery query,
      GroupByQueryResource resource,
      QueryRunner<ResultRow> runner,
      ResponseContext context
  )
  {
    if (isNestedQueryPushDown(query, groupByStrategy)) {
      return mergeResultsWithNestedQueryPushDown(groupByStrategy, query, resource, runner, context);
    }
    return mergeGroupByResultsWithoutPushDown(groupByStrategy, query, resource, runner, context);
  }

  private Sequence<ResultRow> mergeGroupByResultsWithoutPushDown(
      GroupByStrategy groupByStrategy,
      GroupByQuery query,
      GroupByQueryResource resource,
      QueryRunner<ResultRow> runner,
      ResponseContext context
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

      final Sequence<ResultRow> subqueryResult = mergeGroupByResults(
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

      final Sequence<ResultRow> finalizingResults = finalizeSubqueryResults(subqueryResult, subquery);

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

  private Sequence<ResultRow> mergeResultsWithNestedQueryPushDown(
      GroupByStrategy groupByStrategy,
      GroupByQuery query,
      GroupByQueryResource resource,
      QueryRunner<ResultRow> runner,
      ResponseContext context
  )
  {
    Sequence<ResultRow> pushDownQueryResults = groupByStrategy.mergeResults(runner, query, context);
    final Sequence<ResultRow> finalizedResults = finalizeSubqueryResults(pushDownQueryResults, query);
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

  private Sequence<ResultRow> finalizeSubqueryResults(Sequence<ResultRow> subqueryResult, GroupByQuery subquery)
  {
    final Sequence<ResultRow> finalizingResults;
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
  public Function<ResultRow, ResultRow> makePreComputeManipulatorFn(
      final GroupByQuery query,
      final MetricManipulationFn fn
  )
  {
    if (MetricManipulatorFns.identity().equals(fn)) {
      return Functions.identity();
    }

    return row -> {
      final ResultRow newRow = row.copy();
      final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
      final int aggregatorStart = query.getResultRowAggregatorStart();

      for (int i = 0; i < aggregatorSpecs.size(); i++) {
        AggregatorFactory agg = aggregatorSpecs.get(i);
        newRow.set(aggregatorStart + i, fn.manipulate(agg, row.get(aggregatorStart + i)));
      }

      return newRow;
    };
  }

  @Override
  public Function<ResultRow, ResultRow> makePostComputeManipulatorFn(
      final GroupByQuery query,
      final MetricManipulationFn fn
  )
  {
    final BitSet optimizedDims = extractionsToRewrite(query);
    final Function<ResultRow, ResultRow> preCompute = makePreComputeManipulatorFn(query, fn);

    if (optimizedDims.isEmpty()) {
      return preCompute;
    }

    // If we have optimizations that can be done at this level, we apply them here

    final List<DimensionSpec> dimensions = query.getDimensions();
    final List<ExtractionFn> extractionFns = new ArrayList<>(dimensions.size());
    for (int i = 0; i < dimensions.size(); i++) {
      final DimensionSpec dimensionSpec = dimensions.get(i);
      final ExtractionFn extractionFnToAdd;

      if (optimizedDims.get(i)) {
        extractionFnToAdd = dimensionSpec.getExtractionFn();
      } else {
        extractionFnToAdd = null;
      }

      extractionFns.add(extractionFnToAdd);
    }

    final int dimensionStart = query.getResultRowDimensionStart();
    return row -> {
      // preCompute.apply(row) will either return the original row, or create a copy.
      ResultRow newRow = preCompute.apply(row);

      //noinspection ObjectEquality (if preCompute made a copy, no need to make another copy)
      if (newRow == row) {
        newRow = row.copy();
      }

      for (int i = optimizedDims.nextSetBit(0); i >= 0; i = optimizedDims.nextSetBit(i + 1)) {
        newRow.set(
            dimensionStart + i,
            extractionFns.get(i).apply(newRow.get(dimensionStart + i))
        );
      }

      return newRow;
    };
  }

  @Override
  public TypeReference<ResultRow> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public ObjectMapper decorateObjectMapper(final ObjectMapper objectMapper, final GroupByQuery query)
  {
    final boolean resultAsArray = query.getContextBoolean(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, false);

    // Serializer that writes array- or map-based rows as appropriate, based on the "resultAsArray" setting.
    final JsonSerializer<ResultRow> serializer = new JsonSerializer<ResultRow>()
    {
      @Override
      public void serialize(
          final ResultRow resultRow,
          final JsonGenerator jg,
          final SerializerProvider serializers
      ) throws IOException
      {
        if (resultAsArray) {
          jg.writeObject(resultRow.getArray());
        } else {
          jg.writeObject(resultRow.toMapBasedRow(query));
        }
      }
    };

    // Deserializer that can deserialize either array- or map-based rows.
    final JsonDeserializer<ResultRow> deserializer = new JsonDeserializer<ResultRow>()
    {
      @Override
      public ResultRow deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException
      {
        if (jp.isExpectedStartObjectToken()) {
          final Row row = jp.readValueAs(Row.class);
          return ResultRow.fromLegacyRow(row, query);
        } else {
          return ResultRow.of(jp.readValueAs(Object[].class));
        }
      }
    };

    class GroupByResultRowModule extends SimpleModule
    {
      private GroupByResultRowModule()
      {
        addSerializer(ResultRow.class, serializer);
        addDeserializer(ResultRow.class, deserializer);
      }
    }

    final ObjectMapper newObjectMapper = objectMapper.copy();
    newObjectMapper.registerModule(new GroupByResultRowModule());
    return newObjectMapper;
  }

  @Override
  public QueryRunner<ResultRow> preMergeQueryDecoration(final QueryRunner<ResultRow> runner)
  {
    return new SubqueryQueryRunner<>(
        new QueryRunner<ResultRow>()
        {
          @Override
          public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
          {
            GroupByQuery groupByQuery = (GroupByQuery) queryPlus.getQuery();
            if (groupByQuery.getDimFilter() != null) {
              groupByQuery = groupByQuery.withDimFilter(groupByQuery.getDimFilter().optimize());
            }
            final GroupByQuery delegateGroupByQuery = groupByQuery;
            final List<DimensionSpec> dimensionSpecs = new ArrayList<>();
            final BitSet optimizedDimensions = extractionsToRewrite(delegateGroupByQuery);
            final List<DimensionSpec> dimensions = delegateGroupByQuery.getDimensions();
            for (int i = 0; i < dimensions.size(); i++) {
              final DimensionSpec dimensionSpec = dimensions.get(i);
              if (optimizedDimensions.get(i)) {
                dimensionSpecs.add(
                    new DefaultDimensionSpec(dimensionSpec.getDimension(), dimensionSpec.getOutputName())
                );
              } else {
                dimensionSpecs.add(dimensionSpec);
              }
            }

            return runner.run(
                queryPlus.withQuery(delegateGroupByQuery.withDimensionSpecs(dimensionSpecs)),
                responseContext
            );
          }
        }
    );
  }

  @Override
  public CacheStrategy<ResultRow, Object, GroupByQuery> getCacheStrategy(final GroupByQuery query)
  {
    return new CacheStrategy<ResultRow, Object, GroupByQuery>()
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
      public Function<ResultRow, Object> prepareForCache(boolean isResultLevelCache)
      {
        final boolean resultRowHasTimestamp = query.getResultRowHasTimestamp();

        return new Function<ResultRow, Object>()
        {
          @Override
          public Object apply(ResultRow resultRow)
          {
            final List<Object> retVal = new ArrayList<>(1 + dims.size() + aggs.size());
            int inPos = 0;

            if (resultRowHasTimestamp) {
              retVal.add(resultRow.getLong(inPos++));
            } else {
              retVal.add(query.getUniversalTimestamp().getMillis());
            }

            for (int i = 0; i < dims.size(); i++) {
              retVal.add(resultRow.get(inPos++));
            }
            for (int i = 0; i < aggs.size(); i++) {
              retVal.add(resultRow.get(inPos++));
            }
            if (isResultLevelCache) {
              for (int i = 0; i < query.getPostAggregatorSpecs().size(); i++) {
                retVal.add(resultRow.get(inPos++));
              }
            }
            return retVal;
          }
        };
      }

      @Override
      public Function<Object, ResultRow> pullFromCache(boolean isResultLevelCache)
      {
        final boolean resultRowHasTimestamp = query.getResultRowHasTimestamp();
        final int dimensionStart = query.getResultRowDimensionStart();
        final int aggregatorStart = query.getResultRowAggregatorStart();
        final int postAggregatorStart = query.getResultRowPostAggregatorStart();

        return new Function<Object, ResultRow>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          public ResultRow apply(Object input)
          {
            Iterator<Object> results = ((List<Object>) input).iterator();

            DateTime timestamp = granularity.toDateTime(((Number) results.next()).longValue());

            final int size = isResultLevelCache
                             ? query.getResultRowSizeWithPostAggregators()
                             : query.getResultRowSizeWithoutPostAggregators();

            final ResultRow resultRow = ResultRow.create(size);

            if (resultRowHasTimestamp) {
              resultRow.set(0, timestamp.getMillis());
            }

            final Iterator<DimensionSpec> dimsIter = dims.iterator();
            int dimPos = 0;
            while (dimsIter.hasNext() && results.hasNext()) {
              final DimensionSpec dimensionSpec = dimsIter.next();

              // Must convert generic Jackson-deserialized type into the proper type.
              resultRow.set(
                  dimensionStart + dimPos,
                  DimensionHandlerUtils.convertObjectToType(results.next(), dimensionSpec.getOutputType())
              );

              dimPos++;
            }

            CacheStrategy.fetchAggregatorsFromCache(
                aggs,
                results,
                isResultLevelCache,
                (aggName, aggPosition, aggValueObject) -> {
                  resultRow.set(aggregatorStart + aggPosition, aggValueObject);
                }
            );

            if (isResultLevelCache) {
              Iterator<PostAggregator> postItr = query.getPostAggregatorSpecs().iterator();
              int postPos = 0;
              while (postItr.hasNext() && results.hasNext()) {
                resultRow.set(postAggregatorStart + postPos, results.next());
              }
            }
            if (dimsIter.hasNext() || results.hasNext()) {
              throw new ISE(
                  "Found left over objects while reading from cache!! dimsIter[%s] results[%s]",
                  dimsIter.hasNext(),
                  results.hasNext()
              );
            }

            return resultRow;
          }
        };
      }
    };
  }

  @Override
  public boolean canPerformSubquery(Query<?> subquery)
  {
    Query<?> current = subquery;

    while (current != null) {
      if (!(current instanceof GroupByQuery)) {
        return false;
      }

      if (current.getDataSource() instanceof QueryDataSource) {
        current = ((QueryDataSource) current.getDataSource()).getQuery();
      } else {
        current = null;
      }
    }

    return true;
  }

  @Override
  public RowSignature resultArraySignature(GroupByQuery query)
  {
    return query.getResultRowSignature();
  }

  @Override
  public Sequence<Object[]> resultsAsArrays(final GroupByQuery query, final Sequence<ResultRow> resultSequence)
  {
    return resultSequence.map(ResultRow::getArray);
  }

  /**
   * This function checks the query for dimensions which can be optimized by applying the dimension extraction
   * as the final step of the query instead of on every event.
   *
   * @param query The query to check for optimizations
   *
   * @return The set of dimensions (as offsets into {@code query.getDimensions()}) which can be extracted at the last
   * second upon query completion.
   */
  private static BitSet extractionsToRewrite(GroupByQuery query)
  {
    final BitSet retVal = new BitSet();

    final List<DimensionSpec> dimensions = query.getDimensions();
    for (int i = 0; i < dimensions.size(); i++) {
      final DimensionSpec dimensionSpec = dimensions.get(i);
      if (dimensionSpec.getExtractionFn() != null
          && ExtractionFn.ExtractionType.ONE_TO_ONE.equals(dimensionSpec.getExtractionFn().getExtractionType())) {
        retVal.set(i);
      }
    }

    return retVal;
  }
}
