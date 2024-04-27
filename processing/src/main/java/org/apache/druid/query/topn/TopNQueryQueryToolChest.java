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

package org.apache.druid.query.topn;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.frame.segment.FrameCursorUtils;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriterUtils;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.IterableRowsCursorHelper;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.ResultGranularTimestampComparator;
import org.apache.druid.query.ResultMergeQueryRunner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.RowSignature;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class TopNQueryQueryToolChest extends QueryToolChest<Result<TopNResultValue>, TopNQuery>
{
  private static final byte TOPN_QUERY = 0x1;
  private static final TypeReference<Result<TopNResultValue>> TYPE_REFERENCE = new TypeReference<Result<TopNResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  private final TopNQueryConfig config;
  private final TopNQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public TopNQueryQueryToolChest(TopNQueryConfig config)
  {
    this(config, DefaultTopNQueryMetricsFactory.instance());
  }

  @Inject
  public TopNQueryQueryToolChest(
      TopNQueryConfig config,
      TopNQueryMetricsFactory queryMetricsFactory
  )
  {
    this.config = config;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  protected static String[] extractFactoryName(final List<AggregatorFactory> aggregatorFactories)
  {
    return aggregatorFactories.stream().map(AggregatorFactory::getName).toArray(String[]::new);
  }

  private static List<PostAggregator> prunePostAggregators(TopNQuery query)
  {
    return AggregatorUtil.pruneDependentPostAgg(
        query.getPostAggregatorSpecs(),
        query.getTopNMetricSpec().getMetricName(query.getDimensionSpec())
    );
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeResults(QueryRunner<Result<TopNResultValue>> runner)
  {
    final ResultMergeQueryRunner<Result<TopNResultValue>> delegateRunner = new ResultMergeQueryRunner<>(
        runner,
        query -> ResultGranularTimestampComparator.create(query.getGranularity(), query.isDescending()),
        query -> {
          TopNQuery topNQuery = (TopNQuery) query;
          return new TopNBinaryFn(
              topNQuery.getGranularity(),
              topNQuery.getDimensionSpec(),
              topNQuery.getTopNMetricSpec(),
              topNQuery.getThreshold(),
              topNQuery.getAggregatorSpecs(),
              topNQuery.getPostAggregatorSpecs()
          );
        }
    );
    return (queryPlus, responseContext) -> {
      final TopNQuery query = (TopNQuery) queryPlus.getQuery();
      final List<PostAggregator> prunedPostAggs = prunePostAggregators(query);

      //noinspection unchecked
      return (Sequence) delegateRunner.run(
          // Rewrite to prune the post aggs for downstream queries to only the minimum required.  That is, if
          // the topN query sorts by the PostAgg, then it must be pushed down, otherwise, it can be pruned.
          queryPlus.withQuery(query.withPostAggregatorSpecs(prunedPostAggs)),
          responseContext
      ).map(
          result -> {
            final List<PostAggregator> postAggs = query.getPostAggregatorSpecs();

            if (query.context().isBySegment()) {
              @SuppressWarnings("unchecked")
              final BySegmentResultValue<Result<TopNResultValue>> bySeg =
                  (BySegmentResultValue<Result<TopNResultValue>>) result.getValue();

              final List<Result<TopNResultValue>> results = bySeg.getResults();
              final List<Result<TopNResultValue>> resultValues = new ArrayList<>(results.size());
              for (Result<TopNResultValue> bySegResult : results) {
                resultValues.add(resultWithPostAggs(postAggs, bySegResult));
              }
              return new Result<>(
                  result.getTimestamp(),
                  new BySegmentTopNResultValue(resultValues, bySeg.getSegmentId(), bySeg.getInterval())
              );
            } else {
              return resultWithPostAggs(postAggs, result);
            }
          }
      );
    };
  }

  @Override
  public TopNQueryMetrics makeMetrics(TopNQuery query)
  {
    TopNQueryMetrics queryMetrics = queryMetricsFactory.makeMetrics();
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePreComputeManipulatorFn(
      final TopNQuery query,
      final MetricManipulationFn fn
  )
  {
    //noinspection ObjectEquality
    if (MetricManipulatorFns.deserializing() != fn) {
      throw DruidException.defensive("This method can only be used to deserialize.");
    }

    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      private final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs()
                                                                   .toArray(new AggregatorFactory[0]);
      private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());

      @Override
      public Result<TopNResultValue> apply(Result<TopNResultValue> result)
      {
        final List<DimensionAndMetricValueExtractor> values = result.getValue().getValue();
        final List<DimensionAndMetricValueExtractor> newValues = new ArrayList<>(values.size());

        for (DimensionAndMetricValueExtractor input : values) {
          final Map<String, Object> map = new LinkedHashMap<>(input.getBaseObject());

          for (int i = 0; i < aggregatorFactories.length; ++i) {
            final String aggName = aggFactoryNames[i];
            map.put(aggName, aggregatorFactories[i].deserialize(map.get(aggName)));
          }

          newValues.add(new DimensionAndMetricValueExtractor(map));
        }

        return new Result<>(result.getTimestamp(), new TopNResultValue(newValues));
      }
    };
  }

  @SuppressWarnings("ObjectEquality")
  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePostComputeManipulatorFn(
      TopNQuery query,
      MetricManipulationFn fn
  )
  {
    if (MetricManipulatorFns.identity() == fn) {
      return result -> result;
    }

    if (MetricManipulatorFns.finalizing() == fn) {
      return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
      {
        private final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs()
                                                                     .toArray(new AggregatorFactory[0]);
        private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());

        @Override
        public Result<TopNResultValue> apply(Result<TopNResultValue> result)
        {
          final List<DimensionAndMetricValueExtractor> values = result.getValue().getValue();
          final List<DimensionAndMetricValueExtractor> newValues = new ArrayList<>(values.size());

          for (DimensionAndMetricValueExtractor input : values) {
            final Map<String, Object> map = new LinkedHashMap<>(input.getBaseObject());

            for (int i = 0; i < aggregatorFactories.length; ++i) {
              final String aggName = aggFactoryNames[i];
              map.put(aggName, aggregatorFactories[i].finalizeComputation(map.get(aggName)));
            }

            newValues.add(new DimensionAndMetricValueExtractor(map));
          }

          return new Result<>(result.getTimestamp(), new TopNResultValue(newValues));
        }
      };
    }

    throw DruidException.defensive("This method can only be used to finalize.");
  }

  @Override
  public TypeReference<Result<TopNResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }


  @Override
  public CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> getCacheStrategy(final TopNQuery query)
  {
    return new CacheStrategy<Result<TopNResultValue>, Object, TopNQuery>()
    {
      private final List<AggregatorFactory> aggs = Lists.newArrayList(query.getAggregatorSpecs());
      private final List<PostAggregator> postAggs = AggregatorUtil.pruneDependentPostAgg(
          query.getPostAggregatorSpecs(),
          query.getTopNMetricSpec().getMetricName(query.getDimensionSpec())
      );

      @Override
      public boolean isCacheable(TopNQuery query, boolean willMergeRunners, boolean bySegment)
      {
        return true;
      }

      @Override
      public byte[] computeCacheKey(TopNQuery query)
      {
        final CacheKeyBuilder builder = new CacheKeyBuilder(TOPN_QUERY)
            .appendCacheable(query.getDimensionSpec())
            .appendCacheable(query.getTopNMetricSpec())
            .appendInt(query.getThreshold())
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimensionsFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheable(query.getVirtualColumns());

        final List<PostAggregator> postAggregators = prunePostAggregators(query);
        if (!postAggregators.isEmpty()) {
          // Append post aggregators only when they are used as sort keys.
          // Note that appending an empty list produces a different cache key from not appending it.
          builder.appendCacheablesIgnoringOrder(postAggregators);
        }

        return builder.build();
      }

      @Override
      public byte[] computeResultLevelCacheKey(TopNQuery query)
      {
        final CacheKeyBuilder builder = new CacheKeyBuilder(TOPN_QUERY)
            .appendCacheable(query.getDimensionSpec())
            .appendCacheable(query.getTopNMetricSpec())
            .appendInt(query.getThreshold())
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimensionsFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheable(query.getVirtualColumns())
            .appendCacheables(query.getPostAggregatorSpecs());
        return builder.build();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TopNResultValue>, Object> prepareForCache(boolean isResultLevelCache)
      {
        return new Function<Result<TopNResultValue>, Object>()
        {
          private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());

          @Override
          public Object apply(final Result<TopNResultValue> input)
          {
            List<DimensionAndMetricValueExtractor> results = Lists.newArrayList(input.getValue());
            final List<Object> retVal = Lists.newArrayListWithCapacity(results.size() + 1);

            // make sure to preserve timezone information when caching results
            retVal.add(input.getTimestamp().getMillis());
            for (DimensionAndMetricValueExtractor result : results) {
              List<Object> vals = Lists.newArrayListWithCapacity(aggFactoryNames.length + 2);
              vals.add(result.getDimensionValue(query.getDimensionSpec().getOutputName()));
              for (String aggName : aggFactoryNames) {
                vals.add(result.getMetric(aggName));
              }
              if (isResultLevelCache) {
                for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
                  vals.add(result.getMetric(postAgg.getName()));
                }
              }
              retVal.add(vals);
            }
            return retVal;
          }
        };
      }

      @Override
      public Function<Object, Result<TopNResultValue>> pullFromCache(boolean isResultLevelCache)
      {
        return new Function<Object, Result<TopNResultValue>>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          public Result<TopNResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            List<Map<String, Object>> retVal = Lists.newArrayListWithCapacity(results.size());

            Iterator<Object> inputIter = results.iterator();
            DateTime timestamp = granularity.toDateTime(((Number) inputIter.next()).longValue());

            while (inputIter.hasNext()) {
              List<Object> result = (List<Object>) inputIter.next();
              final Map<String, Object> vals = Maps.newLinkedHashMap();

              Iterator<Object> resultIter = result.iterator();

              // Must convert generic Jackson-deserialized type into the proper type.
              vals.put(
                  query.getDimensionSpec().getOutputName(),
                  DimensionHandlerUtils.convertObjectToType(resultIter.next(), query.getDimensionSpec().getOutputType())
              );

              CacheStrategy.fetchAggregatorsFromCache(
                  aggs,
                  resultIter,
                  isResultLevelCache,
                  (aggName, aggPos, aggValueObject) -> {
                    vals.put(aggName, aggValueObject);
                  }
              );

              if (isResultLevelCache) {
                Iterator<PostAggregator> postItr = query.getPostAggregatorSpecs().iterator();
                while (postItr.hasNext() && resultIter.hasNext()) {
                  vals.put(postItr.next().getName(), resultIter.next());
                }
              } else {
                for (PostAggregator postAgg : postAggs) {
                  vals.put(postAgg.getName(), postAgg.compute(vals));
                }
              }
              retVal.add(vals);
            }

            return new Result<>(timestamp, TopNResultValue.create(retVal));
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> preMergeQueryDecoration(final QueryRunner<Result<TopNResultValue>> runner)
  {
    return (queryPlus, responseContext) -> {
      TopNQuery topNQuery = (TopNQuery) queryPlus.getQuery();
      if (TopNQueryEngine.canApplyExtractionInPost(topNQuery)) {
        final DimensionSpec dimensionSpec = topNQuery.getDimensionSpec();
        QueryPlus<Result<TopNResultValue>> delegateQueryPlus = queryPlus.withQuery(
            topNQuery.withDimensionSpec(
                new DefaultDimensionSpec(
                    dimensionSpec.getDimension(),
                    dimensionSpec.getOutputName()
                )
            )
        );
        return runner.run(delegateQueryPlus, responseContext);
      } else {
        return runner.run(queryPlus.withQuery(topNQuery), responseContext);
      }
    };
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> postMergeQueryDecoration(final QueryRunner<Result<TopNResultValue>> runner)
  {
    final ThresholdAdjustingQueryRunner thresholdRunner = new ThresholdAdjustingQueryRunner(
        runner,
        config
    );
    return new QueryRunner<Result<TopNResultValue>>()
    {

      @Override
      public Sequence<Result<TopNResultValue>> run(
          final QueryPlus<Result<TopNResultValue>> queryPlus, final ResponseContext responseContext
      )
      {
        // thresholdRunner.run throws ISE if query is not TopNQuery
        final Sequence<Result<TopNResultValue>> resultSequence = thresholdRunner.run(queryPlus, responseContext);
        final TopNQuery topNQuery = (TopNQuery) queryPlus.getQuery();
        if (!TopNQueryEngine.canApplyExtractionInPost(topNQuery)) {
          return resultSequence;
        } else {
          return Sequences.map(
              resultSequence,
              new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
              {
                @Override
                public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                {
                  TopNResultValue resultValue = input.getValue();

                  return new Result<TopNResultValue>(
                      input.getTimestamp(),
                      TopNResultValue.create(
                          Lists.transform(
                              resultValue.getValue(),
                              new Function<DimensionAndMetricValueExtractor, DimensionAndMetricValueExtractor>()
                              {
                                @Override
                                public DimensionAndMetricValueExtractor apply(
                                    DimensionAndMetricValueExtractor input
                                )
                                {
                                  String dimOutputName = topNQuery.getDimensionSpec().getOutputName();
                                  Object dimValue = input.getDimensionValue(dimOutputName);
                                  Map<String, Object> map = input.getBaseObject();
                                  map.put(
                                      dimOutputName,
                                      topNQuery.getDimensionSpec().getExtractionFn().apply(dimValue)
                                  );
                                  return input;
                                }
                              }
                          )
                      )
                  );
                }
              }
          );
        }
      }
    };
  }

  @Override
  public RowSignature resultArraySignature(TopNQuery query)
  {
    return RowSignature.builder()
                       .addTimeColumn()
                       .addDimensions(Collections.singletonList(query.getDimensionSpec()))
                       .addAggregators(query.getAggregatorSpecs(), RowSignature.Finalization.UNKNOWN)
                       .addPostAggregators(query.getPostAggregatorSpecs())
                       .build();
  }

  @Override
  public Sequence<Object[]> resultsAsArrays(TopNQuery query, Sequence<Result<TopNResultValue>> resultSequence)
  {
    final List<String> fields = resultArraySignature(query).getColumnNames();

    return resultSequence.flatMap(
        result -> {
          final List<DimensionAndMetricValueExtractor> rows = result.getValue().getValue();

          return Sequences.simple(
              Iterables.transform(
                  rows,
                  row -> {
                    final Object[] retVal = new Object[fields.size()];

                    // Position 0 is always __time.
                    retVal[0] = result.getTimestamp().getMillis();

                    // Add other fields.
                    final Map<String, Object> resultMap = row.getBaseObject();
                    for (int i = 1; i < fields.size(); i++) {
                      retVal[i] = resultMap.get(fields.get(i));
                    }

                    return retVal;
                  }
              )
          );
        }
    );
  }

  /**
   * This returns a single frame containing the rows of the topN query's results
   */
  @Override
  public Optional<Sequence<FrameSignaturePair>> resultsAsFrames(
      TopNQuery query,
      Sequence<Result<TopNResultValue>> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes
  )
  {
    final RowSignature rowSignature = resultArraySignature(query);
    final Pair<Cursor, Closeable> cursorAndCloseable = IterableRowsCursorHelper.getCursorFromSequence(
        resultsAsArrays(query, resultSequence),
        rowSignature
    );
    Cursor cursor = cursorAndCloseable.lhs;
    Closeable closeable = cursorAndCloseable.rhs;

    RowSignature modifiedRowSignature = useNestedForUnknownTypes
                                        ? FrameWriterUtils.replaceUnknownTypesWithNestedColumns(rowSignature)
                                        : rowSignature;
    FrameWriterFactory frameWriterFactory = FrameWriters.makeFrameWriterFactory(
        FrameType.COLUMNAR,
        memoryAllocatorFactory,
        rowSignature,
        new ArrayList<>()
    );

    Sequence<Frame> frames = FrameCursorUtils.cursorToFramesSequence(cursor, frameWriterFactory).withBaggage(closeable);

    return Optional.of(frames.map(frame -> new FrameSignaturePair(frame, modifiedRowSignature)));
  }

  private Result<TopNResultValue> resultWithPostAggs(List<PostAggregator> postAggs, Result<TopNResultValue> result)
  {
    final List<DimensionAndMetricValueExtractor> values = result.getValue().getValue();
    final List<DimensionAndMetricValueExtractor> newValues = new ArrayList<>(values.size());

    for (DimensionAndMetricValueExtractor input : values) {
      final Map<String, Object> map = new LinkedHashMap<>(input.getBaseObject());

      for (PostAggregator postAgg : postAggs) {
        map.put(postAgg.getName(), postAgg.compute(map));
      }

      newValues.add(new DimensionAndMetricValueExtractor(map));
    }

    return new Result<>(result.getTimestamp(), new TopNResultValue(newValues));
  }

  static class ThresholdAdjustingQueryRunner implements QueryRunner<Result<TopNResultValue>>
  {
    private final QueryRunner<Result<TopNResultValue>> runner;
    private final TopNQueryConfig config;

    public ThresholdAdjustingQueryRunner(
        QueryRunner<Result<TopNResultValue>> runner,
        TopNQueryConfig config
    )
    {
      this.runner = runner;
      this.config = config;
    }

    @Override
    public Sequence<Result<TopNResultValue>> run(
        QueryPlus<Result<TopNResultValue>> queryPlus,
        ResponseContext responseContext
    )
    {
      Query<Result<TopNResultValue>> input = queryPlus.getQuery();
      if (!(input instanceof TopNQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", TopNQuery.class, input.getClass());
      }

      final TopNQuery query = (TopNQuery) input;
      final int minTopNThreshold = query.context()
                                        .getInt(QueryContexts.MIN_TOP_N_THRESHOLD, config.getMinTopNThreshold());
      if (query.getThreshold() > minTopNThreshold) {
        return runner.run(queryPlus, responseContext);
      }

      final boolean isBySegment = query.context().isBySegment();

      return Sequences.map(
          runner.run(queryPlus.withQuery(query.withThreshold(minTopNThreshold)), responseContext),
          new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
          {
            @Override
            public Result<TopNResultValue> apply(Result<TopNResultValue> input)
            {
              if (isBySegment) {
                BySegmentResultValue<Result<TopNResultValue>> value = (BySegmentResultValue<Result<TopNResultValue>>) input
                    .getValue();

                return new Result<>(
                    input.getTimestamp(),
                    new BySegmentTopNResultValue(
                        Lists.transform(
                            value.getResults(),
                            new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
                            {
                              @Override
                              public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                              {
                                return new Result<>(
                                    input.getTimestamp(),
                                    TopNResultValue.create(
                                        Lists.<Object>newArrayList(
                                            Iterables.limit(
                                                input.getValue(),
                                                query.getThreshold()
                                            )
                                        )
                                    )
                                );
                              }
                            }
                        ),
                        value.getSegmentId(),
                        value.getInterval()
                    )
                );
              }

              return new Result<>(
                  input.getTimestamp(),
                  TopNResultValue.create(
                      Lists.<Object>newArrayList(
                          Iterables.limit(
                              input.getValue(),
                              query.getThreshold()
                          )
                      )
                  )
              );
            }
          }
      );
    }
  }
}
