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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.IntervalChunkingQueryRunnerDecorator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.ResultGranularTimestampComparator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.joda.time.DateTime;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BinaryOperator;

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
  @Deprecated
  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;
  private final TopNQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public TopNQueryQueryToolChest(
      TopNQueryConfig config,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this(config, intervalChunkingQueryRunnerDecorator, DefaultTopNQueryMetricsFactory.instance());
  }

  @Inject
  public TopNQueryQueryToolChest(
      TopNQueryConfig config,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator,
      TopNQueryMetricsFactory queryMetricsFactory
  )
  {
    this.config = config;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
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
  public BinaryOperator<Result<TopNResultValue>> createMergeFn(
      Query<Result<TopNResultValue>> query
  )
  {
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

  @Override
  public Comparator<Result<TopNResultValue>> createResultComparator(Query<Result<TopNResultValue>> query)
  {
    return ResultGranularTimestampComparator.create(query.getGranularity(), query.isDescending());
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
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      private String dimension = query.getDimensionSpec().getOutputName();
      private final List<PostAggregator> prunedAggs = prunePostAggregators(query);
      private final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs()
                                                                   .toArray(new AggregatorFactory[0]);
      private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());

      @Override
      public Result<TopNResultValue> apply(Result<TopNResultValue> result)
      {
        List<Map<String, Object>> serializedValues = Lists.newArrayList(
            Iterables.transform(
                result.getValue(),
                new Function<DimensionAndMetricValueExtractor, Map<String, Object>>()
                {
                  @Override
                  public Map<String, Object> apply(DimensionAndMetricValueExtractor input)
                  {
                    final Map<String, Object> values = Maps.newHashMapWithExpectedSize(
                        aggregatorFactories.length
                        + prunedAggs.size()
                        + 1
                    );

                    for (int i = 0; i < aggregatorFactories.length; ++i) {
                      final String aggName = aggFactoryNames[i];
                      values.put(aggName, fn.manipulate(aggregatorFactories[i], input.getMetric(aggName)));
                    }

                    for (PostAggregator postAgg : prunedAggs) {
                      final String name = postAgg.getName();
                      Object calculatedPostAgg = input.getMetric(name);
                      if (calculatedPostAgg != null) {
                        values.put(name, calculatedPostAgg);
                      } else {
                        values.put(name, postAgg.compute(values));
                      }
                    }
                    values.put(dimension, input.getDimensionValue(dimension));

                    return values;
                  }
                }
            )
        );

        return new Result<TopNResultValue>(
            result.getTimestamp(),
            new TopNResultValue(serializedValues)
        );
      }
    };
  }

  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePostComputeManipulatorFn(
      final TopNQuery query,
      final MetricManipulationFn fn
  )
  {
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      private String dimension = query.getDimensionSpec().getOutputName();
      private final AggregatorFactory[] aggregatorFactories = query.getAggregatorSpecs()
                                                                   .toArray(new AggregatorFactory[0]);
      private final String[] aggFactoryNames = extractFactoryName(query.getAggregatorSpecs());
      private final PostAggregator[] postAggregators = query.getPostAggregatorSpecs().toArray(new PostAggregator[0]);

      @Override
      public Result<TopNResultValue> apply(Result<TopNResultValue> result)
      {
        List<Map<String, Object>> serializedValues = Lists.newArrayList(
            Iterables.transform(
                result.getValue(),
                new Function<DimensionAndMetricValueExtractor, Map<String, Object>>()
                {
                  @Override
                  public Map<String, Object> apply(DimensionAndMetricValueExtractor input)
                  {
                    final Map<String, Object> values = Maps.newHashMapWithExpectedSize(
                        aggregatorFactories.length
                        + query.getPostAggregatorSpecs().size()
                        + 1
                    );

                    // Put non-finalized aggregators before post-aggregators.
                    for (final String name : aggFactoryNames) {
                      values.put(name, input.getMetric(name));
                    }

                    // Put dimension, post-aggregators might depend on it.
                    values.put(dimension, input.getDimensionValue(dimension));

                    // Put post-aggregators.
                    for (PostAggregator postAgg : postAggregators) {
                      Object calculatedPostAgg = input.getMetric(postAgg.getName());
                      if (calculatedPostAgg != null) {
                        values.put(postAgg.getName(), calculatedPostAgg);
                      } else {
                        values.put(postAgg.getName(), postAgg.compute(values));
                      }
                    }

                    // Put finalized aggregators now that post-aggregators are done.
                    for (int i = 0; i < aggFactoryNames.length; ++i) {
                      final String name = aggFactoryNames[i];
                      values.put(name, fn.manipulate(aggregatorFactories[i], input.getMetric(name)));
                    }

                    return values;
                  }
                }
            )
        );

        return new Result<>(
            result.getTimestamp(),
            new TopNResultValue(serializedValues)
        );
      }
    };
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
      public boolean isCacheable(TopNQuery query, boolean willMergeRunners)
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

            return new Result<>(timestamp, new TopNResultValue(retVal));
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> preMergeQueryDecoration(final QueryRunner<Result<TopNResultValue>> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(
        new QueryRunner<Result<TopNResultValue>>()
        {
          @Override
          public Sequence<Result<TopNResultValue>> run(
              QueryPlus<Result<TopNResultValue>> queryPlus,
              ResponseContext responseContext
          )
          {
            TopNQuery topNQuery = (TopNQuery) queryPlus.getQuery();
            if (topNQuery.getDimensionsFilter() != null) {
              topNQuery = topNQuery.withDimFilter(topNQuery.getDimensionsFilter().optimize());
            }
            final TopNQuery delegateTopNQuery = topNQuery;
            if (TopNQueryEngine.canApplyExtractionInPost(delegateTopNQuery)) {
              final DimensionSpec dimensionSpec = delegateTopNQuery.getDimensionSpec();
              QueryPlus<Result<TopNResultValue>> delegateQueryPlus = queryPlus.withQuery(
                  delegateTopNQuery.withDimensionSpec(
                      new DefaultDimensionSpec(
                          dimensionSpec.getDimension(),
                          dimensionSpec.getOutputName()
                      )
                  )
              );
              return runner.run(delegateQueryPlus, responseContext);
            } else {
              return runner.run(queryPlus.withQuery(delegateTopNQuery), responseContext);
            }
          }
        },
        this
    );
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
                      new TopNResultValue(
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
      final int minTopNThreshold = query.getContextValue("minTopNThreshold", config.getMinTopNThreshold());
      if (query.getThreshold() > minTopNThreshold) {
        return runner.run(queryPlus, responseContext);
      }

      final boolean isBySegment = QueryContexts.isBySegment(query);

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
                                    new TopNResultValue(
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
                  new TopNResultValue(
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
