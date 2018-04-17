/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.timeseries;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.CacheStrategy;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.cache.CacheKeyBuilder;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TimeseriesQueryQueryToolChest extends QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private static final byte TIMESERIES_QUERY = 0x0;
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Result<TimeseriesResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<TimeseriesResultValue>>()
      {
      };

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;
  private final TimeseriesQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public TimeseriesQueryQueryToolChest(IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator)
  {
    this(intervalChunkingQueryRunnerDecorator, DefaultTimeseriesQueryMetricsFactory.instance());
  }

  @Inject
  public TimeseriesQueryQueryToolChest(
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator,
      TimeseriesQueryMetricsFactory queryMetricsFactory
  )
  {
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeResults(
      QueryRunner<Result<TimeseriesResultValue>> queryRunner
  )
  {
    final QueryRunner<Result<TimeseriesResultValue>> resultMergeQueryRunner = new ResultMergeQueryRunner<Result<TimeseriesResultValue>>(
        queryRunner)
    {
      @Override
      public Sequence<Result<TimeseriesResultValue>> doRun(
          QueryRunner<Result<TimeseriesResultValue>> baseRunner,
          QueryPlus<Result<TimeseriesResultValue>> queryPlus,
          Map<String, Object> context
      )
      {
        return super.doRun(
            baseRunner,
            // Don't do post aggs until makePostComputeManipulatorFn() is called
            queryPlus.withQuery(((TimeseriesQuery) queryPlus.getQuery()).withPostAggregatorSpecs(ImmutableList.of())),
            context
        );
      }

      @Override
      protected Ordering<Result<TimeseriesResultValue>> makeOrdering(Query<Result<TimeseriesResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(
            ((TimeseriesQuery) query).getGranularity(), query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> createMergeFn(
          Query<Result<TimeseriesResultValue>> input
      )
      {
        TimeseriesQuery query = (TimeseriesQuery) input;
        return new TimeseriesBinaryFn(
            query.getGranularity(),
            query.getAggregatorSpecs()
        );
      }
    };

    return new QueryRunner<Result<TimeseriesResultValue>>()
    {
      @Override
      public Sequence<Result<TimeseriesResultValue>> run(
          final QueryPlus<Result<TimeseriesResultValue>> queryPlus,
          final Map<String, Object> responseContext
      )
      {
        final TimeseriesQuery query = (TimeseriesQuery) queryPlus.getQuery();
        final Sequence<Result<TimeseriesResultValue>> baseResults = resultMergeQueryRunner.run(
            queryPlus.withQuery(
                queryPlus.getQuery()
                         .withOverriddenContext(
                             ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, false)
                         )
            ),
            responseContext
        );

        if (query.isGrandTotal()) {
          // Accumulate grand totals while iterating the sequence.
          final Object[] grandTotals = new Object[query.getAggregatorSpecs().size()];
          final Sequence<Result<TimeseriesResultValue>> mappedSequence = Sequences.map(
              baseResults,
              resultValue -> {
                for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
                  final AggregatorFactory aggregatorFactory = query.getAggregatorSpecs().get(i);
                  final Object value = resultValue.getValue().getMetric(aggregatorFactory.getName());
                  if (grandTotals[i] == null) {
                    grandTotals[i] = value;
                  } else {
                    grandTotals[i] = aggregatorFactory.combine(grandTotals[i], value);
                  }
                }
                return resultValue;
              }
          );

          return Sequences.concat(
              ImmutableList.of(
                  mappedSequence,
                  Sequences.simple(
                      () -> {
                        final Map<String, Object> totalsMap = new HashMap<>();

                        for (int i = 0; i < query.getAggregatorSpecs().size(); i++) {
                          totalsMap.put(query.getAggregatorSpecs().get(i).getName(), grandTotals[i]);
                        }

                        final Result<TimeseriesResultValue> result = new Result<>(
                            null,
                            new TimeseriesResultValue(totalsMap)
                        );

                        return Collections.singletonList(result).iterator();
                      }
                  )
              )
          );
        } else {
          return baseResults;
        }
      }
    };
  }

  @Override
  public TimeseriesQueryMetrics makeMetrics(TimeseriesQuery query)
  {
    TimeseriesQueryMetrics queryMetrics = queryMetricsFactory.makeMetrics();
    queryMetrics.query(query);
    return queryMetrics;
  }

  @Override
  public TypeReference<Result<TimeseriesResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TimeseriesResultValue>, Object, TimeseriesQuery> getCacheStrategy(final TimeseriesQuery query)
  {
    return new CacheStrategy<Result<TimeseriesResultValue>, Object, TimeseriesQuery>()
    {
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();

      @Override
      public boolean isCacheable(TimeseriesQuery query, boolean willMergeRunners)
      {
        return true;
      }

      @Override
      public byte[] computeCacheKey(TimeseriesQuery query)
      {
        return new CacheKeyBuilder(TIMESERIES_QUERY)
            .appendBoolean(query.isDescending())
            .appendBoolean(query.isSkipEmptyBuckets())
            .appendCacheable(query.getGranularity())
            .appendCacheable(query.getDimensionsFilter())
            .appendCacheables(query.getAggregatorSpecs())
            .appendCacheable(query.getVirtualColumns())
            .build();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TimeseriesResultValue>, Object> prepareForCache(boolean isResultLevelCache)
      {
        return new Function<Result<TimeseriesResultValue>, Object>()
        {
          @Override
          public Object apply(final Result<TimeseriesResultValue> input)
          {
            TimeseriesResultValue results = input.getValue();
            final List<Object> retVal = Lists.newArrayListWithCapacity(1 + aggs.size());

            retVal.add(input.getTimestamp().getMillis());
            for (AggregatorFactory agg : aggs) {
              retVal.add(results.getMetric(agg.getName()));
            }
            if (isResultLevelCache) {
              for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
                retVal.add(results.getMetric(postAgg.getName()));
              }
            }
            return retVal;
          }
        };
      }

      @Override
      public Function<Object, Result<TimeseriesResultValue>> pullFromCache(boolean isResultLevelCache)
      {
        return new Function<Object, Result<TimeseriesResultValue>>()
        {
          private final Granularity granularity = query.getGranularity();

          @Override
          public Result<TimeseriesResultValue> apply(@Nullable Object input)
          {
            List<Object> results = (List<Object>) input;
            Map<String, Object> retVal = Maps.newLinkedHashMap();

            Iterator<AggregatorFactory> aggsIter = aggs.iterator();
            Iterator<Object> resultIter = results.iterator();

            DateTime timestamp = granularity.toDateTime(((Number) resultIter.next()).longValue());

            while (aggsIter.hasNext() && resultIter.hasNext()) {
              final AggregatorFactory factory = aggsIter.next();
              retVal.put(factory.getName(), factory.deserialize(resultIter.next()));
            }
            if (isResultLevelCache) {
              Iterator<PostAggregator> postItr = query.getPostAggregatorSpecs().iterator();
              while (postItr.hasNext() && resultIter.hasNext()) {
                retVal.put(postItr.next().getName(), resultIter.next());
              }
            }

            return new Result<TimeseriesResultValue>(
                timestamp,
                new TimeseriesResultValue(retVal)
            );
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> preMergeQueryDecoration(final QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus, Map<String, Object> responseContext
          )
          {
            TimeseriesQuery timeseriesQuery = (TimeseriesQuery) queryPlus.getQuery();
            if (timeseriesQuery.getDimensionsFilter() != null) {
              timeseriesQuery = timeseriesQuery.withDimFilter(timeseriesQuery.getDimensionsFilter().optimize());
              queryPlus = queryPlus.withQuery(timeseriesQuery);
            }
            return runner.run(queryPlus, responseContext);
          }
        }, this);
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makePreComputeManipulatorFn(
      final TimeseriesQuery query, final MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn, false);
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makePostComputeManipulatorFn(
      TimeseriesQuery query, MetricManipulationFn fn
  )
  {
    return makeComputeManipulatorFn(query, fn, true);
  }

  private Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makeComputeManipulatorFn(
      final TimeseriesQuery query, final MetricManipulationFn fn, final boolean calculatePostAggs
  )
  {
    return new Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>()
    {
      @Override
      public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> result)
      {
        final TimeseriesResultValue holder = result.getValue();
        final Map<String, Object> values = Maps.newHashMap(holder.getBaseObject());
        if (calculatePostAggs && !query.getPostAggregatorSpecs().isEmpty()) {
          // put non finalized aggregators for calculating dependent post Aggregators
          for (AggregatorFactory agg : query.getAggregatorSpecs()) {
            values.put(agg.getName(), holder.getMetric(agg.getName()));
          }
          for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
            values.put(postAgg.getName(), postAgg.compute(values));
          }
        }
        for (AggregatorFactory agg : query.getAggregatorSpecs()) {
          values.put(agg.getName(), fn.manipulate(agg, holder.getMetric(agg.getName())));
        }

        return new Result<TimeseriesResultValue>(
            result.getTimestamp(),
            new TimeseriesResultValue(values)
        );
      }
    };
  }
}
