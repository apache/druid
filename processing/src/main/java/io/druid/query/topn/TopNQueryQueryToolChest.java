/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.topn;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.granularity.QueryGranularity;
import io.druid.query.CacheStrategy;
import io.druid.query.IntervalChunkingQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.filter.DimFilter;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Minutes;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TopNQueryQueryToolChest extends QueryToolChest<Result<TopNResultValue>, TopNQuery>
{
  private static final byte TOPN_QUERY = 0x1;
  private static final Joiner COMMA_JOIN = Joiner.on(",");
  private static final TypeReference<Result<TopNResultValue>> TYPE_REFERENCE = new TypeReference<Result<TopNResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };
  private final TopNQueryConfig config;

  @Inject
  public TopNQueryQueryToolChest(
      TopNQueryConfig config
  )
  {
    this.config = config;
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> mergeResults(QueryRunner<Result<TopNResultValue>> runner)
  {
    return new ResultMergeQueryRunner<Result<TopNResultValue>>(runner)
    {
      @Override
      protected Ordering<Result<TopNResultValue>> makeOrdering(Query<Result<TopNResultValue>> query)
      {
        return Ordering.from(
            new ResultGranularTimestampComparator<TopNResultValue>(
                ((TopNQuery) query).getGranularity()
            )
        );
      }

      @Override
      protected BinaryFn<Result<TopNResultValue>, Result<TopNResultValue>, Result<TopNResultValue>> createMergeFn(
          Query<Result<TopNResultValue>> input
      )
      {
        TopNQuery query = (TopNQuery) input;
        return new TopNBinaryFn(
            TopNResultMerger.identity,
            query.getGranularity(),
            query.getDimensionSpec(),
            query.getTopNMetricSpec(),
            query.getThreshold(),
            query.getAggregatorSpecs(),
            query.getPostAggregatorSpecs()
        );
      }
    };
  }

  @Override
  public Sequence<Result<TopNResultValue>> mergeSequences(Sequence<Sequence<Result<TopNResultValue>>> seqOfSequences)
  {
    return new OrderedMergeSequence<Result<TopNResultValue>>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TopNQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource().toString())
        .setUser4(String.format("topN/%s/%s", query.getThreshold(), query.getDimensionSpec().getDimension()))
        .setUser5(COMMA_JOIN.join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser7(String.format("%,d aggs", query.getAggregatorSpecs().size()))
        .setUser9(Minutes.minutes(numMinutes).toString());
  }

  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePreComputeManipulatorFn(
      final TopNQuery query, final MetricManipulationFn fn
  )
  {
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      private String dimension = query.getDimensionSpec().getOutputName();

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
                    final Map<String, Object> values = Maps.newHashMap();
                    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
                      values.put(agg.getName(), fn.manipulate(agg, input.getMetric(agg.getName())));
                    }
                    for (PostAggregator postAgg : prunePostAggregators(query)) {
                      Object calculatedPostAgg = input.getMetric(postAgg.getName());
                      if (calculatedPostAgg != null) {
                        values.put(postAgg.getName(), calculatedPostAgg);
                      } else {
                        values.put(postAgg.getName(), postAgg.compute(values));
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
      final TopNQuery query, final MetricManipulationFn fn
  )
  {
    return new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
    {
      private String dimension = query.getDimensionSpec().getOutputName();

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
                    final Map<String, Object> values = Maps.newHashMap();
                    // put non finalized aggregators for calculating dependent post Aggregators
                    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
                      values.put(agg.getName(), input.getMetric(agg.getName()));
                    }

                    for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
                      Object calculatedPostAgg = input.getMetric(postAgg.getName());
                      if (calculatedPostAgg != null) {
                        values.put(postAgg.getName(), calculatedPostAgg);
                      } else {
                        values.put(postAgg.getName(), postAgg.compute(values));
                      }
                    }
                    for (AggregatorFactory agg : query.getAggregatorSpecs()) {
                      values.put(agg.getName(), fn.manipulate(agg, input.getMetric(agg.getName())));
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
  public TypeReference<Result<TopNResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> getCacheStrategy(final TopNQuery query)
  {
    return new CacheStrategy<Result<TopNResultValue>, Object, TopNQuery>()
    {
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();
      private final List<PostAggregator> postAggs = AggregatorUtil.pruneDependentPostAgg(
          query.getPostAggregatorSpecs(),
          query.getTopNMetricSpec()
               .getMetricName(query.getDimensionSpec())
      );

      @Override
      public byte[] computeCacheKey(TopNQuery query)
      {
        final byte[] dimensionSpecBytes = query.getDimensionSpec().getCacheKey();
        final byte[] metricSpecBytes = query.getTopNMetricSpec().getCacheKey();

        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().cacheKey();

        return ByteBuffer
            .allocate(
                1 + dimensionSpecBytes.length + metricSpecBytes.length + 4 +
                granularityBytes.length + filterBytes.length + aggregatorBytes.length
            )
            .put(TOPN_QUERY)
            .put(dimensionSpecBytes)
            .put(metricSpecBytes)
            .put(Ints.toByteArray(query.getThreshold()))
            .put(granularityBytes)
            .put(filterBytes)
            .put(aggregatorBytes)
            .array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TopNResultValue>, Object> prepareForCache()
      {
        return new Function<Result<TopNResultValue>, Object>()
        {
          @Override
          public Object apply(final Result<TopNResultValue> input)
          {
            List<DimensionAndMetricValueExtractor> results = Lists.newArrayList(input.getValue());
            final List<Object> retVal = Lists.newArrayListWithCapacity(results.size() + 1);

            // make sure to preserve timezone information when caching results
            retVal.add(input.getTimestamp().getMillis());
            for (DimensionAndMetricValueExtractor result : results) {
              List<Object> vals = Lists.newArrayListWithCapacity(aggs.size() + 2);
              vals.add(result.getStringDimensionValue(query.getDimensionSpec().getOutputName()));
              for (AggregatorFactory agg : aggs) {
                vals.add(result.getMetric(agg.getName()));
              }
              retVal.add(vals);
            }
            return retVal;
          }
        };
      }

      @Override
      public Function<Object, Result<TopNResultValue>> pullFromCache()
      {
        return new Function<Object, Result<TopNResultValue>>()
        {
          private final QueryGranularity granularity = query.getGranularity();

          @Override
          public Result<TopNResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            List<Map<String, Object>> retVal = Lists.newArrayListWithCapacity(results.size());

            Iterator<Object> inputIter = results.iterator();
            DateTime timestamp = granularity.toDateTime(new DateTime(inputIter.next()).getMillis());

            while (inputIter.hasNext()) {
              List<Object> result = (List<Object>) inputIter.next();
              Map<String, Object> vals = Maps.newLinkedHashMap();

              Iterator<AggregatorFactory> aggIter = aggs.iterator();
              Iterator<Object> resultIter = result.iterator();

              vals.put(query.getDimensionSpec().getOutputName(), resultIter.next());

              while (aggIter.hasNext() && resultIter.hasNext()) {
                final AggregatorFactory factory = aggIter.next();
                vals.put(factory.getName(), factory.deserialize(resultIter.next()));
              }

              for (PostAggregator postAgg : postAggs) {
                vals.put(postAgg.getName(), postAgg.compute(vals));
              }

              retVal.add(vals);
            }

            return new Result<TopNResultValue>(timestamp, new TopNResultValue(retVal));
          }
        };
      }

      @Override
      public Sequence<Result<TopNResultValue>> mergeSequences(Sequence<Sequence<Result<TopNResultValue>>> seqOfSequences)
      {
        return new MergeSequence<Result<TopNResultValue>>(getOrdering(), seqOfSequences);
      }
    };
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> preMergeQueryDecoration(QueryRunner<Result<TopNResultValue>> runner)
  {
    return new IntervalChunkingQueryRunner<Result<TopNResultValue>>(runner, config.getChunkPeriod());
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> postMergeQueryDecoration(final QueryRunner<Result<TopNResultValue>> runner)
  {
    return new ThresholdAdjustingQueryRunner(runner, config.getMinTopNThreshold());
  }

  public Ordering<Result<TopNResultValue>> getOrdering()
  {
    return Ordering.natural();
  }

  private static class ThresholdAdjustingQueryRunner implements QueryRunner<Result<TopNResultValue>>
  {
    private final QueryRunner<Result<TopNResultValue>> runner;
    private final int minTopNThreshold;

    public ThresholdAdjustingQueryRunner(
        QueryRunner<Result<TopNResultValue>> runner,
        int minTopNThreshold
    )
    {
      this.runner = runner;
      this.minTopNThreshold = minTopNThreshold;
    }

    @Override
    public Sequence<Result<TopNResultValue>> run(Query<Result<TopNResultValue>> input)
    {
      if (!(input instanceof TopNQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", TopNQuery.class, input.getClass());
      }

      final TopNQuery query = (TopNQuery) input;
      if (query.getThreshold() > minTopNThreshold) {
        return runner.run(query);
      }

      final boolean isBySegment = query.getContextBySegment(false);

      return Sequences.map(
          runner.run(query.withThreshold(minTopNThreshold)),
          new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
          {
            @Override
            public Result<TopNResultValue> apply(Result<TopNResultValue> input)
            {
              if (isBySegment) {
                BySegmentTopNResultValue value = (BySegmentTopNResultValue) input.getValue();

                return new Result<TopNResultValue>(
                    input.getTimestamp(),
                    new BySegmentTopNResultValue(
                        Lists.transform(
                            value.getResults(),
                            new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
                            {
                              @Override
                              public Result<TopNResultValue> apply(Result<TopNResultValue> input)
                              {
                                return new Result<TopNResultValue>(
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
                        value.getIntervalString()
                    )
                );
              }

              return new Result<TopNResultValue>(
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

  private static List<PostAggregator> prunePostAggregators(TopNQuery query)
  {
    return AggregatorUtil.pruneDependentPostAgg(
        query.getPostAggregatorSpecs(),
        query.getTopNMetricSpec().getMetricName(query.getDimensionSpec())
    );
  }
}
