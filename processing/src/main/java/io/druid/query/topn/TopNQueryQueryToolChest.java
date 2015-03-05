/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.topn;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
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
import io.druid.query.BySegmentResultValue;
import io.druid.query.CacheStrategy;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryMetricUtil;
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
  private static final TypeReference<Result<TopNResultValue>> TYPE_REFERENCE = new TypeReference<Result<TopNResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };
  private final TopNQueryConfig config;

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public TopNQueryQueryToolChest(
      TopNQueryConfig config,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this.config = config;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  protected static String[] extractFactoryName(final List<AggregatorFactory> aggregatorFactories){
    return Lists.transform(
        aggregatorFactories, new Function<AggregatorFactory, String>()
        {
          @Nullable
          @Override
          public String apply(@Nullable AggregatorFactory input)
          {
            return input.getName();
          }
        }
    ).toArray(new String[0]);
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
    return new OrderedMergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public Sequence<Result<TopNResultValue>> mergeSequencesUnordered(Sequence<Sequence<Result<TopNResultValue>>> seqOfSequences)
  {
    return new MergeSequence<>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TopNQuery query)
  {
    return QueryMetricUtil.makeQueryTimeMetric(query)
                          .setUser4(
                              String.format(
                                  "topN/%s/%s",
                                  query.getThreshold(),
                                  query.getDimensionSpec().getDimension()
                              )
                          )
                          .setUser7(String.format("%,d aggs", query.getAggregatorSpecs().size()));
  }

  @Override
  public Function<Result<TopNResultValue>, Result<TopNResultValue>> makePreComputeManipulatorFn(
      final TopNQuery query, final MetricManipulationFn fn
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
                    // JVM couldn't optimize this too well, so this is helping it out a bit.
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
      final TopNQuery query, final MetricManipulationFn fn
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
                    // put non finalized aggregators for calculating dependent post Aggregators
                    // JVM is dumb about optimization
                    for( int i = 0; i < aggFactoryNames.length; ++i){
                      final String name = aggFactoryNames[i];
                      values.put(name, input.getMetric(name));
                    }

                    for (PostAggregator postAgg : postAggregators) {
                      Object calculatedPostAgg = input.getMetric(postAgg.getName());
                      if (calculatedPostAgg != null) {
                        values.put(postAgg.getName(), calculatedPostAgg);
                      } else {
                        values.put(postAgg.getName(), postAgg.compute(values));
                      }
                    }
                    for( int i = 0; i < aggFactoryNames.length; ++i){
                      final String name = aggFactoryNames[i];
                      values.put(name, fn.manipulate(aggregatorFactories[i], input.getMetric(name)));
                    }

                    values.put(dimension, input.getDimensionValue(dimension));

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
              vals.add(result.getStringDimensionValue(query.getDimensionSpec().getOutputName()));
              for (String aggName : aggFactoryNames) {
                vals.add(result.getMetric(aggName));
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

            return new Result<>(timestamp, new TopNResultValue(retVal));
          }
        };
      }

      @Override
      public Sequence<Result<TopNResultValue>> mergeSequences(Sequence<Sequence<Result<TopNResultValue>>> seqOfSequences)
      {
        return new MergeSequence<>(getOrdering(), seqOfSequences);
      }
    };
  }

  @Override
  public QueryRunner<Result<TopNResultValue>> preMergeQueryDecoration(QueryRunner<Result<TopNResultValue>> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(runner, this);
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
    public Sequence<Result<TopNResultValue>> run(
        Query<Result<TopNResultValue>> input,
        Map<String, Object> responseContext
    )
    {
      if (!(input instanceof TopNQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", TopNQuery.class, input.getClass());
      }

      final TopNQuery query = (TopNQuery) input;
      if (query.getThreshold() > minTopNThreshold) {
        return runner.run(query, responseContext);
      }

      final boolean isBySegment = query.getContextBySegment(false);

      return Sequences.map(
          runner.run(query.withThreshold(minTopNThreshold), responseContext),
          new Function<Result<TopNResultValue>, Result<TopNResultValue>>()
          {
            @Override
            public Result<TopNResultValue> apply(Result<TopNResultValue> input)
            {
              if (isBySegment) {
                BySegmentResultValue<Result<TopNResultValue>> value = (BySegmentResultValue<Result<TopNResultValue>>) input
                    .getValue();

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
