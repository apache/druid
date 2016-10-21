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
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.AggregatorFactory;
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

  @Inject
  public TimeseriesQueryQueryToolChest(IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator)
  {
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeResults(
      QueryRunner<Result<TimeseriesResultValue>> queryRunner
  )
  {
    return new ResultMergeQueryRunner<Result<TimeseriesResultValue>>(queryRunner)
    {
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
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TimeseriesQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query)
                       .setDimension(
                           "numMetrics",
                           String.valueOf(query.getAggregatorSpecs().size())
                       )
                       .setDimension(
                           "numComplexMetrics",
                           String.valueOf(DruidMetrics.findNumComplexAggs(query.getAggregatorSpecs()))
                       );
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
      public byte[] computeCacheKey(TimeseriesQuery query)
      {
        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] aggregatorBytes = QueryCacheHelper.computeAggregatorBytes(query.getAggregatorSpecs());
        final byte[] granularityBytes = query.getGranularity().cacheKey();
        final byte descending = query.isDescending() ? (byte) 1 : 0;
        final byte skipEmptyBuckets = query.isSkipEmptyBuckets() ? (byte) 1 : 0;

        return ByteBuffer
            .allocate(3 + granularityBytes.length + filterBytes.length + aggregatorBytes.length)
            .put(TIMESERIES_QUERY)
            .put(descending)
            .put(skipEmptyBuckets)
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
      public Function<Result<TimeseriesResultValue>, Object> prepareForCache()
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

            return retVal;
          }
        };
      }

      @Override
      public Function<Object, Result<TimeseriesResultValue>> pullFromCache()
      {
        return new Function<Object, Result<TimeseriesResultValue>>()
        {
          private final QueryGranularity granularity = query.getGranularity();

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
              Query<Result<TimeseriesResultValue>> query, Map<String, Object> responseContext
          )
          {
            TimeseriesQuery timeseriesQuery = (TimeseriesQuery) query;
            if (timeseriesQuery.getDimensionsFilter() != null) {
              timeseriesQuery = timeseriesQuery.withDimFilter(timeseriesQuery.getDimensionsFilter().optimize());
            }
            return runner.run(timeseriesQuery, responseContext);
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
        if (calculatePostAggs) {
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
