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

package io.druid.query.timeseries;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.collections.OrderedMergeSequence;
import io.druid.granularity.QueryGranularity;
import io.druid.query.CacheStrategy;
import io.druid.query.IntervalChunkingQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryCacheHelper;
import io.druid.query.QueryConfig;
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
import org.joda.time.Interval;
import org.joda.time.Minutes;

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
  private static final Joiner COMMA_JOIN = Joiner.on(",");
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Result<TimeseriesResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<TimeseriesResultValue>>()
      {
      };
  private final QueryConfig config;

  @Inject
  public TimeseriesQueryQueryToolChest(QueryConfig config)
  {
    this.config = config;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeResults(QueryRunner<Result<TimeseriesResultValue>> queryRunner)
  {
    return new ResultMergeQueryRunner<Result<TimeseriesResultValue>>(queryRunner)
    {
      @Override
      protected Ordering<Result<TimeseriesResultValue>> makeOrdering(Query<Result<TimeseriesResultValue>> query)
      {
        return Ordering.from(
            new ResultGranularTimestampComparator<TimeseriesResultValue>(
                ((TimeseriesQuery) query).getGranularity()
            )
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
  public Sequence<Result<TimeseriesResultValue>> mergeSequences(Sequence<Sequence<Result<TimeseriesResultValue>>> seqOfSequences)
  {
    return new OrderedMergeSequence<Result<TimeseriesResultValue>>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TimeseriesQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource().toString())
        .setUser4("timeseries")
        .setUser5(COMMA_JOIN.join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser7(String.format("%,d aggs", query.getAggregatorSpecs().size()))
        .setUser9(Minutes.minutes(numMinutes).toString());
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

        return ByteBuffer
            .allocate(1 + granularityBytes.length + filterBytes.length + aggregatorBytes.length)
            .put(TIMESERIES_QUERY)
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
          public Object apply(@Nullable final Result<TimeseriesResultValue> input)
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

      @Override
      public Sequence<Result<TimeseriesResultValue>> mergeSequences(Sequence<Sequence<Result<TimeseriesResultValue>>> seqOfSequences)
      {
        return new MergeSequence<Result<TimeseriesResultValue>>(getOrdering(), seqOfSequences);
      }
    };
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> preMergeQueryDecoration(QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    return new IntervalChunkingQueryRunner<Result<TimeseriesResultValue>>(runner, config.getChunkPeriod());
  }

  public Ordering<Result<TimeseriesResultValue>> getOrdering()
  {
    return Ordering.natural();
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
        final Map<String, Object> values = Maps.newHashMap();
        final TimeseriesResultValue holder = result.getValue();
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
