/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.timeseries;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.druid.Query;
import com.metamx.druid.ResultGranularTimestampComparator;
import com.metamx.druid.TimeseriesBinaryFn;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.collect.OrderedMergeSequence;
import com.metamx.druid.query.CacheStrategy;
import com.metamx.druid.query.IntervalChunkingQueryRunner;
import com.metamx.druid.query.MetricManipulationFn;
import com.metamx.druid.query.QueryCacheHelper;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.ResultMergeQueryRunner;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeseriesResultValue;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class TimeseriesQueryQueryToolChest implements QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private static final byte TIMESERIES_QUERY = 0x0;

  private static final Joiner COMMA_JOIN = Joiner.on(",");
  private static final TypeReference<Result<TimeseriesResultValue>> TYPE_REFERENCE = new TypeReference<Result<TimeseriesResultValue>>()
  {
  };

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
            query.getAggregatorSpecs(),
            query.getPostAggregatorSpecs()
        );
      }
    };
  }

  @Override
  public Sequence<Result<TimeseriesResultValue>> mergeSequences(Sequence<Sequence<Result<TimeseriesResultValue>>> seqOfSequences)
  {
    return new OrderedMergeSequence<Result<TimeseriesResultValue>>(
        getOrdering(),
        seqOfSequences
    );
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TimeseriesQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource())
        .setUser4("timeseries")
        .setUser5(COMMA_JOIN.join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser7(String.format("%,d aggs", query.getAggregatorSpecs().size()))
        .setUser9(Minutes.minutes(numMinutes).toString());
  }

  @Override
  public Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>> makeMetricManipulatorFn(
      final TimeseriesQuery query, final MetricManipulationFn fn
  )
  {
    return new Function<Result<TimeseriesResultValue>, Result<TimeseriesResultValue>>()
    {
      @Override
      public Result<TimeseriesResultValue> apply(Result<TimeseriesResultValue> result)
      {
        final Map<String, Object> values = Maps.newHashMap();
        final TimeseriesResultValue holder = result.getValue();
        for (AggregatorFactory agg : query.getAggregatorSpecs()) {
          values.put(agg.getName(), fn.manipulate(agg, holder.getMetric(agg.getName())));
        }
        for (PostAggregator postAgg : query.getPostAggregatorSpecs()) {
          values.put(postAgg.getName(), holder.getMetric(postAgg.getName()));
        }
        return new Result<TimeseriesResultValue>(
            result.getTimestamp(),
            new TimeseriesResultValue(values)
        );
      }
    };
  }

  @Override
  public TypeReference<Result<TimeseriesResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TimeseriesResultValue>, TimeseriesQuery> getCacheStrategy(final TimeseriesQuery query)
  {
    return new CacheStrategy<Result<TimeseriesResultValue>, TimeseriesQuery>()
    {
      private final List<AggregatorFactory> aggs = query.getAggregatorSpecs();
      private final List<PostAggregator> postAggs = query.getPostAggregatorSpecs();

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
          @Override
          public Result<TimeseriesResultValue> apply(@Nullable Object input)
          {
            List<Object> results = (List<Object>) input;
            Map<String, Object> retVal = Maps.newLinkedHashMap();

            Iterator<AggregatorFactory> aggsIter = aggs.iterator();
            Iterator<Object> resultIter = results.iterator();

            DateTime timestamp = new DateTime(resultIter.next());
            while (aggsIter.hasNext() && resultIter.hasNext()) {
              final AggregatorFactory factory = aggsIter.next();
              retVal.put(factory.getName(), factory.deserialize(resultIter.next()));
            }

            for (PostAggregator postAgg : postAggs) {
              retVal.put(postAgg.getName(), postAgg.compute(retVal));
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
    return new IntervalChunkingQueryRunner<Result<TimeseriesResultValue>>(runner, Period.months(1));
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> postMergeQueryDecoration(QueryRunner<Result<TimeseriesResultValue>> runner)
  {
    return runner;
  }

  public Ordering<Result<TimeseriesResultValue>> getOrdering()
  {
    return Ordering.natural();
  }


}
