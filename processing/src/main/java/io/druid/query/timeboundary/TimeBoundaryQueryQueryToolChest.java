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

package io.druid.query.timeboundary;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.BySegmentSkippingQueryRunner;
import io.druid.query.CacheStrategy;
import io.druid.query.DataSourceUtil;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 */
public class TimeBoundaryQueryQueryToolChest
    extends QueryToolChest<Result<TimeBoundaryResultValue>, TimeBoundaryQuery>
{
  private static final byte TIMEBOUNDARY_QUERY = 0x3;

  private static final TypeReference<Result<TimeBoundaryResultValue>> TYPE_REFERENCE = new TypeReference<Result<TimeBoundaryResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(TimeBoundaryQuery query, List<T> segments)
  {
    if (segments.size() <= 1 || query.hasFilters()) {
      return segments;
    }

    final T min = query.isMaxTime() ? null : segments.get(0);
    final T max = query.isMinTime() ? null : segments.get(segments.size() - 1);

    return Lists.newArrayList(
        Iterables.filter(
            segments,
            new Predicate<T>()
            {
              @Override
              public boolean apply(T input)
              {
                return (min != null && input.getInterval().overlaps(min.getInterval())) ||
                       (max != null && input.getInterval().overlaps(max.getInterval()));
              }
            }
        )
    );
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> mergeResults(
      final QueryRunner<Result<TimeBoundaryResultValue>> runner
  )
  {
    return new BySegmentSkippingQueryRunner<Result<TimeBoundaryResultValue>>(runner)
    {
      @Override
      protected Sequence<Result<TimeBoundaryResultValue>> doRun(
          QueryRunner<Result<TimeBoundaryResultValue>> baseRunner, Query<Result<TimeBoundaryResultValue>> input, Map<String, Object> context
      )
      {
        TimeBoundaryQuery query = (TimeBoundaryQuery) input;
        return Sequences.simple(
            query.mergeResults(
                Sequences.toList(baseRunner.run(query, context), Lists.<Result<TimeBoundaryResultValue>>newArrayList())
            )
        );
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TimeBoundaryQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query)
            .setDimension(DruidMetrics.DATASOURCE, DataSourceUtil.getMetricName(query.getDataSource()))
            .setDimension(DruidMetrics.TYPE, query.getType());
  }

  @Override
  public Function<Result<TimeBoundaryResultValue>, Result<TimeBoundaryResultValue>> makePreComputeManipulatorFn(
      TimeBoundaryQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<TimeBoundaryResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TimeBoundaryResultValue>, Object, TimeBoundaryQuery> getCacheStrategy(final TimeBoundaryQuery query)
  {
    return new CacheStrategy<Result<TimeBoundaryResultValue>, Object, TimeBoundaryQuery>()
    {
      @Override
      public byte[] computeCacheKey(TimeBoundaryQuery query)
      {
        final byte[] cacheKey = query.getCacheKey();
        return ByteBuffer.allocate(1 + cacheKey.length)
                         .put(TIMEBOUNDARY_QUERY)
                         .put(cacheKey)
                         .array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TimeBoundaryResultValue>, Object> prepareForCache()
      {
        return new Function<Result<TimeBoundaryResultValue>, Object>()
        {
          @Override
          public Object apply(Result<TimeBoundaryResultValue> input)
          {
            return Lists.newArrayList(input.getTimestamp().getMillis(), input.getValue());
          }
        };
      }

      @Override
      public Function<Object, Result<TimeBoundaryResultValue>> pullFromCache()
      {
        return new Function<Object, Result<TimeBoundaryResultValue>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<TimeBoundaryResultValue> apply(Object input)
          {
            List<Object> result = (List<Object>) input;

            return new Result<>(
                new DateTime(((Number)result.get(0)).longValue()),
                new TimeBoundaryResultValue(result.get(1))
            );
          }
        };
      }
    };
  }
}
