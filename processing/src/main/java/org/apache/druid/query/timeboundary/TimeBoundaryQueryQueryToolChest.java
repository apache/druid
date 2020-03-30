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

package org.apache.druid.query.timeboundary;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.BySegmentSkippingQueryRunner;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.timeline.LogicalSegment;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

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

  private final GenericQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public TimeBoundaryQueryQueryToolChest()
  {
    this(DefaultGenericQueryMetricsFactory.instance());
  }

  @Inject
  public TimeBoundaryQueryQueryToolChest(GenericQueryMetricsFactory queryMetricsFactory)
  {
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(TimeBoundaryQuery query, List<T> segments)
  {
    if (segments.size() <= 1 || query.hasFilters()) {
      return segments;
    }

    final T min = query.isMaxTime() ? null : segments.get(0);
    final T max = query.isMinTime() ? null : segments.get(segments.size() - 1);

    return segments.stream()
                   .filter(input -> (min != null && input.getInterval().overlaps(min.getTrueInterval())) ||
                                    (max != null && input.getInterval().overlaps(max.getTrueInterval())))
                   .collect(Collectors.toList());
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
          QueryRunner<Result<TimeBoundaryResultValue>> baseRunner,
          QueryPlus<Result<TimeBoundaryResultValue>> input,
          ResponseContext context
      )
      {
        TimeBoundaryQuery query = (TimeBoundaryQuery) input.getQuery();
        return Sequences.simple(
            query.mergeResults(baseRunner.run(input, context).toList())
        );
      }
    };
  }

  @Override
  public BinaryOperator<Result<TimeBoundaryResultValue>> createMergeFn(Query<Result<TimeBoundaryResultValue>> query)
  {
    TimeBoundaryQuery boundQuery = (TimeBoundaryQuery) query;
    return (result1, result2) -> {
      final List<Result<TimeBoundaryResultValue>> mergeList;
      if (result1 == null) {
        mergeList = result2 != null ? ImmutableList.of(result2) : null;
      } else {
        mergeList = result2 != null ? ImmutableList.of(result1, result2) : ImmutableList.of(result1);
      }
      return Iterables.getOnlyElement(boundQuery.mergeResults(mergeList));
    };
  }

  @Override
  public Comparator<Result<TimeBoundaryResultValue>> createResultComparator(Query<Result<TimeBoundaryResultValue>> query)
  {
    return query.getResultOrdering();
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(TimeBoundaryQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public Function<Result<TimeBoundaryResultValue>, Result<TimeBoundaryResultValue>> makePreComputeManipulatorFn(
      TimeBoundaryQuery query,
      MetricManipulationFn fn
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
      public boolean isCacheable(TimeBoundaryQuery query, boolean willMergeRunners)
      {
        return true;
      }

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
      public byte[] computeResultLevelCacheKey(TimeBoundaryQuery query)
      {
        return computeCacheKey(query);
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<TimeBoundaryResultValue>, Object> prepareForCache(boolean isResultLevelCache)
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
      public Function<Object, Result<TimeBoundaryResultValue>> pullFromCache(boolean isResultLevelCache)
      {
        return new Function<Object, Result<TimeBoundaryResultValue>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<TimeBoundaryResultValue> apply(Object input)
          {
            List<Object> result = (List<Object>) input;

            return new Result<>(
                DateTimes.utc(((Number) result.get(0)).longValue()),
                new TimeBoundaryResultValue(result.get(1))
            );
          }
        };
      }
    };
  }
}
