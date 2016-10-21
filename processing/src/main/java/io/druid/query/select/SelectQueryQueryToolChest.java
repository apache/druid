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

package io.druid.query.select;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.CacheStrategy;
import io.druid.query.DruidMetrics;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.timeline.DataSegmentUtils;
import io.druid.timeline.LogicalSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 */
public class SelectQueryQueryToolChest extends QueryToolChest<Result<SelectResultValue>, SelectQuery>
{
  private static final byte SELECT_QUERY = 0x16;
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE =
      new TypeReference<Object>()
      {
      };
  private static final TypeReference<Result<SelectResultValue>> TYPE_REFERENCE =
      new TypeReference<Result<SelectResultValue>>()
      {
      };

  private final ObjectMapper jsonMapper;

  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;

  @Inject
  public SelectQueryQueryToolChest(ObjectMapper jsonMapper,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator)
  {
    this.jsonMapper = jsonMapper;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> mergeResults(
      QueryRunner<Result<SelectResultValue>> queryRunner
  )
  {
    return new ResultMergeQueryRunner<Result<SelectResultValue>>(queryRunner)
    {
      @Override
      protected Ordering<Result<SelectResultValue>> makeOrdering(Query<Result<SelectResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(
            ((SelectQuery) query).getGranularity(), query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<SelectResultValue>, Result<SelectResultValue>, Result<SelectResultValue>> createMergeFn(
          Query<Result<SelectResultValue>> input
      )
      {
        SelectQuery query = (SelectQuery) input;
        return new SelectBinaryFn(
            query.getGranularity(),
            query.getPagingSpec(),
            query.isDescending()
        );
      }
    };
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SelectQuery query)
  {
    return DruidMetrics.makePartialQueryTimeMetric(query);
  }

  @Override
  public Function<Result<SelectResultValue>, Result<SelectResultValue>> makePreComputeManipulatorFn(
      final SelectQuery query, final MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<SelectResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<SelectResultValue>, Object, SelectQuery> getCacheStrategy(final SelectQuery query)
  {

    return new CacheStrategy<Result<SelectResultValue>, Object, SelectQuery>()
    {
      private final List<DimensionSpec> dimensionSpecs =
          query.getDimensions() != null ? query.getDimensions() : Collections.<DimensionSpec>emptyList();
      private final List<String> dimOutputNames = dimensionSpecs.size() > 0 ?
          Lists.transform(
              dimensionSpecs,
              new Function<DimensionSpec, String>() {
                @Override
                public String apply(DimensionSpec input) {
                  return input.getOutputName();
                }
              }
          )
          :
          Collections.<String>emptyList();

      @Override
      public byte[] computeCacheKey(SelectQuery query)
      {
        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] granularityBytes = query.getGranularity().cacheKey();

        final List<DimensionSpec> dimensionSpecs =
            query.getDimensions() != null ? query.getDimensions() : Collections.<DimensionSpec>emptyList();
        final byte[][] dimensionsBytes = new byte[dimensionSpecs.size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimension : dimensionSpecs) {
          dimensionsBytes[index] = dimension.getCacheKey();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }

        final Set<String> metrics = Sets.newTreeSet();
        if (query.getMetrics() != null) {
          metrics.addAll(query.getMetrics());
        }

        final byte[][] metricBytes = new byte[metrics.size()][];
        int metricBytesSize = 0;
        index = 0;
        for (String metric : metrics) {
          metricBytes[index] = StringUtils.toUtf8(metric);
          metricBytesSize += metricBytes[index].length;
          ++index;
        }

        final ByteBuffer queryCacheKey = ByteBuffer
            .allocate(
                1
                + granularityBytes.length
                + filterBytes.length
                + query.getPagingSpec().getCacheKey().length
                + dimensionsBytesSize
                + metricBytesSize
            )
            .put(SELECT_QUERY)
            .put(granularityBytes)
            .put(filterBytes)
            .put(query.getPagingSpec().getCacheKey());

        for (byte[] dimensionsByte : dimensionsBytes) {
          queryCacheKey.put(dimensionsByte);
        }

        for (byte[] metricByte : metricBytes) {
          queryCacheKey.put(metricByte);
        }

        return queryCacheKey.array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<SelectResultValue>, Object> prepareForCache()
      {
        return new Function<Result<SelectResultValue>, Object>()
        {
          @Override
          public Object apply(final Result<SelectResultValue> input)
          {
            if (!dimOutputNames.isEmpty()) {
              return Arrays.asList(
                  input.getTimestamp().getMillis(),
                  input.getValue().getPagingIdentifiers(),
                  input.getValue().getDimensions(),
                  input.getValue().getMetrics(),
                  input.getValue().getEvents(),
                  dimOutputNames
              );
            }
            return Arrays.asList(
                input.getTimestamp().getMillis(),
                input.getValue().getPagingIdentifiers(),
                input.getValue().getDimensions(),
                input.getValue().getMetrics(),
                input.getValue().getEvents()
            );
          }
        };
      }

      @Override
      public Function<Object, Result<SelectResultValue>> pullFromCache()
      {
        return new Function<Object, Result<SelectResultValue>>()
        {
          private final QueryGranularity granularity = query.getGranularity();

          @Override
          public Result<SelectResultValue> apply(Object input)
          {
            List<Object> results = (List<Object>) input;
            Iterator<Object> resultIter = results.iterator();

            DateTime timestamp = granularity.toDateTime(((Number) resultIter.next()).longValue());

            Map<String, Integer> pageIdentifier = jsonMapper.convertValue(
                resultIter.next(), new TypeReference<Map<String, Integer>>() {}
                );
            Set<String> dimensionSet = jsonMapper.convertValue(
                resultIter.next(), new TypeReference<Set<String>>() {}
            );
            Set<String> metricSet = jsonMapper.convertValue(
                resultIter.next(), new TypeReference<Set<String>>() {}
            );
            List<EventHolder> eventHolders = jsonMapper.convertValue(
                resultIter.next(), new TypeReference<List<EventHolder>>() {}
                );
            // check the condition that outputName of cached result should be updated
            if (resultIter.hasNext()) {
              List<String> cachedOutputNames = (List<String>) resultIter.next();
              Preconditions.checkArgument(cachedOutputNames.size() == dimOutputNames.size(),
                  "Cache hit but different number of dimensions??");
              for (int idx = 0; idx < dimOutputNames.size(); idx++) {
                if (!cachedOutputNames.get(idx).equals(dimOutputNames.get(idx))) {
                  // rename outputName in the EventHolder
                  for (EventHolder eventHolder: eventHolders) {
                    Object obj = eventHolder.getEvent().remove(cachedOutputNames.get(idx));
                    if (obj != null) {
                      eventHolder.getEvent().put(dimOutputNames.get(idx), obj);
                    }
                  }
                }
              }
            }

            return new Result<>(
                timestamp,
                new SelectResultValue(
                    pageIdentifier,
                    dimensionSet,
                    metricSet,
                    eventHolders
                )
            );
          }
        };
      }
    };
  }

  @Override
  public QueryRunner<Result<SelectResultValue>> preMergeQueryDecoration(final QueryRunner<Result<SelectResultValue>> runner)
  {
    return intervalChunkingQueryRunnerDecorator.decorate(
        new QueryRunner<Result<SelectResultValue>>()
        {
          @Override
          public Sequence<Result<SelectResultValue>> run(
              Query<Result<SelectResultValue>> query, Map<String, Object> responseContext
          )
          {
            SelectQuery selectQuery = (SelectQuery) query;
            if (selectQuery.getDimensionsFilter() != null) {
              selectQuery = selectQuery.withDimFilter(selectQuery.getDimensionsFilter().optimize());
            }
            return runner.run(selectQuery, responseContext);
          }
        }, this);
  }

  @Override
  public <T extends LogicalSegment> List<T> filterSegments(SelectQuery query, List<T> segments)
  {
    // at the point where this code is called, only one datasource should exist.
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    PagingSpec pagingSpec = query.getPagingSpec();
    Map<String, Integer> paging = pagingSpec.getPagingIdentifiers();
    if (paging == null || paging.isEmpty()) {
      return segments;
    }

    final QueryGranularity granularity = query.getGranularity();

    List<Interval> intervals = Lists.newArrayList(
        Iterables.transform(paging.keySet(), DataSegmentUtils.INTERVAL_EXTRACTOR(dataSource))
    );
    Collections.sort(
        intervals, query.isDescending() ? Comparators.intervalsByEndThenStart()
                                        : Comparators.intervalsByStartThenEnd()
    );

    TreeMap<Long, Long> granularThresholds = Maps.newTreeMap();
    for (Interval interval : intervals) {
      if (query.isDescending()) {
        long granularEnd = granularity.truncate(interval.getEndMillis());
        Long currentEnd = granularThresholds.get(granularEnd);
        if (currentEnd == null || interval.getEndMillis() > currentEnd) {
          granularThresholds.put(granularEnd, interval.getEndMillis());
        }
      } else {
        long granularStart = granularity.truncate(interval.getStartMillis());
        Long currentStart = granularThresholds.get(granularStart);
        if (currentStart == null || interval.getStartMillis() < currentStart) {
          granularThresholds.put(granularStart, interval.getStartMillis());
        }
      }
    }

    List<T> queryIntervals = Lists.newArrayList(segments);

    Iterator<T> it = queryIntervals.iterator();
    if (query.isDescending()) {
      while (it.hasNext()) {
        Interval interval = it.next().getInterval();
        Map.Entry<Long, Long> ceiling = granularThresholds.ceilingEntry(granularity.truncate(interval.getEndMillis()));
        if (ceiling == null || interval.getStartMillis() >= ceiling.getValue()) {
          it.remove();
        }
      }
    } else {
      while (it.hasNext()) {
        Interval interval = it.next().getInterval();
        Map.Entry<Long, Long> floor = granularThresholds.floorEntry(granularity.truncate(interval.getStartMillis()));
        if (floor == null || interval.getEndMillis() <= floor.getValue()) {
          it.remove();
        }
      }
    }
    return queryIntervals;
  }
}
