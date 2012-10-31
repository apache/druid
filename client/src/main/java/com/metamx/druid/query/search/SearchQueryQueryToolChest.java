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

package com.metamx.druid.query.search;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.annotation.Nullable;

import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Minutes;
import org.joda.time.Period;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.druid.Query;
import com.metamx.druid.ResultGranularTimestampComparator;
import com.metamx.druid.SearchBinaryFn;
import com.metamx.druid.collect.OrderedMergeSequence;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.query.CacheStrategy;
import com.metamx.druid.query.IntervalChunkingQueryRunner;
import com.metamx.druid.query.MetricManipulationFn;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.ResultMergeQueryRunner;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.result.BySegmentSearchResultValue;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.SearchResultValue;
import com.metamx.druid.utils.PropUtils;
import com.metamx.emitter.service.ServiceMetricEvent;

/**
 */
public class SearchQueryQueryToolChest implements QueryToolChest<Result<SearchResultValue>, SearchQuery>
{
  private static final byte SEARCH_QUERY = 0x2;

  private static final Joiner COMMA_JOIN = Joiner.on(",");
  private static final TypeReference<Result<SearchResultValue>> TYPE_REFERENCE = new TypeReference<Result<SearchResultValue>>()
  {
  };

  private static final int maxSearchLimit;

  static {
    // I dislike this static loading of properies, but it's the only mechanism available right now.
    Properties props = Initialization.loadProperties();

    maxSearchLimit = PropUtils.getPropertyAsInt(props, "com.metamx.query.search.maxSearchLimit", 1000);
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> mergeResults(QueryRunner<Result<SearchResultValue>> runner)
  {
    return new ResultMergeQueryRunner<Result<SearchResultValue>>(runner)
    {
      @Override
      protected Ordering<Result<SearchResultValue>> makeOrdering(Query<Result<SearchResultValue>> query)
      {
        return Ordering.from(
            new ResultGranularTimestampComparator<SearchResultValue>(((SearchQuery) query).getGranularity())
        );
      }

      @Override
      protected BinaryFn<Result<SearchResultValue>, Result<SearchResultValue>, Result<SearchResultValue>> createMergeFn(
          Query<Result<SearchResultValue>> input
      )
      {
        SearchQuery query = (SearchQuery) input;
        return new SearchBinaryFn(query.getQuery().getSearchSortSpec(), query.getGranularity());
      }
    };
  }

  @Override
  public Sequence<Result<SearchResultValue>> mergeSequences(Sequence<Sequence<Result<SearchResultValue>>> seqOfSequences)
  {
    return new OrderedMergeSequence<Result<SearchResultValue>>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(SearchQuery query)
  {
    int numMinutes = 0;
    for (Interval interval : query.getIntervals()) {
      numMinutes += Minutes.minutesIn(interval).getMinutes();
    }

    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource())
        .setUser4("search")
        .setUser5(COMMA_JOIN.join(query.getIntervals()))
        .setUser6(String.valueOf(query.hasFilters()))
        .setUser9(Minutes.minutes(numMinutes).toString());
  }

  @Override
  public Function<Result<SearchResultValue>, Result<SearchResultValue>> makeMetricManipulatorFn(
      SearchQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<SearchResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<SearchResultValue>, SearchQuery> getCacheStrategy(SearchQuery query)
  {
    return new CacheStrategy<Result<SearchResultValue>, SearchQuery>()
    {
      @Override
      public byte[] computeCacheKey(SearchQuery query)
      {
        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] querySpecBytes = query.getQuery().getCacheKey();
        final byte[] granularityBytes = query.getGranularity().cacheKey();

        final Set<String> dimensions = Sets.newTreeSet();
        if (query.getDimensions() != null) {
          dimensions.addAll(query.getDimensions());
        }

        final byte[][] dimensionsBytes = new byte[dimensions.size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (String dimension : dimensions) {
          dimensionsBytes[index] = dimension.getBytes();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }

        final ByteBuffer queryCacheKey = ByteBuffer
            .allocate(1 + granularityBytes.length + filterBytes.length + querySpecBytes.length + dimensionsBytesSize)
            .put(SEARCH_QUERY)
            .put(granularityBytes)
            .put(filterBytes)
            .put(querySpecBytes);

        for (byte[] bytes : dimensionsBytes) {
          queryCacheKey.put(bytes);
        }

        return queryCacheKey.array();
      }

      @Override
      public Function<Result<SearchResultValue>, Object> prepareForCache()
      {
        return new Function<Result<SearchResultValue>, Object>()
        {
          @Override
          public Object apply(Result<SearchResultValue> input)
          {
            return Lists.newArrayList(input.getTimestamp().getMillis(), input.getValue());
          }
        };
      }

      @Override
      public Function<Object, Result<SearchResultValue>> pullFromCache()
      {
        return new Function<Object, Result<SearchResultValue>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<SearchResultValue> apply(Object input)
          {
            List<Object> result = (List<Object>) input;

            return new Result<SearchResultValue>(
                new DateTime(result.get(0)),
                new SearchResultValue(
                    Lists.transform(
                        (List) result.get(1),
                        new Function<Object, SearchHit>()
                        {
                          @Override
                          public SearchHit apply(@Nullable Object input)
                          {
                            if (input instanceof Map) {
                              return new SearchHit(
                                  (String) ((Map) input).get("dimension"),
                                  (String) ((Map) input).get("value")
                              );
                            } else if (input instanceof SearchHit) {
                              return (SearchHit) input;
                            } else {
                              throw new IAE("Unknown format [%s]", input.getClass());
                            }
                          }
                        }
                    )
                )
            );
          }
        };
      }

      @Override
      public Sequence<Result<SearchResultValue>> mergeSequences(Sequence<Sequence<Result<SearchResultValue>>> seqOfSequences)
      {
        return new MergeSequence<Result<SearchResultValue>>(getOrdering(), seqOfSequences);
      }
    };
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> preMergeQueryDecoration(QueryRunner<Result<SearchResultValue>> runner)
  {
    return new SearchThresholdAdjustingQueryRunner(
        new IntervalChunkingQueryRunner<Result<SearchResultValue>>(runner, Period.months(1))
    );
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> postMergeQueryDecoration(final QueryRunner<Result<SearchResultValue>> runner)
  {
    return runner;
  }

  private static class SearchThresholdAdjustingQueryRunner implements QueryRunner<Result<SearchResultValue>>
  {
    private final QueryRunner<Result<SearchResultValue>> runner;

    public SearchThresholdAdjustingQueryRunner(QueryRunner<Result<SearchResultValue>> runner) {this.runner = runner;}

    @Override
    public Sequence<Result<SearchResultValue>> run(Query<Result<SearchResultValue>> input)
    {
      if (!(input instanceof SearchQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", SearchQuery.class, input.getClass());
      }

      final SearchQuery query = (SearchQuery) input;
      if (query.getLimit() < maxSearchLimit) {
        return runner.run(query);
      }

      final boolean isBySegment = Boolean.parseBoolean(query.getContextValue("bySegment", "false"));

      return Sequences.map(
          runner.run(query.withLimit(maxSearchLimit)),
          new Function<Result<SearchResultValue>, Result<SearchResultValue>>()
          {
            @Override
            public Result<SearchResultValue> apply(Result<SearchResultValue> input)
            {
              if (isBySegment) {
                BySegmentSearchResultValue value = (BySegmentSearchResultValue) input.getValue();

                return new Result<SearchResultValue>(
                    input.getTimestamp(),
                    new BySegmentSearchResultValue(
                        Lists.transform(
                            value.getResults(),
                            new Function<Result<SearchResultValue>, Result<SearchResultValue>>()
                            {
                              @Override
                              public Result<SearchResultValue> apply(@Nullable Result<SearchResultValue> input)
                              {
                                return new Result<SearchResultValue>(
                                    input.getTimestamp(),
                                    new SearchResultValue(
                                        Lists.newArrayList(
                                            Iterables.limit(
                                                input.getValue(),
                                                query.getLimit()
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

              return new Result<SearchResultValue>(
                  input.getTimestamp(),
                  new SearchResultValue(
                      Lists.<SearchHit>newArrayList(
                          Iterables.limit(input.getValue(), query.getLimit())
                      )
                  )
              );
            }
          }
      );
    }
  }

  public Ordering<Result<SearchResultValue>> getOrdering()
  {
    return Ordering.natural();
  }
}
