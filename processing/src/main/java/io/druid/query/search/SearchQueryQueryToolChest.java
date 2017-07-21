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

package io.druid.query.search;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.query.CacheStrategy;
import io.druid.query.DefaultGenericQueryMetricsFactory;
import io.druid.query.GenericQueryMetricsFactory;
import io.druid.query.IntervalChunkingQueryRunnerDecorator;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.QueryMetrics;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.ResultGranularTimestampComparator;
import io.druid.query.ResultMergeQueryRunner;
import io.druid.query.aggregation.MetricManipulationFn;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.search.search.SearchQueryConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
public class SearchQueryQueryToolChest extends QueryToolChest<Result<SearchResultValue>, SearchQuery>
{
  private static final byte SEARCH_QUERY = 0x15;
  private static final TypeReference<Result<SearchResultValue>> TYPE_REFERENCE = new TypeReference<Result<SearchResultValue>>()
  {
  };
  private static final TypeReference<Object> OBJECT_TYPE_REFERENCE = new TypeReference<Object>()
  {
  };

  private final SearchQueryConfig config;
  private final IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator;
  private final GenericQueryMetricsFactory queryMetricsFactory;

  @VisibleForTesting
  public SearchQueryQueryToolChest(
      SearchQueryConfig config,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator
  )
  {
    this(config, intervalChunkingQueryRunnerDecorator, DefaultGenericQueryMetricsFactory.instance());
  }

  @Inject
  public SearchQueryQueryToolChest(
      SearchQueryConfig config,
      IntervalChunkingQueryRunnerDecorator intervalChunkingQueryRunnerDecorator,
      GenericQueryMetricsFactory queryMetricsFactory
  )
  {
    this.config = config;
    this.intervalChunkingQueryRunnerDecorator = intervalChunkingQueryRunnerDecorator;
    this.queryMetricsFactory = queryMetricsFactory;
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> mergeResults(
      QueryRunner<Result<SearchResultValue>> runner
  )
  {
    return new ResultMergeQueryRunner<Result<SearchResultValue>>(runner)
    {
      @Override
      protected Ordering<Result<SearchResultValue>> makeOrdering(Query<Result<SearchResultValue>> query)
      {
        return ResultGranularTimestampComparator.create(
            ((SearchQuery) query).getGranularity(),
            query.isDescending()
        );
      }

      @Override
      protected BinaryFn<Result<SearchResultValue>, Result<SearchResultValue>, Result<SearchResultValue>> createMergeFn(
          Query<Result<SearchResultValue>> input
      )
      {
        SearchQuery query = (SearchQuery) input;
        return new SearchBinaryFn(query.getSort(), query.getGranularity(), query.getLimit());
      }
    };
  }

  @Override
  public QueryMetrics<Query<?>> makeMetrics(SearchQuery query)
  {
    return queryMetricsFactory.makeMetrics(query);
  }

  @Override
  public Function<Result<SearchResultValue>, Result<SearchResultValue>> makePreComputeManipulatorFn(
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
  public CacheStrategy<Result<SearchResultValue>, Object, SearchQuery> getCacheStrategy(final SearchQuery query)
  {

    return new CacheStrategy<Result<SearchResultValue>, Object, SearchQuery>()
    {
      private final List<DimensionSpec> dimensionSpecs =
          query.getDimensions() != null ? query.getDimensions() : Collections.<DimensionSpec>emptyList();
      private final List<String> dimOutputNames = dimensionSpecs.size() > 0 ?
          Lists.transform(dimensionSpecs, DimensionSpec::getOutputName) : Collections.emptyList();

      @Override
      public boolean isCacheable(SearchQuery query, boolean willMergeRunners)
      {
        return true;
      }

      @Override
      public byte[] computeCacheKey(SearchQuery query)
      {
        final DimFilter dimFilter = query.getDimensionsFilter();
        final byte[] filterBytes = dimFilter == null ? new byte[]{} : dimFilter.getCacheKey();
        final byte[] querySpecBytes = query.getQuery().getCacheKey();
        final byte[] granularityBytes = query.getGranularity().getCacheKey();

        final List<DimensionSpec> dimensionSpecs =
            query.getDimensions() != null ? query.getDimensions() : Collections.<DimensionSpec>emptyList();
        final byte[][] dimensionsBytes = new byte[dimensionSpecs.size()][];
        int dimensionsBytesSize = 0;
        int index = 0;
        for (DimensionSpec dimensionSpec : dimensionSpecs) {
          dimensionsBytes[index] = dimensionSpec.getCacheKey();
          dimensionsBytesSize += dimensionsBytes[index].length;
          ++index;
        }

        final byte[] sortSpecBytes = query.getSort().getCacheKey();

        final ByteBuffer queryCacheKey = ByteBuffer
            .allocate(
                1 + 4 + granularityBytes.length + filterBytes.length +
                querySpecBytes.length + dimensionsBytesSize + sortSpecBytes.length
            )
            .put(SEARCH_QUERY)
            .put(Ints.toByteArray(query.getLimit()))
            .put(granularityBytes)
            .put(filterBytes)
            .put(querySpecBytes)
            .put(sortSpecBytes)
            ;

        for (byte[] bytes : dimensionsBytes) {
          queryCacheKey.put(bytes);
        }

        return queryCacheKey.array();
      }

      @Override
      public TypeReference<Object> getCacheObjectClazz()
      {
        return OBJECT_TYPE_REFERENCE;
      }

      @Override
      public Function<Result<SearchResultValue>, Object> prepareForCache()
      {
        return new Function<Result<SearchResultValue>, Object>()
        {
          @Override
          public Object apply(Result<SearchResultValue> input)
          {
            return dimensionSpecs.size() > 0
                ? Lists.newArrayList(input.getTimestamp().getMillis(), input.getValue(), dimOutputNames)
                : Lists.newArrayList(input.getTimestamp().getMillis(), input.getValue());
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
            boolean needsRename = false;
            final Map<String, String> outputNameMap = Maps.newHashMap();
            if (hasOutputName(result)) {
              List<String> cachedOutputNames = (List) result.get(2);
              Preconditions.checkArgument(cachedOutputNames.size() == dimOutputNames.size(),
                  "cache hit, but number of dimensions mismatch");
              needsRename = false;
              for (int idx = 0; idx < cachedOutputNames.size(); idx++) {
                String cachedOutputName = cachedOutputNames.get(idx);
                String outputName = dimOutputNames.get(idx);
                if (!cachedOutputName.equals(outputName)) {
                  needsRename = true;
                }
                outputNameMap.put(cachedOutputName, outputName);
              }
            }

            return !needsRename
                ? new Result<>(
                    new DateTime(((Number) result.get(0)).longValue()),
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
                                      (String) ((Map) input).get("value"),
                                      (Integer) ((Map) input).get("count")
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
                )
                : new Result<>(
                    new DateTime(((Number) result.get(0)).longValue()),
                    new SearchResultValue(
                        Lists.transform(
                            (List) result.get(1),
                            new Function<Object, SearchHit>()
                            {
                              @Override
                              public SearchHit apply(@Nullable Object input)
                              {
                                String dim = null;
                                String val = null;
                                Integer cnt = null;
                                if (input instanceof Map) {
                                  dim = outputNameMap.get((String)((Map) input).get("dimension"));
                                  val = (String) ((Map) input).get("value");
                                  cnt = (Integer) ((Map) input).get("count");
                                } else if (input instanceof SearchHit) {
                                  SearchHit cached = (SearchHit)input;
                                  dim = outputNameMap.get(cached.getDimension());
                                  val = cached.getValue();
                                  cnt = cached.getCount();
                                } else {
                                  throw new IAE("Unknown format [%s]", input.getClass());
                                }
                                return new SearchHit(dim, val, cnt);
                              }
                            }
                        )
                    )
                )
                ;
          }
        };
      }

      private boolean hasOutputName(List<Object> cachedEntry)
      {
        /*
         * cached entry is list of two or three objects
         *  1. timestamp
         *  2. SearchResultValue
         *  3. outputName of each dimension (optional)
         *
         * if a cached entry has three objects, dimension name of SearchResultValue should be check if rename is needed
         */
        return cachedEntry.size() == 3;
      }
    };
  }

  @Override
  public QueryRunner<Result<SearchResultValue>> preMergeQueryDecoration(final QueryRunner<Result<SearchResultValue>> runner)
  {
    return new SearchThresholdAdjustingQueryRunner(
        intervalChunkingQueryRunnerDecorator.decorate(
            new QueryRunner<Result<SearchResultValue>>()
            {
              @Override
              public Sequence<Result<SearchResultValue>> run(
                  QueryPlus<Result<SearchResultValue>> queryPlus, Map<String, Object> responseContext
              )
              {
                SearchQuery searchQuery = (SearchQuery) queryPlus.getQuery();
                if (searchQuery.getDimensionsFilter() != null) {
                  searchQuery = searchQuery.withDimFilter(searchQuery.getDimensionsFilter().optimize());
                  queryPlus = queryPlus.withQuery(searchQuery);
                }
                return runner.run(queryPlus, responseContext);
              }
            } , this),
        config
    );
  }

  private static class SearchThresholdAdjustingQueryRunner implements QueryRunner<Result<SearchResultValue>>
  {
    private final QueryRunner<Result<SearchResultValue>> runner;
    private final SearchQueryConfig config;

    public SearchThresholdAdjustingQueryRunner(
        QueryRunner<Result<SearchResultValue>> runner,
        SearchQueryConfig config
    )
    {
      this.runner = runner;
      this.config = config;
    }

    @Override
    public Sequence<Result<SearchResultValue>> run(
        QueryPlus<Result<SearchResultValue>> queryPlus,
        Map<String, Object> responseContext
    )
    {
      Query<Result<SearchResultValue>> input = queryPlus.getQuery();
      if (!(input instanceof SearchQuery)) {
        throw new ISE("Can only handle [%s], got [%s]", SearchQuery.class, input.getClass());
      }

      final SearchQuery query = (SearchQuery) input;
      if (query.getLimit() < config.getMaxSearchLimit()) {
        return runner.run(queryPlus, responseContext);
      }

      final boolean isBySegment = QueryContexts.isBySegment(query);

      return Sequences.map(
          runner.run(queryPlus.withQuery(query.withLimit(config.getMaxSearchLimit())), responseContext),
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
                        value.getInterval()
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
}
