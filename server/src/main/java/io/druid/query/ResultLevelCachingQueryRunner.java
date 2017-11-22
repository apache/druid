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

package io.druid.query;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.ResultLevelCacheUtil;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.SequenceWrapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;


import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

public class ResultLevelCachingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ResultLevelCachingQueryRunner.class);
  private final QueryRunner baseRunner;
  private final ListeningExecutorService backgroundExecutorService;
  private QueryToolChest queryToolChest;
  private ObjectMapper objectMapper;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final boolean useResultCache;
  private final boolean populateResultCache;
  private final Query<T> query;
  private final CacheStrategy<T, Object, Query<T>> strategy;


  public ResultLevelCachingQueryRunner(
      QueryRunner baseRunner,
      QueryToolChest queryToolChest,
      Query<T> query,
      ObjectMapper objectMapper,
      ExecutorService cachingExec,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.baseRunner = baseRunner;
    this.queryToolChest = queryToolChest;
    this.backgroundExecutorService = MoreExecutors.listeningDecorator(cachingExec);
    this.objectMapper = objectMapper;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.query = query;
    this.strategy = queryToolChest.getCacheStrategy(query);
    this.populateResultCache = ResultLevelCacheUtil.populateResultLevelCacheOnBrokers(query, strategy, cacheConfig);
    this.useResultCache = ResultLevelCacheUtil.useResultLevelCacheOnBrokers(query, strategy, cacheConfig);
  }

  @Override
  public Sequence<T> run(QueryPlus queryPlus, Map responseContext)
  {
    if (useResultCache) {

      final String cacheKeyStr = StringUtils.fromUtf8(strategy.computeCacheKey(query));

      Map<String, Iterable<Object>> resultMap;
      @Nullable
      final byte[] cachedResultSet = fetchResultsFromResultLevelCache(cacheKeyStr);

      resultMap = extractMapFromResults(cachedResultSet);
      responseContext.put("resultLevelCacheEnabled", true);

      if (!resultMap.isEmpty()) {
        String resultSetIdentifier = resultMap.entrySet().iterator().next().getKey();
        responseContext.put("prevResultSetIdentifier", resultSetIdentifier);
      }

      @Nullable
      ResultLevelCachePopulator resultLevelCachePopulator = createResultLevelCachePopulator(
          cachedResultSet,
          cacheKeyStr
      );
      Sequence<T> resultFromClient = baseRunner.run(
          queryPlus,
          responseContext
      );

      String currentResultSetIdentifier = (String) responseContext.get("currentResultSetIdentifier");
      String prevResultSetIdentifier = (String) responseContext.get("prevResultSetIdentifier");
      responseContext.remove("currentResultSetIdentifier");
      responseContext.remove("prevResultSetIdentifier");
      if (currentResultSetIdentifier != null && currentResultSetIdentifier.equals(prevResultSetIdentifier)) {
        log.info("Return cached result set as there is no change in identifiers for query %s ", query.getId());
        Iterable<Object> cachedResults = resultMap.entrySet().iterator().next().getValue();
        return fetchSequenceFromResults(cachedResults, strategy);
      } else {
        return Sequences.wrap(Sequences.map(
            resultFromClient,
            new Function<T, T>()
            {
              @Override
              public T apply(T input)
              {
                cacheResultEntry(resultLevelCachePopulator, input);
                return input;
              }
            }
        ), new SequenceWrapper()
        {
          @Override
          public void after(boolean isDone, Throwable thrown) throws Exception
          {
            if (resultLevelCachePopulator != null) {
              // The resultset identifier is cached along with the resultset
              resultLevelCachePopulator.populateResults(currentResultSetIdentifier);
            }
          }
        });
      }
    } else {
      return baseRunner.run(
          queryPlus,
          responseContext
      );
    }
  }

  private T cacheResultEntry(
      ResultLevelCachePopulator resultLevelCachePopulator,
      T resultEntry
  )
  {
    final Function<T, Object> cacheFn = strategy.prepareForCache();
    if (resultLevelCachePopulator != null) {
      resultLevelCachePopulator.cacheFutures
          .add(backgroundExecutorService.submit(() -> cacheFn.apply(resultEntry)));
    }
    return resultEntry;
  }

  private byte[] fetchResultsFromResultLevelCache(
      final String queryCacheKey
  )
  {
    if (useResultCache && queryCacheKey != null) {
      log.info("Fetching cached result for query: %s", query.getId());
      return cache.get(ResultLevelCacheUtil.computeResultLevelCacheKey(queryCacheKey));
    }
    return null;
  }

  private Map<String, Iterable<Object>> extractMapFromResults(
      final byte[] cachedResult
  )
  {
    if (cachedResult == null) {
      return Collections.emptyMap();
    }
    final TypeReference<Map<String, Iterable<Object>>> cacheObjectClazz = new TypeReference<Map<String, Iterable<Object>>>()
    {
    };
    try {
      Map<String, Iterable<Object>> res = objectMapper.readValue(
          cachedResult, cacheObjectClazz
      );
      return res;
    }
    catch (IOException ioe) {
      log.error("Error parsing cached result set.");
      return Collections.emptyMap();
    }
  }

  private Sequence<T> fetchSequenceFromResults(
      Iterable<Object> cachedResult, CacheStrategy strategy
  )
  {
    if (strategy == null) {
      return null;
    }
    final Function<Object, T> pullFromCacheFunction = strategy.pullFromCache();
    Sequence<Object> cachedSequence = Sequences.simple(() -> cachedResult.iterator());
    return Sequences.map(cachedSequence, pullFromCacheFunction);
  }

  private ResultLevelCachePopulator createResultLevelCachePopulator(byte[] cachedResultSet, String cacheKeyStr)
  {
    if (cachedResultSet == null && populateResultCache) {
      return new ResultLevelCachePopulator(
          cache,
          objectMapper,
          ResultLevelCacheUtil.computeResultLevelCacheKey(cacheKeyStr),
          cacheConfig,
          backgroundExecutorService
      );
    } else {
      return null;
    }
  }

  public class ResultLevelCachePopulator
  {
    private final Cache cache;
    private final ObjectMapper mapper;
    private final Cache.NamedKey key;
    private final ConcurrentLinkedQueue<ListenableFuture<Object>> cacheFutures = new ConcurrentLinkedQueue<>();
    private final CacheConfig cacheConfig;
    private final ListeningExecutorService backgroundExecutorService;

    private ResultLevelCachePopulator(
        Cache cache,
        ObjectMapper mapper,
        Cache.NamedKey key,
        CacheConfig cacheConfig,
        ListeningExecutorService backgroundExecutorService
    )
    {
      this.cache = cache;
      this.mapper = mapper;
      this.key = key;
      this.cacheConfig = cacheConfig;
      this.backgroundExecutorService = backgroundExecutorService;
    }

    public void populateResults(
        String resultSetIdentifier
    )
    {
      Futures.addCallback(
          Futures.allAsList(cacheFutures),
          new FutureCallback<List<Object>>()
          {
            @Override
            public void onSuccess(List<Object> cacheData)
            {
              ResultLevelCacheUtil.populate(
                  cache,
                  mapper,
                  key,
                  cacheData,
                  cacheConfig.getResultLevelCacheLimit(),
                  resultSetIdentifier
              );
              // Help out GC by making sure all references are gone
              cacheFutures.clear();
            }

            @Override
            public void onFailure(Throwable throwable)
            {
              log.error(throwable, "Result-Level caching failed");
            }
          },
          backgroundExecutorService
      );
    }
  }
}
