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
import com.google.common.collect.Iterators;
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
import io.druid.server.QueryResource;


import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
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
      @Nullable
      final byte[] cachedResultSet = fetchResultsFromResultLevelCache(cacheKeyStr);
      String existingResultSetId = extractEtagFromResults(cachedResultSet);

      responseContext.put(CacheConfig.ENABLE_RESULTLEVEL_CACHE, true);

      if (existingResultSetId != null) {
        responseContext.put(QueryResource.EXISTING_RESULT_ID, existingResultSetId);
      }

      Sequence<T> resultFromClient = baseRunner.run(
          queryPlus,
          responseContext
      );
      String newResultSetId = (String) responseContext.get(QueryResource.HEADER_ETAG);

      @Nullable
      ResultLevelCachePopulator resultLevelCachePopulator = createResultLevelCachePopulator(
          cacheKeyStr,
          newResultSetId
      );

      responseContext.remove(QueryResource.EXISTING_RESULT_ID);
      if (newResultSetId != null && newResultSetId.equals(existingResultSetId)) {
        log.info("Return cached result set as there is no change in identifiers for query %s ", query.getId());
        return deserializeResults(cachedResultSet, strategy, existingResultSetId);
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
              // The resultset identifier and its length is cached along with the resultset
              resultLevelCachePopulator.populateResults(newResultSetId);
              log.info("Cache population complete for query %s", query.getId());
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
    final Function<T, Object> cacheFn = strategy.prepareForResultLevelCache();
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
      return cache.get(ResultLevelCacheUtil.computeResultLevelCacheKey(queryCacheKey));
    }
    return null;
  }

  private String extractEtagFromResults(
      final byte[] cachedResult
  )
  {
    if (cachedResult == null) {
      return null;
    }
    log.info("Fetching result level cache identifier for query: %s", query.getId());
    int etagLength = ByteBuffer.wrap(cachedResult, 0, Integer.BYTES).getInt();
    return StringUtils.fromUtf8(Arrays.copyOfRange(cachedResult, Integer.BYTES, etagLength + Integer.BYTES));
  }

  private Sequence<T> deserializeResults(
      final byte[] cachedResult, CacheStrategy strategy, String resultSetId
  )
  {
    if (cachedResult == null) {
      return null;
    }
    final Function<Object, T> pullFromCacheFunction = strategy.pullFromResultLevelCache();
    final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();
    //Skip the resultsetID and its length bytes
    byte[] prunedCacheData = Arrays.copyOfRange(
        cachedResult,
        Integer.BYTES + resultSetId.length(),
        cachedResult.length
    );
    Sequence<T> cachedSequence = Sequences.simple(() -> {
      try {
        if (cachedResult.length == 0) {
          return Iterators.emptyIterator();
        }

        return objectMapper.readValues(
            objectMapper.getFactory().createParser(prunedCacheData),
            cacheObjectClazz
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    });

    return Sequences.map(cachedSequence, pullFromCacheFunction);
  }

  private ResultLevelCachePopulator createResultLevelCachePopulator(
      String cacheKeyStr,
      String resultSetId
  )
  {
    // Results need to be cached only if all the segments are historical segments
    if (resultSetId != null && populateResultCache) {
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
              log.error(throwable, "Result-Level caching failed!");
            }
          },
          backgroundExecutorService
      );
    }
  }
}
