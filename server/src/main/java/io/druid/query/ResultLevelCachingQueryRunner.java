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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.ResultLevelCacheUtil;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.SequenceWrapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.QueryResource;


import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

public class ResultLevelCachingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ResultLevelCachingQueryRunner.class);
  private final QueryRunner baseRunner;
  private final ListeningExecutorService cachingExec;
  private ObjectMapper objectMapper;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final boolean useResultCache;
  private final boolean populateResultCache;
  private Query<T> query;
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
    this.cachingExec = MoreExecutors.listeningDecorator(cachingExec);
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
      final byte[] cachedResultSet = fetchResultsFromResultLevelCache(cacheKeyStr);
      String existingResultSetId = extractEtagFromResults(cachedResultSet);
      existingResultSetId = existingResultSetId == null ? "" : existingResultSetId;

      if (query.getContext().get(QueryResource.HEADER_IF_NONE_MATCH) == null) {
        query = query.withOverriddenContext(
            ImmutableMap.of(QueryResource.HEADER_IF_NONE_MATCH, existingResultSetId));
        query = query.withOverriddenContext(ImmutableMap.of("ENABLE_RESULT_CACHE", true));

      }
      Sequence<T> resultFromClient = baseRunner.run(
          QueryPlus.wrap(query),
          responseContext
      );
      String newResultSetId = (String) responseContext.get(QueryResource.HEADER_ETAG);

      if (newResultSetId != null && newResultSetId.equals(existingResultSetId)) {
        log.debug("Return cached result set as there is no change in identifiers for query %s ", query.getId());
        return deserializeResults(cachedResultSet, strategy, existingResultSetId);
      } else {
        @Nullable
        ResultLevelCachePopulator resultLevelCachePopulator = createResultLevelCachePopulator(
            cacheKeyStr,
            newResultSetId
        );
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
              log.debug("Cache population complete for query %s", query.getId());
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
    final Function<T, Object> cacheFn = strategy.prepareForCache(true);
    if (resultLevelCachePopulator != null) {
      resultLevelCachePopulator.cacheFutures
          .add(cachingExec.submit(() -> cacheFn.apply(resultEntry)));
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
    log.debug("Fetching result level cache identifier for query: %s", query.getId());
    int etagLength = ByteBuffer.wrap(cachedResult, 0, Integer.BYTES).getInt();
    return StringUtils.fromUtf8(Arrays.copyOfRange(cachedResult, Integer.BYTES, etagLength + Integer.BYTES));
  }

  private Sequence<T> deserializeResults(
      final byte[] cachedResult, CacheStrategy strategy, String resultSetId
  )
  {
    if (cachedResult == null) {
      log.error("Cached result set is null");
    }
    final Function<Object, T> pullFromCacheFunction = strategy.pullFromCache(true);
    final TypeReference<Object> cacheObjectClazz = strategy.getCacheObjectClazz();
    //Skip the resultsetID and its length bytes
    Sequence<T> cachedSequence = Sequences.simple(() -> {
      try {
        if (cachedResult.length == 0) {
          return Iterators.emptyIterator();
        }

        return objectMapper.readValues(
            objectMapper.getFactory().createParser(Arrays.copyOfRange(
                cachedResult,
                Integer.BYTES + resultSetId.length(),
                cachedResult.length
            )),
            cacheObjectClazz
        );
      }
      catch (IOException e) {
        throw new RE(e, "Failed to retrieve results from cache for query ID [%s]", query.getId());
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
          cachingExec
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

    public void populateResults(String resultSetIdentifier)
    {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try {
        // Save the resultSetId and its length
        bytes.write(ByteBuffer.allocate(Integer.BYTES).putInt(resultSetIdentifier.length()).array());
        bytes.write(StringUtils.toUtf8(resultSetIdentifier));
        byte[] resultBytes = fetchResultBytes(bytes, cacheConfig.getCacheBulkMergeLimit());
        if (resultBytes != null) {
          ResultLevelCacheUtil.populate(
              cache,
              key,
              resultBytes
          );
        }
        // Help out GC by making sure all references are gone
        cacheFutures.clear();
      }
      catch (IOException ioe) {
        log.error("Failed to write cached values for query %s", query.getId());
      }
    }

    private byte[] fetchResultBytes(ByteArrayOutputStream resultStream, int cacheLimit)
    {
      for (ListenableFuture lsf : cacheFutures) {
        try (JsonGenerator gen = mapper.getFactory().createGenerator(resultStream)) {
          gen.writeObject(lsf.get());
          if (cacheLimit > 0 && resultStream.size() > cacheLimit) {
            return null;
          }
        }
        catch (ExecutionException | InterruptedException | IOException ex) {
          log.error("Failed to retrieve entry to be cached. Result Level caching will not be performed!");
          return null;
        }
      }
      return resultStream.toByteArray();
    }
  }
}
