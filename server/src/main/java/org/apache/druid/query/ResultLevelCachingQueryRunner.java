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

package org.apache.druid.query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.QueryResource;

import javax.annotation.Nullable;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ResultLevelCachingQueryRunner<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(ResultLevelCachingQueryRunner.class);
  private final QueryRunner baseRunner;
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
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.baseRunner = baseRunner;
    this.objectMapper = objectMapper;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.query = query;
    this.strategy = queryToolChest.getCacheStrategy(query);
    this.populateResultCache = CacheUtil.isPopulateResultCache(
        query,
        strategy,
        cacheConfig,
        CacheUtil.ServerType.BROKER
    );
    this.useResultCache = CacheUtil.isUseResultCache(query, strategy, cacheConfig, CacheUtil.ServerType.BROKER);
  }

  @Override
  public Sequence<T> run(QueryPlus queryPlus, ResponseContext responseContext)
  {
    if (useResultCache || populateResultCache) {

      final String cacheKeyStr = StringUtils.fromUtf8(strategy.computeResultLevelCacheKey(query));
      final byte[] cachedResultSet = fetchResultsFromResultLevelCache(cacheKeyStr);
      String existingResultSetId = extractEtagFromResults(cachedResultSet);

      existingResultSetId = existingResultSetId == null ? "" : existingResultSetId;
      query = query.withOverriddenContext(
          ImmutableMap.of(QueryResource.HEADER_IF_NONE_MATCH, existingResultSetId));

      Sequence<T> resultFromClient = baseRunner.run(
          QueryPlus.wrap(query),
          responseContext
      );
      String newResultSetId = (String) responseContext.get(ResponseContext.Key.ETAG);

      if (useResultCache && newResultSetId != null && newResultSetId.equals(existingResultSetId)) {
        log.debug("Return cached result set as there is no change in identifiers for query %s ", query.getId());
        return deserializeResults(cachedResultSet, strategy, existingResultSetId);
      } else {
        @Nullable
        ResultLevelCachePopulator resultLevelCachePopulator = createResultLevelCachePopulator(
            cacheKeyStr,
            newResultSetId
        );
        if (resultLevelCachePopulator == null) {
          return resultFromClient;
        }
        final Function<T, Object> cacheFn = strategy.prepareForCache(true);

        return Sequences.wrap(
            Sequences.map(
                resultFromClient,
                new Function<T, T>()
                {
                  @Override
                  public T apply(T input)
                  {
                    if (resultLevelCachePopulator.isShouldPopulate()) {
                      resultLevelCachePopulator.cacheResultEntry(input, cacheFn);
                    }
                    return input;
                  }
                }
            ),
            new SequenceWrapper()
            {
              @Override
              public void after(boolean isDone, Throwable thrown)
              {
                Preconditions.checkNotNull(
                    resultLevelCachePopulator,
                    "ResultLevelCachePopulator cannot be null during cache population"
                );
                if (thrown != null) {
                  log.error(
                      thrown,
                      "Error while preparing for result level caching for query %s with error %s ",
                      query.getId(),
                      thrown.getMessage()
                  );
                } else if (resultLevelCachePopulator.isShouldPopulate()) {
                  // The resultset identifier and its length is cached along with the resultset
                  resultLevelCachePopulator.populateResults();
                  log.debug("Cache population complete for query %s", query.getId());
                }
                resultLevelCachePopulator.stopPopulating();
              }
            }
        );
      }
    } else {
      return baseRunner.run(
          queryPlus,
          responseContext
      );
    }
  }

  private byte[] fetchResultsFromResultLevelCache(
      final String queryCacheKey
  )
  {
    if (useResultCache && queryCacheKey != null) {
      return cache.get(CacheUtil.computeResultLevelCacheKey(queryCacheKey));
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

  private Sequence<T> deserializeResults(final byte[] cachedResult, CacheStrategy strategy, String resultSetId)
  {
    if (cachedResult == null) {
      log.error("Cached result set is null");
    }
    final Function<Object, T> pullFromCacheFunction = strategy.pullFromCache(true);
    final TypeReference<T> cacheObjectClazz = strategy.getCacheObjectClazz();
    //Skip the resultsetID and its length bytes
    Sequence<T> cachedSequence = Sequences.simple(() -> {
      try {
        int resultOffset = Integer.BYTES + resultSetId.length();
        return objectMapper.readValues(
            objectMapper.getFactory().createParser(
                cachedResult,
                resultOffset,
                cachedResult.length - resultOffset
            ),
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
    if (resultSetId != null && populateResultCache) {
      ResultLevelCachePopulator resultLevelCachePopulator = new ResultLevelCachePopulator(
          cache,
          objectMapper,
          CacheUtil.computeResultLevelCacheKey(cacheKeyStr),
          cacheConfig,
          true
      );
      try {
        //   Save the resultSetId and its length
        resultLevelCachePopulator.cacheObjectStream.write(ByteBuffer.allocate(Integer.BYTES)
                                                                    .putInt(resultSetId.length())
                                                                    .array());
        resultLevelCachePopulator.cacheObjectStream.write(StringUtils.toUtf8(resultSetId));
      }
      catch (IOException ioe) {
        log.error(ioe, "Failed to write cached values for query %s", query.getId());
        return null;
      }
      return resultLevelCachePopulator;
    } else {
      return null;
    }
  }

  private class ResultLevelCachePopulator
  {
    private final Cache cache;
    private final ObjectMapper mapper;
    private final Cache.NamedKey key;
    private final CacheConfig cacheConfig;
    @Nullable
    private ByteArrayOutputStream cacheObjectStream;

    private ResultLevelCachePopulator(
        Cache cache,
        ObjectMapper mapper,
        Cache.NamedKey key,
        CacheConfig cacheConfig,
        boolean shouldPopulate
    )
    {
      this.cache = cache;
      this.mapper = mapper;
      this.key = key;
      this.cacheConfig = cacheConfig;
      this.cacheObjectStream = shouldPopulate ? new ByteArrayOutputStream() : null;
    }

    boolean isShouldPopulate()
    {
      return cacheObjectStream != null;
    }

    void stopPopulating()
    {
      cacheObjectStream = null;
    }

    private void cacheResultEntry(
        T resultEntry,
        Function<T, Object> cacheFn
    )
    {
      Preconditions.checkNotNull(cacheObjectStream, "cacheObjectStream");
      int cacheLimit = cacheConfig.getResultLevelCacheLimit();
      try (JsonGenerator gen = mapper.getFactory().createGenerator(cacheObjectStream)) {
        gen.writeObject(cacheFn.apply(resultEntry));
        if (cacheLimit > 0 && cacheObjectStream.size() > cacheLimit) {
          stopPopulating();
        }
      }
      catch (IOException ex) {
        log.error(ex, "Failed to retrieve entry to be cached. Result Level caching will not be performed!");
        stopPopulating();
      }
    }

    public void populateResults()
    {
      CacheUtil.populateResultCache(
          cache,
          key,
          Preconditions.checkNotNull(cacheObjectStream, "cacheObjectStream").toByteArray()
      );
    }
  }
}
