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

package org.apache.druid.queryng.operator.general;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import org.apache.druid.client.CacheUtil;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.ResultLevelCachingQueryRunner;
import org.apache.druid.query.ResultLevelCachingQueryRunner.ResultLevelCachePopulator;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.Iterators.CountingResultIterator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.Iterator;

/**
 * Read from, or write to the result-level cache.
 * <p>
 * Retrieval is done only if the signature from this query (expressed as the
 * E-Tag) matches the cached results. The query is sent downstream even if
 * there is a preliminary cache so that execution can check that the cache
 * key has not, in fact changed.
 * <p>
 * Write to the cache is done if enabled, and if there is a cache miss.
 * This operator is omitted from the stack if no cache read or write is
 * required.
 *
 * @see ResultLevelCachingQueryRunner
 */
public class ResultLevelCacheOperator<T> implements Operator<T>
{
  private static final Logger log = new Logger(ResultLevelCacheOperator.class);

  private final FragmentContext context;
  private final Operator<T> input;
  private final String cacheKey;
  private final boolean writeCache;
  private final CacheStrategy<T, Object, Query<T>> strategy;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final ObjectMapper objectMapper;
  private final String existingResultSetId;
  private final byte[] cachedResultSet;
  private ResultIterator<T> inputIter;
  private String newResultSetId;
  private CountingResultIterator<T> cacheIter;
  private ResultLevelCachePopulator<T> cacheWriter;
  private int cacheWriteRowCount;
  private State state = State.START;

  public ResultLevelCacheOperator(
      final FragmentContext context,
      final Operator<T> input,
      final CacheStrategy<T, Object, Query<T>> strategy,
      final Cache cache,
      final CacheConfig cacheConfig,
      final ObjectMapper objectMapper,
      final String cacheKey,
      final String existingResultSetId,
      final byte[] cachedResultSet,
      final boolean writeCache
  )
  {
    this.context = context;
    this.input = input;
    this.strategy = strategy;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.objectMapper = objectMapper;
    this.cacheKey = cacheKey;
    this.existingResultSetId = existingResultSetId;
    this.cachedResultSet = cachedResultSet;
    this.writeCache = writeCache;
    context.register(this);
    context.registerChild(this, input);
  }

  @Override
  public ResultIterator<T> open()
  {
    state = State.RUN;

    // Open the input to compute the revised result set ID.
    inputIter = input.open();
    newResultSetId = context.responseContext().getEntityTag();

    // Retrieve from cache if we have results and the IDs match.
    if (newResultSetId != null && newResultSetId.equals(existingResultSetId)) {
      return prepareCacheRead();
    } else if (writeCache) {
      // Cache miss: populate the cache if requested
      return prepareCacheWrite();
    } else {
      // Cache miss and no write: just pass along the input and step out of the way.
      return inputIter;
    }
  }

  /**
   * Retrieve results from the cache, converting from generic objects to type T.
   */
  private ResultIterator<T> prepareCacheRead()
  {
    log.debug("Return cached result set as there is no change in identifiers for query %s ", context.queryId());

    // The input operator has done its job. Close it early.
    input.close(true);
    inputIter = null;

    // Create a result iterator that converts the generic Objects stored in
    // the cache to type T required by this operator.
    Iterator<Object> resultIter = ResultLevelCachingQueryRunner.deserializeResults(
        cachedResultSet,
        strategy,
        existingResultSetId,
        objectMapper,
        context.queryId()
    );
    final Function<Object, T> pullFromCacheFunction = strategy.pullFromCache(true);
    cacheIter = Iterators.map(resultIter, pullFromCacheFunction);
    return cacheIter;
  }

  /**
   * Create a "tee" iterator that retrieves the input rows and writes them to the
   * cache after conversion to a generic Object.
   */
  private ResultIterator<T> prepareCacheWrite()
  {
    // TODO: This form is not very efficient. It materializes the entire
    // result set in a byte buffer before writing to in-memory storage. Better to
    // stream results to an external storage.
    cacheWriter = new ResultLevelCachePopulator<T>(
        cache,
        objectMapper,
        CacheUtil.computeResultLevelCacheKey(cacheKey),
        cacheConfig,
        true
    );
    cacheWriter.writeHeader(newResultSetId);
    final Function<T, Object> cacheFn = strategy.prepareForCache(true);
    return new ResultIterator<T>()
    {
      @Override
      public T next() throws ResultIterator.EofException
      {
        T row = inputIter.next();
        cacheWriter.cacheResultEntry(row, cacheFn);
        cacheWriteRowCount++;
        return row;
      }
    };
  }

  @Override
  public void close(boolean cascade)
  {
    if (state != State.RUN) {
      state = State.CLOSED;
      return;
    }
    state = State.CLOSED;

    // Close the input if we still have one.
    if (inputIter != null && cascade) {
      input.close(cascade);
    }

    // Finalize the cache write, if we're writing to the cache. Don't finalize
    // if the query failed. Since the cache is memory-based, we let the GC clean
    // up any left-over cached values if there is a failure.
    if (cacheWriter != null && !context.failed()) {
      log.debug("Cache population complete for query %s", context.queryId());
      cacheWriter.populateResults();
    }

    // Profile this operator.
    OperatorProfile profile = new OperatorProfile("result-cache");
    if (cacheIter != null) {
      profile.add("action", "read");
      profile.add("cache-read-rows", cacheIter.rowCount());
    } else if (writeCache) {
      profile.add("action", "miss");
      profile.add("cache-write-rows", cacheWriteRowCount);
    } else {
      profile.add("action", "bypass");
      profile.omitFromProfile = true;
    }
    context.updateProfile(this, profile);

    // Reset the stateful knick-knacks.
    inputIter = null;
    cacheIter = null;
    cacheWriter = null;
  }
}
