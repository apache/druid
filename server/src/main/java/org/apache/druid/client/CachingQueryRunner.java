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

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.primitives.Bytes;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

public class CachingQueryRunner<T> implements QueryRunner<T>
{
  private final String cacheId;
  private final SegmentDescriptor segmentDescriptor;
  private final Interval actualDataInterval;
  private final Optional<byte[]> cacheKeyPrefix;
  private final QueryRunner<T> base;
  private final QueryToolChest toolChest;
  private final Cache cache;
  private final ObjectMapper mapper;
  private final CachePopulator cachePopulator;
  private final CacheConfig cacheConfig;

  public CachingQueryRunner(
      String cacheId,
      Optional<byte[]> cacheKeyPrefix,
      SegmentDescriptor segmentDescriptor,
      Interval actualDataInterval,
      ObjectMapper mapper,
      Cache cache,
      QueryToolChest toolchest,
      QueryRunner<T> base,
      CachePopulator cachePopulator,
      CacheConfig cacheConfig
  )
  {
    this.cacheKeyPrefix = cacheKeyPrefix;
    this.base = base;
    this.cacheId = cacheId;
    this.segmentDescriptor = segmentDescriptor;
    this.actualDataInterval = actualDataInterval;
    this.toolChest = toolchest;
    this.cache = cache;
    this.mapper = mapper;
    this.cachePopulator = cachePopulator;
    this.cacheConfig = cacheConfig;
  }

  @Override
  public Sequence<T> run(QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    Query<T> query = queryPlus.getQuery();
    final CacheStrategy strategy = toolChest.getCacheStrategy(query);
    final boolean populateCache = canPopulateCache(query, strategy);
    final boolean useCache = canUseCache(query, strategy);

    final Cache.NamedKey key;
    if (useCache || populateCache) {
      key = CacheUtil.computeSegmentCacheKey(
          cacheId,
          alignToActualDataInterval(segmentDescriptor),
          Bytes.concat(cacheKeyPrefix.get(), strategy.computeCacheKey(query))
      );
    } else {
      key = null;
    }

    if (useCache) {
      final Function cacheFn = strategy.pullFromSegmentLevelCache();
      final byte[] cachedResult = cache.get(key);
      if (cachedResult != null) {
        final TypeReference cacheObjectClazz = strategy.getCacheObjectClazz();

        return Sequences.map(
            new BaseSequence<>(
                new BaseSequence.IteratorMaker<T, Iterator<T>>()
                {
                  @Override
                  public Iterator<T> make()
                  {
                    try {
                      if (cachedResult.length == 0) {
                        return Collections.emptyIterator();
                      }

                      return mapper.readValues(
                          mapper.getFactory().createParser(cachedResult),
                          cacheObjectClazz
                      );
                    }
                    catch (IOException e) {
                      throw new RuntimeException(e);
                    }
                  }

                  @Override
                  public void cleanup(Iterator<T> iterFromMake)
                  {
                  }
                }
            ),
            cacheFn
        );
      }
    }

    if (populateCache) {
      final Function cacheFn = strategy.prepareForSegmentLevelCache();
      return cachePopulator.wrap(base.run(queryPlus, responseContext), value -> cacheFn.apply(value), cache, key);
    } else {
      return base.run(queryPlus, responseContext);
    }
  }

  /**
   * @return whether the segment level cache should be used or not. False if strategy is null
   */
  @VisibleForTesting
  boolean canUseCache(Query<T> query, CacheStrategy strategy)
  {
    return CacheUtil.isUseSegmentCache(
        query,
        strategy,
        cacheConfig,
        CacheUtil.ServerType.DATA
    ) && cacheKeyPrefix.isPresent();
  }

  /**
   * @return whether the segment level cache should be populated or not. False if strategy is null
   */
  @VisibleForTesting
  boolean canPopulateCache(Query<T> query, CacheStrategy strategy)
  {
    return CacheUtil.isPopulateSegmentCache(
        query,
        strategy,
        cacheConfig,
        CacheUtil.ServerType.DATA
    ) && cacheKeyPrefix.isPresent();
  }

  private SegmentDescriptor alignToActualDataInterval(SegmentDescriptor in)
  {
    Interval interval = in.getInterval();
    return new SegmentDescriptor(
        interval.overlaps(actualDataInterval) ? interval.overlap(actualDataInterval) : interval,
        in.getVersion(),
        in.getPartitionNumber()
    );
  }

}
