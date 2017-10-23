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

package io.druid.client;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.java.util.common.StringUtils;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryContexts;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class CacheUtil
{
  public static Cache.NamedKey computeSegmentCacheKey(
      String segmentIdentifier,
      SegmentDescriptor descriptor,
      byte[] queryCacheKey
  )
  {
    final Interval segmentQueryInterval = descriptor.getInterval();
    final byte[] versionBytes = StringUtils.toUtf8(descriptor.getVersion());

    return new Cache.NamedKey(
        segmentIdentifier, ByteBuffer
        .allocate(16 + versionBytes.length + 4 + queryCacheKey.length)
        .putLong(segmentQueryInterval.getStartMillis())
        .putLong(segmentQueryInterval.getEndMillis())
        .put(versionBytes)
        .putInt(descriptor.getPartitionNumber())
        .put(queryCacheKey).array()
    );
  }

  public static void populate(Cache cache, ObjectMapper mapper, Cache.NamedKey key, Iterable<Object> results)
  {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (JsonGenerator gen = mapper.getFactory().createGenerator(bytes)) {
        for (Object result : results) {
          gen.writeObject(result);
        }
      }

      cache.put(key, bytes.toByteArray());
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> boolean useCacheOnBrokers(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return useCache(query, strategy, cacheConfig) && strategy.isCacheable(query, false);
  }

  public static <T> boolean populateCacheOnBrokers(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return populateCache(query, strategy, cacheConfig) && strategy.isCacheable(query, false);
  }

  public static <T> boolean useCacheOnDataNodes(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return useCache(query, strategy, cacheConfig) && strategy.isCacheable(query, true);
  }

  public static <T> boolean populateCacheOnDataNodes(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return populateCache(query, strategy, cacheConfig) && strategy.isCacheable(query, true);
  }

  private static <T> boolean useCache(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return QueryContexts.isUseCache(query)
           && strategy != null
           && cacheConfig.isUseCache()
           && cacheConfig.isQueryCacheable(query);
  }

  private static <T> boolean populateCache(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return QueryContexts.isPopulateCache(query)
           && strategy != null
           && cacheConfig.isPopulateCache()
           && cacheConfig.isQueryCacheable(query);
  }

}
