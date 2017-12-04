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


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class ResultLevelCacheUtil
{
  public static Cache.NamedKey computeResultLevelCacheKey(
      String resultLevelCacheIdentifier
  )
  {
    return new Cache.NamedKey(
        resultLevelCacheIdentifier, StringUtils.toUtf8(resultLevelCacheIdentifier)
    );
  }

  public static void populate(
      Cache cache,
      ObjectMapper mapper,
      Cache.NamedKey key,
      Iterable<Object> results,
      int cacheLimit,
      String resultSetId
  )
  {
    try {
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      try (JsonGenerator gen = mapper.getFactory().createGenerator(bytes)) {
        // Save the resultSetId and its length
        bytes.write(ByteBuffer.allocate(Integer.BYTES).putInt(resultSetId.length()).array());
        bytes.write(StringUtils.toUtf8(resultSetId));
        for (Object result : results) {
          gen.writeObject(result);
        }
      }
      if (cacheLimit > 0 && bytes.size() > cacheLimit) {
        return;
      }
      cache.put(key, bytes.toByteArray());
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static <T> boolean useResultLevelCacheOnBrokers(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return useResultLevelCache(query, strategy, cacheConfig) && strategy.isCacheable(query, false);
  }

  public static <T> boolean populateResultLevelCacheOnBrokers(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return populateResultLevelCache(query, strategy, cacheConfig) && strategy.isCacheable(query, false);
  }

  private static <T> boolean useResultLevelCache(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return QueryContexts.isUseResultLevelCache(query)
           && strategy != null
           && cacheConfig.isUseResultLevelCache()
           && cacheConfig.isQueryCacheable(query);
  }

  private static <T> boolean populateResultLevelCache(
      Query<T> query,
      CacheStrategy<T, Object, Query<T>> strategy,
      CacheConfig cacheConfig
  )
  {
    return QueryContexts.isPopulateResultLevelCache(query)
           && strategy != null
           && cacheConfig.isPopulateResultLevelCache()
           && cacheConfig.isQueryCacheable(query);
  }
}
