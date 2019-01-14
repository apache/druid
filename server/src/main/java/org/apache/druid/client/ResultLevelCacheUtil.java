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

import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;

public class ResultLevelCacheUtil
{
  private static final Logger log = new Logger(ResultLevelCacheUtil.class);

  public static Cache.NamedKey computeResultLevelCacheKey(String resultLevelCacheIdentifier)
  {
    return new Cache.NamedKey(resultLevelCacheIdentifier, StringUtils.toUtf8(resultLevelCacheIdentifier));
  }

  public static void populate(
      Cache cache,
      Cache.NamedKey key,
      byte[] resultBytes
  )
  {
    log.debug("Populating results into cache");
    cache.put(key, resultBytes);
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
