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

package org.apache.druid.data.input.impl.prefetch;

import java.util.concurrent.TimeUnit;

/**
 * Holds the essential configuration required by {@link Fetcher} for prefetching purposes.
 */
public class PrefetchConfig
{
  public static final long DEFAULT_MAX_CACHE_CAPACITY_BYTES = 1024 * 1024 * 1024; // 1GB
  public static final long DEFAULT_MAX_FETCH_CAPACITY_BYTES = 1024 * 1024 * 1024; // 1GB
  public static final long DEFAULT_FETCH_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(60);

  // A roughly max size of total fetched objects, but the actual fetched size can be bigger. The reason is our current
  // client implementations for cloud storages like s3 don't support range scan yet, so we must download the whole file
  // at once. It's still possible for the size of cached/fetched data to not exceed these variables by estimating the
  // after-fetch size, but it makes us consider the case when any files cannot be fetched due to their large size, which
  // makes the implementation complicated.
  private final long maxFetchCapacityBytes;

  private final long maxCacheCapacityBytes;

  private final long prefetchTriggerBytes;

  // timeout for fetching an object from the remote site
  private final long fetchTimeout;


  public PrefetchConfig(
      Long maxCacheCapacityBytes,
      Long maxFetchCapacityBytes,
      Long prefetchTriggerBytes,
      Long fetchTimeout
  )
  {
    this.maxCacheCapacityBytes = maxCacheCapacityBytes == null
                                 ? DEFAULT_MAX_CACHE_CAPACITY_BYTES
                                 : maxCacheCapacityBytes;
    this.maxFetchCapacityBytes = maxFetchCapacityBytes == null
                                 ? DEFAULT_MAX_FETCH_CAPACITY_BYTES
                                 : maxFetchCapacityBytes;
    this.prefetchTriggerBytes = prefetchTriggerBytes == null
                                ? this.maxFetchCapacityBytes / 2
                                : prefetchTriggerBytes;
    this.fetchTimeout = fetchTimeout == null ? DEFAULT_FETCH_TIMEOUT_MS : fetchTimeout;
  }

  public long getMaxCacheCapacityBytes()
  {
    return maxCacheCapacityBytes;
  }

  public long getMaxFetchCapacityBytes()
  {
    return maxFetchCapacityBytes;
  }

  public long getPrefetchTriggerBytes()
  {
    return prefetchTriggerBytes;
  }

  public long getFetchTimeout()
  {
    return fetchTimeout;
  }

}
