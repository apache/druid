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

package org.apache.druid.server.lookup;


import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.server.lookup.cache.loading.LoadingCache;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Loading  lookup will load the key\value pair upon request on the key it self, the general algorithm is load key if absent.
 * Once the key/value  pair is loaded eviction will occur according to the cache eviction policy.
 * This module comes with two loading cache implementations, the first {@link org.apache.druid.server.lookup.cache.loading.OnHeapLoadingCache}is onheap backed by a Guava cache implementation, the second {@link org.apache.druid.server.lookup.cache.loading.OffHeapLoadingCache}is MapDB offheap implementation.
 * Both implementations offer various eviction strategies.
 * <p>
 * A key that the {@link DataFetcher} has no value for is not cached by default, so every lookup of it goes back to
 * the source. If a negative cache is configured, such keys are remembered in it, and looked up again only after
 * their entry is evicted. Failures of the {@link DataFetcher} are never remembered.
 */
public class LoadingLookup extends LookupExtractor
{
  private static final Logger LOGGER = new Logger(LoadingLookup.class);

  /**
   * The value stored for a key in the negative cache. Only the presence of the key is meaningful.
   */
  private static final String ABSENT_MARKER = "";

  private final DataFetcher<String, String> dataFetcher;
  private final LoadingCache<String, String> loadingCache;
  private final LoadingCache<String, List<String>> reverseLoadingCache;
  @Nullable
  private final LoadingCache<String, String> negativeLoadingCache;
  private final AtomicBoolean isOpen;
  private final String id = Integer.toHexString(System.identityHashCode(this));

  public LoadingLookup(
      DataFetcher dataFetcher,
      LoadingCache<String, String> loadingCache,
      LoadingCache<String, List<String>> reverseLoadingCache
  )
  {
    this(dataFetcher, loadingCache, reverseLoadingCache, null);
  }

  /**
   * @param negativeLoadingCache remembers keys that the fetcher has no value for, or null to look such keys up every
   *                             time. Only the presence of a key in this cache matters, not its value.
   */
  public LoadingLookup(
      DataFetcher dataFetcher,
      LoadingCache<String, String> loadingCache,
      LoadingCache<String, List<String>> reverseLoadingCache,
      @Nullable LoadingCache<String, String> negativeLoadingCache
  )
  {
    this.dataFetcher = Preconditions.checkNotNull(dataFetcher, "lookup must have a DataFetcher");
    this.loadingCache = Preconditions.checkNotNull(loadingCache, "loading lookup need a cache");
    this.reverseLoadingCache = Preconditions.checkNotNull(reverseLoadingCache, "loading lookup need reverse cache");
    this.negativeLoadingCache = negativeLoadingCache;
    this.isOpen = new AtomicBoolean(true);
  }


  @Override
  public String apply(@Nullable final String key)
  {
    if (key == null) {
      return null;
    }

    final String presentVal = this.loadingCache.getIfPresent(key);
    if (presentVal != null) {
      return presentVal;
    }

    if (negativeLoadingCache != null && negativeLoadingCache.getIfPresent(key) != null) {
      return null;
    }

    // If fetch throws, nothing is cached, so a failure is never mistaken for a missing key.
    final String val = this.dataFetcher.fetch(key);
    if (val == null) {
      if (negativeLoadingCache != null) {
        negativeLoadingCache.putAll(Collections.singletonMap(key, ABSENT_MARKER));
      }
      return null;
    }

    this.loadingCache.putAll(Collections.singletonMap(key, val));

    return val;
  }

  @Override
  public List<String> unapply(@Nullable final String value)
  {
    if (value == null) {
      return Collections.emptyList();
    }
    final List<String> retList;
    try {
      retList = reverseLoadingCache.get(value, new UnapplyCallable(value));
      return retList;
    }
    catch (ExecutionException e) {
      LOGGER.debug("list of keys not found for value [%s]", value);
      return Collections.emptyList();
    }
  }

  @Override
  public boolean supportsAsMap()
  {
    return true;
  }

  @Override
  public Map<String, String> asMap()
  {
    final Map<String, String> map = new HashMap<>();
    Optional.ofNullable(this.dataFetcher.fetchAll())
            .ifPresent(data -> data.forEach(entry -> map.put(entry.getKey(), entry.getValue())));
    return map;
  }

  @Override
  public byte[] getCacheKey()
  {
    return LookupExtractionModule.getRandomCacheKey();
  }

  public synchronized void close()
  {
    if (isOpen.getAndSet(false)) {
      LOGGER.info("Closing loading cache [%s]", id);
      loadingCache.close();
      reverseLoadingCache.close();
      if (negativeLoadingCache != null) {
        negativeLoadingCache.close();
      }
    } else {
      LOGGER.info("Closing already closed lookup");
    }
  }

  public boolean isOpen()
  {
    return isOpen.get();
  }

  private class UnapplyCallable implements Callable<List<String>>
  {
    private final String value;

    public UnapplyCallable(String value)
    {
      this.value = value;
    }

    @Override
    public List<String> call()
    {
      return dataFetcher.reverseFetchKeys(value);
    }
  }

  @Override
  public String toString()
  {
    return "LoadingLookup{" +
           "dataFetcher=" + dataFetcher +
           ", id='" + id + '\'' +
           '}';
  }
}
