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

package io.druid.server.lookup;


import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.LookupExtractor;
import io.druid.server.lookup.cache.loading.LoadingCache;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Loading  lookup will load the key\value pair upon request on the key it self, the general algorithm is load key if absent.
 * Once the key/value  pair is loaded eviction will occur according to the cache eviction policy.
 * This module comes with two loading cache implementations, the first {@link io.druid.server.lookup.cache.loading.OnHeapLoadingCache}is onheap backed by a Guava cache implementation, the second {@link io.druid.server.lookup.cache.loading.OffHeapLoadingCache}is MapDB offheap implementation.
 * Both implementations offer various eviction strategies.
 */
public class LoadingLookup extends LookupExtractor
{
  private static final Logger LOGGER = new Logger(LoadingLookup.class);

  private final DataFetcher<String, String> dataFetcher;
  private final LoadingCache<String, String> loadingCache;
  private final LoadingCache<String, List<String>> reverseLoadingCache;
  private final AtomicBoolean isOpen;
  private final String id = Integer.toHexString(System.identityHashCode(this));

  public LoadingLookup(
      DataFetcher dataFetcher,
      LoadingCache<String, String> loadingCache,
      LoadingCache<String, List<String>> reverseLoadingCache
  )
  {
    this.dataFetcher = Preconditions.checkNotNull(dataFetcher, "lookup must have a DataFetcher");
    this.loadingCache = Preconditions.checkNotNull(loadingCache, "loading lookup need a cache");
    this.reverseLoadingCache = Preconditions.checkNotNull(reverseLoadingCache, "loading lookup need reverse cache");
    this.isOpen = new AtomicBoolean(true);
  }


  @Override
  public String apply(final String key)
  {
    if (key == null) {
      return null;
    }
    final String presentVal;
    try {
      presentVal = loadingCache.get(key, new ApplyCallable(key));
      return Strings.emptyToNull(presentVal);
    }
    catch (ExecutionException e) {
      LOGGER.debug("value not found for key [%s]", key);
      return null;
    }
  }

  @Override
  public List<String> unapply(final String value)
  {
    // null value maps to empty list
    if (value == null) {
      return Collections.EMPTY_LIST;
    }
    final List<String> retList;
    try {
      retList = reverseLoadingCache.get(value, new UnapplyCallable(value));
      return retList;
    }
    catch (ExecutionException e) {
      LOGGER.debug("list of keys not found for value [%s]", value);
      return Collections.EMPTY_LIST;
    }
  }

  public synchronized void close()
  {
    if (isOpen.getAndSet(false)) {
      LOGGER.info("Closing loading cache [%s]", id);
      loadingCache.close();
      reverseLoadingCache.close();
    } else {
      LOGGER.info("Closing already closed lookup");
      return;
    }
  }

  public boolean isOpen()
  {
    return isOpen.get();
  }

  @Override
  public byte[] getCacheKey()
  {
    return LookupExtractionModule.getRandomCacheKey();
  }

  private class ApplyCallable implements Callable<String>
  {
    private final String key;

    public ApplyCallable(String key)
    {
      this.key = key;
    }

    @Override
    public String call() throws Exception
    {
      // avoid returning null and return an empty string to cache it.
      return Strings.nullToEmpty(dataFetcher.fetch(key));
    }
  }

  private class UnapplyCallable implements Callable<List<String>>
  {
    private final String value;

    public UnapplyCallable(String value)
    {
      this.value = value;
    }

    @Override
    public List<String> call() throws Exception
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
