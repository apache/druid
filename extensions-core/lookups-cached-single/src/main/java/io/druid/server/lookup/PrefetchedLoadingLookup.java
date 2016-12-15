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

import com.google.common.base.Strings;
import io.druid.server.lookup.cache.loading.LoadingCache;

import java.util.List;

/**
 * Variation of LoadingLookup
 * It supports prefetching of key-value pairs based on requested key
 */
public class PrefetchedLoadingLookup extends LoadingLookup
{
  private final PrefetchableFetcher<String, String> dataFetcher;

  public PrefetchedLoadingLookup(
      PrefetchableFetcher prefetchableFetcher,
      LoadingCache<String, String> loadingCache,
      LoadingCache<String, List<String>> reverseLoadingCache
  )
  {
    super(prefetchableFetcher, loadingCache, reverseLoadingCache);
    this.dataFetcher = prefetchableFetcher;
  }

  @Override
  public String apply(final String key)
  {
    if (key == null) {
      return null;
    }

    final String presentVal = loadingCache.getIfPresent(key);
    if (presentVal == null) {
      loadingCache.putAll(dataFetcher.prefetch(key));
      return super.apply(key);
    } else {
      return Strings.nullToEmpty(presentVal);
    }
  }
}
