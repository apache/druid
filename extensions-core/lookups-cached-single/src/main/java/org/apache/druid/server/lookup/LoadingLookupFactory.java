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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupExtractorFactory;
import org.apache.druid.query.lookup.LookupIntrospectHandler;
import org.apache.druid.server.lookup.cache.loading.LoadingCache;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

@JsonTypeName("loadingLookup")
public class LoadingLookupFactory implements LookupExtractorFactory
{
  private static final Logger LOGGER = new Logger(LoadingLookupFactory.class);

  @JsonProperty("dataFetcher")
  private final DataFetcher<String, String> dataFetcher;

  @JsonProperty("loadingCacheSpec")
  private final LoadingCache<String, String> loadingCache;

  @JsonProperty("reverseLoadingCacheSpec")
  private final LoadingCache<String, List<String>> reverseLoadingCache;

  private final String id = Integer.toHexString(System.identityHashCode(this));
  private final LoadingLookup loadingLookup;
  private final AtomicBoolean started = new AtomicBoolean(false);

  public LoadingLookupFactory(
      @JsonProperty("dataFetcher") DataFetcher dataFetcher,
      @JsonProperty("loadingCacheSpec") LoadingCache<String, String> loadingCache,
      @JsonProperty("reverseLoadingCacheSpec") LoadingCache<String, List<String>> reverseLoadingCache
  )
  {
    this(dataFetcher, loadingCache, reverseLoadingCache, new LoadingLookup(dataFetcher, loadingCache, reverseLoadingCache));
  }

  protected LoadingLookupFactory(
      DataFetcher dataFetcher,
      LoadingCache<String, String> loadingCache,
      LoadingCache<String, List<String>> reverseLoadingCache,
      LoadingLookup loadingLookup
  )
  {
    this.dataFetcher = Preconditions.checkNotNull(dataFetcher);
    this.loadingCache = Preconditions.checkNotNull(loadingCache);
    this.reverseLoadingCache = Preconditions.checkNotNull(reverseLoadingCache);
    this.loadingLookup = loadingLookup;
  }

  @Override
  public synchronized boolean start()
  {
    if (!started.get()) {
      started.set(loadingLookup.isOpen());
      LOGGER.info("created loading lookup with id [%s]", id);
    }
    return started.get();
  }

  @Override
  public synchronized boolean close()
  {
    if (started.getAndSet(false)) {
      LOGGER.info("closing loading lookup [%s]", id);
      loadingLookup.close();
    }
    return !started.get();
  }

  @Override
  public boolean replaces(
      @Nullable LookupExtractorFactory lookupExtractorFactory
  )
  {
    if (lookupExtractorFactory == null) {
      return true;
    }
    return !this.equals(lookupExtractorFactory);
  }

  @Nullable
  @Override
  public LookupIntrospectHandler getIntrospectHandler()
  {
    //not supported yet
    return null;
  }

  @Override
  public void awaitInitialization()
  {
    // LoadingLookupFactory does not have any initialization period as it fetches the key from loadingCache and DataFetcher as necessary.
  }

  @Override
  public boolean isInitialized()
  {
    return true;
  }
  @Override
  public LoadingLookup get()
  {
    return loadingLookup;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof LoadingLookupFactory)) {
      return false;
    }

    LoadingLookupFactory that = (LoadingLookupFactory) o;

    if (dataFetcher != null ? !dataFetcher.equals(that.dataFetcher) : that.dataFetcher != null) {
      return false;
    }
    if (loadingCache != null ? !loadingCache.equals(that.loadingCache) : that.loadingCache != null) {
      return false;
    }
    return reverseLoadingCache != null
           ? reverseLoadingCache.equals(that.reverseLoadingCache)
           : that.reverseLoadingCache == null;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataFetcher, loadingCache, reverseLoadingCache);
  }
}
