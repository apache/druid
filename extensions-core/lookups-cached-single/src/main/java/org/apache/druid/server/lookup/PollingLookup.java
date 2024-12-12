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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.server.lookup.cache.polling.OnHeapPollingCache;
import org.apache.druid.server.lookup.cache.polling.PollingCache;
import org.apache.druid.server.lookup.cache.polling.PollingCacheFactory;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PollingLookup extends LookupExtractor
{
  private static final Logger LOGGER = new Logger(PollingLookup.class);

  private final long pollPeriodMs;

  private final DataFetcher dataFetcher;

  private final PollingCacheFactory cacheFactory;

  private final AtomicReference<CacheRefKeeper> refOfCacheKeeper = new AtomicReference<>();


  private final ListeningScheduledExecutorService scheduledExecutorService;

  private final AtomicBoolean isOpen = new AtomicBoolean(false);

  private final ListenableFuture<?> pollFuture;

  private final String id = Integer.toHexString(System.identityHashCode(this));

  public PollingLookup(
      long pollPeriodMs,
      DataFetcher dataFetcher,
      PollingCacheFactory cacheFactory
  )
  {
    this.pollPeriodMs = pollPeriodMs;
    this.dataFetcher = Preconditions.checkNotNull(dataFetcher);
    this.cacheFactory = cacheFactory == null ? new OnHeapPollingCache.OnHeapPollingCacheProvider() : cacheFactory;
    refOfCacheKeeper.set(new CacheRefKeeper(this.cacheFactory.makeOf(dataFetcher.fetchAll())));
    if (pollPeriodMs > 0) {
      scheduledExecutorService = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor(
          Execs.makeThreadFactory("PollingLookup-" + StringUtils.encodeForFormat(id), Thread.MIN_PRIORITY)
      ));
      pollFuture = scheduledExecutorService.scheduleWithFixedDelay(
          pollAndSwap(),
          pollPeriodMs,
          pollPeriodMs,
          TimeUnit.MILLISECONDS
      );
    } else {
      scheduledExecutorService = null;
      pollFuture = null;
    }
    this.isOpen.set(true);
  }


  public void close()
  {
    LOGGER.info("Closing polling lookup [%s]", id);
    synchronized (isOpen) {
      isOpen.getAndSet(false);
      if (pollFuture != null) {
        pollFuture.cancel(true);
        scheduledExecutorService.shutdown();
      }
      CacheRefKeeper cacheRefKeeper = refOfCacheKeeper.getAndSet(null);
      if (cacheRefKeeper != null) {
        cacheRefKeeper.doneWithIt();
      }
    }
  }

  @Override
  @Nullable
  public String apply(@Nullable String key)
  {
    String keyEquivalent = NullHandling.nullToEmptyIfNeeded(key);
    if (keyEquivalent == null) {
      // valueEquivalent is null only for SQL Compatible Null Behavior
      // otherwise null will be replaced with empty string in nullToEmptyIfNeeded above.
      return null;
    }
    final CacheRefKeeper cacheRefKeeper = refOfCacheKeeper.get();
    if (cacheRefKeeper == null) {
      throw new ISE("Cache reference is null");
    }
    final PollingCache cache = cacheRefKeeper.getAndIncrementRef();
    try {
      if (cache == null) {
        // it must've been closed after swapping while I was getting it.  Try again.
        return this.apply(keyEquivalent);
      }
      return NullHandling.emptyToNullIfNeeded((String) cache.get(keyEquivalent));
    }
    finally {
      if (cache != null) {
        cacheRefKeeper.doneWithIt();
      }
    }
  }

  @Override
  public List<String> unapply(@Nullable final String value)
  {
    String valueEquivalent = NullHandling.nullToEmptyIfNeeded(value);
    if (valueEquivalent == null) {
      // valueEquivalent is null only for SQL Compatible Null Behavior
      // otherwise null will be replaced with empty string in nullToEmptyIfNeeded above.
      // null value maps to empty list when SQL Compatible
      return Collections.emptyList();
    }

    CacheRefKeeper cacheRefKeeper = refOfCacheKeeper.get();
    if (cacheRefKeeper == null) {
      throw new ISE("pollingLookup id [%s] is closed", id);
    }
    PollingCache cache = cacheRefKeeper.getAndIncrementRef();
    try {
      if (cache == null) {
        // it must've been closed after swapping while I was getting it.  Try again.
        return this.unapply(valueEquivalent);
      }
      return cache.getKeys(valueEquivalent);
    }
    finally {
      if (cache != null) {
        cacheRefKeeper.doneWithIt();
      }
    }
  }

  @Override
  public boolean supportsAsMap()
  {
    return false;
  }

  @Override
  public Map<String, String> asMap()
  {
    throw new UnsupportedOperationException("Cannot get map view");
  }

  @Override
  public byte[] getCacheKey()
  {
    return LookupExtractionModule.getRandomCacheKey();
  }

  private Runnable pollAndSwap()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        LOGGER.debug("Polling and swapping of PollingLookup [%s]", id);
        CacheRefKeeper newCacheKeeper = new CacheRefKeeper(cacheFactory.makeOf(dataFetcher.fetchAll()));
        CacheRefKeeper oldCacheKeeper = refOfCacheKeeper.getAndSet(newCacheKeeper);
        if (oldCacheKeeper != null) {
          oldCacheKeeper.doneWithIt();
        }
      }
    };
  }

  @Override
  public long estimateHeapFootprint()
  {
    PollingCache<?, ?> cache = null;

    while (cache == null) {
      final CacheRefKeeper cacheRefKeeper = refOfCacheKeeper.get();

      if (cacheRefKeeper == null) {
        // Closed.
        return 0;
      }

      // If null, we'll do another run through the while loop.
      cache = cacheRefKeeper.getAndIncrementRef();
    }

    return cache.estimateHeapFootprint();
  }

  @Override
  public int hashCode()
  {
    int result = (int) (pollPeriodMs ^ (pollPeriodMs >>> 32));
    result = 31 * result + dataFetcher.hashCode();
    result = 31 * result + cacheFactory.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof PollingLookup)) {
      return false;
    }

    PollingLookup that = (PollingLookup) o;

    if (pollPeriodMs != that.pollPeriodMs) {
      return false;
    }
    if (!dataFetcher.equals(that.dataFetcher)) {
      return false;
    }
    return cacheFactory.equals(that.cacheFactory);

  }

  public boolean isOpen()
  {
    return isOpen.get();
  }


  protected static class CacheRefKeeper
  {
    private final PollingCache pollingCache;
    private final AtomicLong refCounts = new AtomicLong(0L);

    CacheRefKeeper(PollingCache pollingCache)
    {
      this.pollingCache = pollingCache;
    }

    PollingCache getAndIncrementRef()
    {
      synchronized (refCounts) {
        if (refCounts.get() < 0) {
          return null;
        }
        refCounts.incrementAndGet();
        return pollingCache;
      }
    }

    void doneWithIt()
    {
      synchronized (refCounts) {
        if (refCounts.get() == 0) {
          pollingCache.close();
        }
        refCounts.decrementAndGet();
      }
    }
  }

  @Override
  public String toString()
  {
    return "PollingLookup{" +
           "dataFetcher=" + dataFetcher +
           ", id='" + id + '\'' +
           '}';
  }
}
