/*
 *
 *  Licensed to Metamarkets Group Inc. (Metamarkets) under one
 *  or more contributor license agreements. See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership. Metamarkets licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 * /
 */

package io.druid.server.lookup;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import io.druid.concurrent.Execs;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.LookupExtractor;
import io.druid.server.lookup.cache.polling.OnHeapPollingCache;
import io.druid.server.lookup.cache.polling.PollingCache;
import io.druid.server.lookup.cache.polling.PollingCacheFactory;

import javax.validation.constraints.NotNull;
import java.util.List;
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

  private final String id =  Integer.toHexString(System.identityHashCode(this));

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
          Execs.makeThreadFactory("PollingLookup-" + id, Thread.MIN_PRIORITY)
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
  public String apply(@NotNull String key)
  {
    final CacheRefKeeper cacheRefKeeper = refOfCacheKeeper.get();
    if (cacheRefKeeper == null) {
      throw new ISE("Cache reference is null WTF");
    }
    final PollingCache cache = cacheRefKeeper.getAndIncrementRef();
    try {
      if (cache == null) {
        // it must've been closed after swapping while I was getting it.  Try again.
        return this.apply(key);
      }
      return Strings.emptyToNull((String) cache.get(key));
    }
    finally {
      if (cacheRefKeeper != null && cache != null) {
        cacheRefKeeper.doneWithIt();
      }
    }
  }

  @Override
  public List<String> unapply(final String value)
  {
    CacheRefKeeper cacheRefKeeper = refOfCacheKeeper.get();
    if (cacheRefKeeper == null) {
      throw new ISE("pollingLookup id [%s] is closed", id);
    }
    PollingCache cache = cacheRefKeeper.getAndIncrementRef();
    try {
      if (cache == null) {
        // it must've been closed after swapping while I was getting it.  Try again.
        return this.unapply(value);
      }
      return cache.getKeys(value);
    }
    finally {
      if (cacheRefKeeper != null && cache != null) {
        cacheRefKeeper.doneWithIt();
      }
    }
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

    CacheRefKeeper(PollingCache pollingCache) {this.pollingCache = pollingCache;}

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

}
