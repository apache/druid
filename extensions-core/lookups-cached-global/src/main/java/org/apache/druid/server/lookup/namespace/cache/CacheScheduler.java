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

package org.apache.druid.server.lookup.namespace.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.concurrent.ConcurrentAwaitableCounter;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.Cleaners;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.query.lookup.namespace.CacheGenerator;
import org.apache.druid.query.lookup.namespace.ExtractionNamespace;

import javax.annotation.Nullable;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Usage:
 * <pre>{@code
 * CacheScheduler.Entry entry = cacheScheduler.schedule(namespace); // or scheduleAndWait(namespace, timeout)
 * CacheState cacheState = entry.getCacheState();
 * // cacheState could be either NoCache or VersionedCache.
 * if (cacheState instanceof NoCache) {
 *   // the cache is not yet created, or already closed
 * } else {
 *   Map<String, String> cache = ((VersionedCache) cacheState).getCache(); // use the cache
 *   // Although VersionedCache implements AutoCloseable, versionedCache shouldn't be manually closed
 *   // when obtained from entry.getCacheState(). If the namespace updates should be ceased completely,
 *   // entry.close() (see below) should be called, it will close the last VersionedCache as well.
 *   // On scheduled updates, outdated VersionedCaches are also closed automatically.
 * }
 * ...
 * entry.close(); // close the last VersionedCache and unschedule future updates
 * }</pre>
 */
@LazySingleton
public final class CacheScheduler
{
  private static final Logger log = new Logger(CacheScheduler.class);

  public final class Entry<T extends ExtractionNamespace> implements AutoCloseable
  {
    private final EntryImpl<T> impl;

    private Entry(final T namespace, final CacheGenerator<T> cacheGenerator)
    {
      impl = new EntryImpl<>(namespace, this, cacheGenerator);
    }

    /**
     * Returns the last cache state, either {@link NoCache} or {@link VersionedCache}.
     */
    public CacheState getCacheState()
    {
      return impl.cacheStateHolder.get();
    }

    /**
     * @return the entry's cache if it is already initialized and not yet closed
     * @throws IllegalStateException if the entry's cache is not yet initialized, or {@link #close()} has
     * already been called
     */
    public Map<String, String> getCache()
    {
      CacheState cacheState = getCacheState();
      if (cacheState instanceof VersionedCache) {
        return ((VersionedCache) cacheState).getCache();
      } else {
        throw new ISE("Cannot get cache: %s", cacheState);
      }
    }

    @VisibleForTesting
    Future<?> getUpdaterFuture()
    {
      return impl.updaterFuture;
    }

    @VisibleForTesting
    public void awaitTotalUpdates(int totalUpdates) throws InterruptedException
    {
      impl.updateCounter.awaitCount(totalUpdates);
    }

    @VisibleForTesting
    public void awaitTotalUpdatesWithTimeout(int totalUpdates, long timeoutMills)
        throws InterruptedException, TimeoutException
    {
      impl.updateCounter.awaitCount(totalUpdates, timeoutMills, TimeUnit.MILLISECONDS);
    }
    @VisibleForTesting
    void awaitNextUpdates(int nextUpdates) throws InterruptedException
    {
      impl.updateCounter.awaitNextIncrements(nextUpdates);
    }

    /**
     * Close the last {@link #getCacheState()}, if it is {@link VersionedCache}, and unschedule future updates.
     */
    @Override
    public void close()
    {
      impl.close();
    }

    @Override
    public String toString()
    {
      return impl.toString();
    }
  }

  /**
   * This class effectively contains the whole state and most of the logic of {@link Entry}, need to be a separate class
   * because the Entry must not be referenced from the runnable executed in {@link #cacheManager}'s ExecutorService,
   * that would be a leak preventing the Entry to be collected by GC, and therefore {@link #entryCleanable} to be run by
   * the JVM. Also, {@link #entryCleanable} must not reference the Entry through it's Runnable hunk.
   */
  public class EntryImpl<T extends ExtractionNamespace> implements AutoCloseable
  {
    private final T namespace;
    private final String asString;
    private final AtomicReference<CacheState> cacheStateHolder = new AtomicReference<>(NoCache.CACHE_NOT_INITIALIZED);
    private final Future<?> updaterFuture;
    private final Cleaners.Cleanable entryCleanable;
    private final CacheGenerator<T> cacheGenerator;
    private final ConcurrentAwaitableCounter updateCounter = new ConcurrentAwaitableCounter();
    private final CountDownLatch startLatch = new CountDownLatch(1);
    private final CompletableFuture<Boolean> firstLoadFinishedSuccessfully = new CompletableFuture<>();

    private EntryImpl(final T namespace, final Entry<T> entry, final CacheGenerator<T> cacheGenerator)
    {
      try {
        this.namespace = namespace;
        this.asString = StringUtils.format("namespace [%s] : %s", namespace, super.toString());
        this.updaterFuture = schedule(namespace);
        this.entryCleanable = createCleaner(entry);
        this.cacheGenerator = cacheGenerator;
        activeEntries.incrementAndGet();
      }
      finally {
        startLatch.countDown();
      }
    }

    private Cleaners.Cleanable createCleaner(Entry<T> entry)
    {
      return Cleaners.register(entry, this::closeFromCleaner);
    }

    private Future<?> schedule(final T namespace)
    {
      final long updateMs = namespace.getPollMs();
      Runnable command = this::updateCache;
      if (updateMs > 0) {
        return cacheManager.scheduledExecutorService().scheduleAtFixedRate(command, namespace.getJitterMills(), updateMs, TimeUnit.MILLISECONDS);
      } else {
        return cacheManager.scheduledExecutorService().schedule(command, namespace.getJitterMills(), TimeUnit.MILLISECONDS);
      }
    }

    private void updateCache()
    {
      boolean updatedCacheSuccessfully = false;
      try {
        // Ensures visibility of the whole EntryImpl's state (fields and their state).
        startLatch.await();
        CacheState currentCacheState = cacheStateHolder.get();
        if (!Thread.currentThread().isInterrupted() && currentCacheState != NoCache.ENTRY_CLOSED) {
          final String currentVersion = currentVersionOrNull(currentCacheState);
          updatedCacheSuccessfully = tryUpdateCache(currentVersion);
        }
      }
      catch (Throwable t) {
        try {
          close();
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        if (Thread.currentThread().isInterrupted() || t instanceof InterruptedException || t instanceof Error) {
          throw new RuntimeException(t);
        }
      }
      finally {
        if (!firstLoadFinishedSuccessfully.isDone()) {
          firstLoadFinishedSuccessfully.complete(updatedCacheSuccessfully);
        }
      }
    }

    private boolean tryUpdateCache(String currentVersion) throws Exception
    {
      boolean updatedCacheSuccessfully = false;
      CacheHandler newCache = null;
      try {
        updatesStarted.incrementAndGet();
        newCache = CacheScheduler.this.cacheManager.allocateCache();
        final String newVersion = cacheGenerator.generateCache(
            namespace,
            this,
            currentVersion,
            newCache
        );
        if (newVersion != null) {
          newCache = cacheManager.attachCache(newCache);
          final VersionedCache newVersionedCache = new VersionedCache(String.valueOf(this), newVersion, newCache);
          final CacheState previousCacheState = swapCacheState(newVersionedCache);
          if (previousCacheState != NoCache.ENTRY_CLOSED) {
            updatedCacheSuccessfully = true;
            if (previousCacheState instanceof VersionedCache) {
              ((VersionedCache) previousCacheState).close();
            }
            log.debug("%s: the cache was successfully updated", this);
          } else {
            newVersionedCache.close();
            log.debug("%s was closed while the cache was being updated, discarding the update", this);
          }
        } else {
          log.debug("%s: Version `%s` not updated, the cache is not updated", this, currentVersion);
        }
      }
      catch (Throwable t) {
        try {
          if (newCache != null && !updatedCacheSuccessfully) {
            newCache.close();
          }
          log.error(t, "Failed to update %s", this);
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        if (Thread.currentThread().isInterrupted() || t instanceof InterruptedException || t instanceof Error) {
          // propagate to the catch block in updateCache()
          throw t;
        }
      }
      return updatedCacheSuccessfully;
    }

    private String currentVersionOrNull(CacheState currentCacheState)
    {
      if (currentCacheState instanceof VersionedCache) {
        return ((VersionedCache) currentCacheState).version;
      } else {
        return null;
      }
    }

    private CacheState swapCacheState(VersionedCache newVersionedCache)
    {
      CacheState lastCacheState;
      // CAS loop
      do {
        lastCacheState = cacheStateHolder.get();
        if (lastCacheState == NoCache.ENTRY_CLOSED) {
          return lastCacheState;
        }
      } while (!cacheStateHolder.compareAndSet(lastCacheState, newVersionedCache));
      updateCounter.increment();
      return lastCacheState;
    }

    @Override
    public void close()
    {
      if (!doClose(true)) {
        log.error("Cache for %s has already been closed", this);
      }
      // This clean() call effectively just removes the object from the internal linked list of all cleanables.
      // It will delegate to closeFromCleaner() which will be a no-op because cacheStateHolder is already set to
      // ENTRY_CLOSED.
      entryCleanable.clean();
    }

    private void closeFromCleaner()
    {
      try {
        if (doClose(false)) {
          log.error("Entry.close() was not called, closed resources by the JVM");
        }
      }
      catch (Throwable t) {
        try {
          log.error(t, "Error while closing %s", this);
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        Throwables.propagateIfInstanceOf(t, Error.class);
        // Must not throw exceptions in the cleaner thread, possibly run by the JVM.
      }
    }

    /**
     * @param calledManually true if called manually from {@link #close()}, false if called by the JVM via Cleaner
     * @return true if successfully closed, false if has already closed before
     */
    private boolean doClose(boolean calledManually)
    {
      CacheState lastCacheState = cacheStateHolder.getAndSet(NoCache.ENTRY_CLOSED);
      if (lastCacheState != NoCache.ENTRY_CLOSED) {
        try {
          log.info("Closing %s", this);
          logExecutionError();
        }
        // Logging (above) is not the main goal of the closing process, so try to cancel the updaterFuture even if
        // logging failed for whatever reason.
        finally {
          activeEntries.decrementAndGet();
          updaterFuture.cancel(true);
          // If calledManually = false, i. e. called by the JVM via Cleaner.clean(), let the JVM close cache itself
          // via it's own Cleaner as well, when the cache becomes unreachable. Because when somebody forgets to call
          // entry.close(), it may be harmful to forcibly close the cache, which could still be used, at some
          // non-deterministic point of time. Cleaners are introduced to mitigate possible errors, not to escalate them.
          if (calledManually && lastCacheState instanceof VersionedCache) {
            ((VersionedCache) lastCacheState).cacheHandler.close();
          }
        }
        return true;
      } else {
        return false;
      }
    }

    private void logExecutionError()
    {
      if (updaterFuture.isDone()) {
        try {
          updaterFuture.get();
        }
        catch (ExecutionException ee) {
          log.error(ee.getCause(), "Error in %s", this);
        }
        catch (CancellationException ce) {
          log.error(ce, "Future for %s has already been cancelled", this);
        }
        catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new RuntimeException(ie);
        }
      }
    }

    @Override
    public String toString()
    {
      return asString;
    }
  }

  public interface CacheState
  {}

  public enum NoCache implements CacheState
  {
    CACHE_NOT_INITIALIZED,
    ENTRY_CLOSED
  }

  public static final class VersionedCache implements CacheState, AutoCloseable
  {
    final String entryId;
    final CacheHandler cacheHandler;
    final String version;

    private VersionedCache(String entryId, String version, CacheHandler cache)
    {
      this.entryId = entryId;
      this.cacheHandler = cache;
      this.version = version;
    }

    public Map<String, String> getCache()
    {
      return cacheHandler.getCache();
    }

    /**
     * Returns a {@link LookupExtractor} view of the cached data.
     */
    public LookupExtractor asLookupExtractor(final boolean isOneToOne, final Supplier<byte[]> cacheKeySupplier)
    {
      return cacheHandler.asLookupExtractor(isOneToOne, cacheKeySupplier);
    }

    public String getVersion()
    {
      return version;
    }

    @Override
    public void close()
    {
      cacheHandler.close();
      // Log statement after cacheHandler.close(), because logging may fail (e. g. in shutdown hooks)
      log.debug("Closed version [%s] of %s", version, entryId);
    }
  }

  private final Map<Class<? extends ExtractionNamespace>, CacheGenerator<?>> namespaceGeneratorMap;
  private final NamespaceExtractionCacheManager cacheManager;
  private final AtomicLong updatesStarted = new AtomicLong(0);
  private final AtomicInteger activeEntries = new AtomicInteger();

  @Inject
  public CacheScheduler(
      final ServiceEmitter serviceEmitter,
      final Map<Class<? extends ExtractionNamespace>, CacheGenerator<?>> namespaceGeneratorMap,
      NamespaceExtractionCacheManager cacheManager
  )
  {
    // Accesses to IdentityHashMap should be faster than to HashMap or ImmutableMap.
    // Class doesn't override Object.equals().
    this.namespaceGeneratorMap = new IdentityHashMap<>(namespaceGeneratorMap);
    this.cacheManager = cacheManager;
    cacheManager.scheduledExecutorService().scheduleAtFixedRate(
        new Runnable()
        {
          long priorUpdatesStarted = 0L;

          @Override
          public void run()
          {
            try {
              final long tasks = updatesStarted.get();
              serviceEmitter.emit(
                  ServiceMetricEvent.builder()
                                    .setMetric("namespace/deltaTasksStarted", tasks - priorUpdatesStarted)
              );
              priorUpdatesStarted = tasks;
            }
            catch (Exception e) {
              log.error(e, "Error emitting namespace stats");
              if (Thread.currentThread().isInterrupted()) {
                throw new RuntimeException(e);
              }
            }
          }
        },
        1,
        10, TimeUnit.MINUTES
    );
  }

  @VisibleForTesting
  long updatesStarted()
  {
    return updatesStarted.get();
  }

  @VisibleForTesting
  public long getActiveEntries()
  {
    return activeEntries.get();
  }

  @Nullable
  public Entry scheduleAndWait(ExtractionNamespace namespace, long waitForFirstRunMs) throws InterruptedException
  {
    Exception loadException = null;
    final Entry entry = schedule(namespace);
    log.debug("Scheduled new %s", entry);
    boolean success = false;
    try {
      success = (boolean) entry.impl.firstLoadFinishedSuccessfully.get(waitForFirstRunMs, TimeUnit.MILLISECONDS);
      if (success) {
        return entry;
      } else {
        return null;
      }
    }
    catch (ExecutionException | TimeoutException e) {
      loadException = e;
      return null;
    }
    finally {
      if (!success) {
        // ExecutionException's cause is logged in entry.close()
        entry.close();
        if (loadException != null) {
          log.error(loadException, "CacheScheduler[%s] - problem during start or waiting for the first run", entry);
        } else {
          log.error("CacheScheduler[%s] - problem during start or waiting for the first run", entry);
        }
      }
    }
  }

  public <T extends ExtractionNamespace> Entry schedule(final T namespace)
  {
    @SuppressWarnings("unchecked")
    final CacheGenerator<T> generator = (CacheGenerator<T>) namespaceGeneratorMap.get(namespace.getClass());
    if (generator == null) {
      throw new ISE("Cannot find generator for namespace [%s]", namespace);
    }
    return new Entry<>(namespace, generator);
  }
}
