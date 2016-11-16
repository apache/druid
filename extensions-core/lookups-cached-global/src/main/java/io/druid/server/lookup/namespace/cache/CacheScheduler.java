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

package io.druid.server.lookup.namespace.cache;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;
import sun.misc.Cleaner;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Usage:
 * <pre>{@code
 * CacheScheduler.Entry entry = cacheScheduler.schedule(namespace); // or scheduleAndWait(namespace, timeout)
 * CacheState cacheState = entry.getCacheState();
 * if (cacheState instanceof NoCache) {
 *   // the cache is not yet created, or already disposed
 * } else if (cacheState instanceof VersionedCache) {
 *   Map<String, String> cache = ((VersionedCache) cacheState).getCache(); // use the cache
 * }
 * ...
 * entry.close(); // dispose the last VersionedCache and unschedule future updates
 * }</pre>
 */
public final class CacheScheduler
{
  private static final Logger log = new Logger(CacheScheduler.class);

  public final class Entry<T extends ExtractionNamespace> implements AutoCloseable
  {
    private final EntryImpl<T> impl;

    private Entry(final T namespace, final ExtractionNamespaceCacheFactory<T> cachePopulator)
    {
      impl = new EntryImpl<>(namespace, this, cachePopulator);
    }

    /**
     * Returns the last cache state, either {@link NoCache} or {@link VersionedCache}.
     */
    public CacheState getCacheState()
    {
      return impl.cacheStateHolder.get();
    }

    /**
     * @return the entry's cache if it is already initialized and not yet disposed
     * @throws IllegalStateException if the entry's cache is not yet initialized, or {@link #close()} has
     * already been called
     */
    public Map<String, String> getCache()
    {
      CacheState cacheState = getCacheState();
      if (cacheState instanceof VersionedCache) {
        return ((VersionedCache) cacheState).getCache();
      } else {
        throw new IllegalStateException("Cannot get cache: " + cacheState);
      }
    }

    /** For tests */
    Future<?> getUpdaterFuture()
    {
      return impl.updaterFuture;
    }

    /**
     * Awaits for {@code totalUpdates} cache updates since Entry creation. Should be used in tests only, unreliable
     * for production.
     *
     * <p>Proper implementation should use {@link java.util.concurrent.locks.AbstractQueuedSynchronizer}, that adds
     * a lot of complexity.
     */
    public void awaitTotalUpdates(long totalUpdates) throws InterruptedException
    {
      while (totalUpdates - impl.updateCounter.get() > 0) { // overflow-aware
        Thread.sleep(1);
      }
    }

    /**
     * Awaits for {@code nextUpdates} cache updates since this method is called. Should be used in tests only,
     * unreliable for production.
     */
    void awaitNextUpdates(int nextUpdates) throws InterruptedException
    {
      awaitTotalUpdates(impl.updateCounter.get() + nextUpdates);
    }

    /**
     * Dispose the last {@link #getCacheState()}, if it is {@link VersionedCache}, and unschedule future updates.
     */
    public void close()
    {
      impl.dispose();
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
   * that would be a leak preventing the Entry to be collected by GC, and therefore {@link #entryCleaner} to be run by
   * the JVM. Also, {@link #entryCleaner} must not reference the Entry through it's Runnable hunk.
   */
  public class EntryImpl<T extends ExtractionNamespace> {

    private final T namespace;
    private final EntryImpl<T> entryId = this;
    private final AtomicReference<CacheState> cacheStateHolder = new AtomicReference<CacheState>(NoCache.CACHE_NOT_INITIALIZED);
    private final Future<?> updaterFuture;
    private final Cleaner entryCleaner;
    private final ExtractionNamespaceCacheFactory<T> cachePopulator;
    private final AtomicLong updateCounter = new AtomicLong(0);
    private final CountDownLatch firstRunLatch = new CountDownLatch(1);
    private final CountDownLatch startLatch = new CountDownLatch(1);

    private EntryImpl(final T namespace, final Entry<T> entry, final ExtractionNamespaceCacheFactory<T> cachePopulator)
    {
      try {
        this.namespace = namespace;
        this.updaterFuture = schedule(namespace);
        this.entryCleaner = createCleaner(entry);
        this.cachePopulator = cachePopulator;
        activeEntries.incrementAndGet();
      }
      finally {
        startLatch.countDown();
      }
    }

    private Cleaner createCleaner(Entry<T> entry)
    {
      return Cleaner.create(entry, new Runnable()
      {
        @Override
        public void run()
        {
          disposeFromCleaner();
        }
      });
    }

    private Future<?> schedule(final T namespace)
    {
      final long updateMs = namespace.getPollMs();
      Runnable command = new Runnable()
      {
        @Override
        public void run()
        {
          updateCache();
        }
      };
      if (updateMs > 0) {
        return cacheManager.scheduledExecutorService().scheduleAtFixedRate(command, 0, updateMs, TimeUnit.MILLISECONDS);
      } else {
        return cacheManager.scheduledExecutorService().schedule(command, 0, TimeUnit.MILLISECONDS);
      }
    }

    private void updateCache()
    {
      try {
        // Ensures visibility of the whole EntryImpl's state (fields and their state).
        startLatch.await();

        CacheState currentCacheState = cacheStateHolder.get();
        if (!Thread.currentThread().isInterrupted() && currentCacheState != NoCache.ENTRY_DISPOSED) {
          final String currentVersion = currentVersionOrNull(currentCacheState);
          final VersionedCache newVersionedCache =
              cachePopulator.populateCache(namespace, entryId, currentVersion, CacheScheduler.this);
          if (newVersionedCache != null) {
            swapCacheState(newVersionedCache);
          } else {
            log.debug("%s: Version `%s` not updated, the cache is not updated", entryId, currentVersion);
          }
        }
      }
      catch (Throwable t) {
        try {
          dispose();
          log.error(t, "Failed to update %s", entryId);
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        if (Thread.currentThread().isInterrupted() || t instanceof Error) {
          throw Throwables.propagate(t);
        }
      }
    }

    private String currentVersionOrNull(CacheState currentCacheState)
    {
      if (currentCacheState instanceof VersionedCache) {
        return ((VersionedCache) currentCacheState).version;
      } else {
        // currentCacheState == CACHE_NOT_INITIALIZED
        return null;
      }
    }

    private void swapCacheState(VersionedCache newVersionedCache)
    {
      CacheState lastCacheState;
      // CAS loop
      do {
        lastCacheState = cacheStateHolder.get();
        if (lastCacheState == NoCache.ENTRY_DISPOSED) {
          newVersionedCache.close();
          log.debug("%s was disposed while the cache was being updated, discarding the update", entryId);
          return;
        }
      } while (!cacheStateHolder.compareAndSet(lastCacheState, newVersionedCache));

      if (updateCounter.incrementAndGet() == 1) {
        firstRunLatch.countDown();
      }
      if (lastCacheState instanceof VersionedCache) {
        ((VersionedCache) lastCacheState).cacheHandler.close();
      }
      log.debug("%s: the cache was successfully updated", entryId);
    }

    private void dispose()
    {
      if (!doDispose(true)) {
        log.error("Cache for %s has already been disposed", entryId);
      }
      // This Cleaner.clean() call effectively just removes the Cleaner from the internal linked list of all cleaners.
      // It will delegate to EntryDisposer.run() which will be a no-op because entryDisposer.cacheStateHolder is already
      // set to ENTRY_DISPOSED.
      entryCleaner.clean();
    }

    private void disposeFromCleaner()
    {
      try {
        if (doDispose(false)) {
          log.error("Entry.close() was not called, disposed resources by the JVM");
        }
      }
      catch (Throwable t) {
        try {
          log.error(t, "Error while disposing %s", entryId);
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        Throwables.propagateIfInstanceOf(t, Error.class);
        // Must not throw exceptions in the cleaner thread, run by the JVM.
      }
    }

    /**
     * @param calledManually true if called manually from {@link #dispose()}, false if called by the JVM via Cleaner
     * @return true if successfully disposed, false if has already disposed before
     */
    private boolean doDispose(boolean calledManually)
    {
      CacheState lastCacheState = cacheStateHolder.getAndSet(NoCache.ENTRY_DISPOSED);
      if (lastCacheState != NoCache.ENTRY_DISPOSED) {
        try {
          log.info("Disposing %s", entryId);
          logExecutionError();
        }
        // Logging (above) is not the main goal of the disposal process, so try to cancel the updaterFuture even if
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
          log.error(ee.getCause(), "Error in %s", entryId);
        }
        catch (CancellationException ce) {
          log.error(ce, "Future for %s has already been cancelled", entryId);
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
      return String.format("namespace [%s] : %s", namespace, super.toString());
    }
  }

  public interface CacheState
  {}

  public enum NoCache implements CacheState
  {
    CACHE_NOT_INITIALIZED,
    ENTRY_DISPOSED
  }

  public final class VersionedCache implements CacheState, AutoCloseable
  {
    final CacheHandler cacheHandler;
    final String version;

    private VersionedCache(CacheHandler cacheHandler, String version)
    {
      this.cacheHandler = cacheHandler;
      this.version = version;
    }

    public Map<String, String> getCache()
    {
      return cacheHandler.getCache();
    }

    public String getVersion()
    {
      return version;
    }

    @Override
    public void close()
    {
      cacheHandler.close();
    }
  }

  private final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> namespacePopulatorMap;
  private final NamespaceExtractionCacheManager cacheManager;
  private final AtomicLong updatesStarted = new AtomicLong(0);
  private final AtomicInteger activeEntries = new AtomicInteger();

  @Inject
  public CacheScheduler(
      final ServiceEmitter serviceEmitter,
      final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> namespacePopulatorMap,
      NamespaceExtractionCacheManager cacheManager
  )
  {
    this.namespacePopulatorMap = namespacePopulatorMap;
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
                                    .build("namespace/deltaTasksStarted", tasks - priorUpdatesStarted)
              );
              priorUpdatesStarted = tasks;
            }
            catch (Exception e) {
              log.error(e, "Error emitting namespace stats");
              if (Thread.currentThread().isInterrupted()) {
                throw Throwables.propagate(e);
              }
            }
          }
        },
        1,
        10, TimeUnit.MINUTES
    );
  }

  /**
   * This method should be used from {@link ExtractionNamespaceCacheFactory#populateCache} implementations, to obtain
   * a {@link VersionedCache} to be returned.
   *
   * @param version version, associated with the cache
   */
  public VersionedCache createVersionedCache(String version)
  {
    updatesStarted.incrementAndGet();
    return new VersionedCache(cacheManager.createCache(), version);
  }

  /** For tests */
  long updatesStarted()
  {
    return updatesStarted.get();
  }

  /** For tests */
  public long getActiveEntries()
  {
    return activeEntries.get();
  }

  @Nullable
  public Entry scheduleAndWait(ExtractionNamespace namespace, long waitForFirstRunMs) throws InterruptedException
  {
    final Entry entry = schedule(namespace);
    log.debug("Scheduled new %s", entry);
    boolean success = false;
    try {
      success = entry.impl.firstRunLatch.await(waitForFirstRunMs, TimeUnit.MILLISECONDS);
      if (success) {
        return entry;
      } else {
        return null;
      }
    }
    finally {
      if (!success) {
        log.error("CacheScheduler[%s] - problem during start or waiting for the first run", entry);
        // ExecutionException's cause is logged in entry.close()
        entry.close();
      }
    }
  }

  public <T extends ExtractionNamespace> Entry schedule(final T namespace)
  {
    final ExtractionNamespaceCacheFactory<T> populator =
        (ExtractionNamespaceCacheFactory<T>) namespacePopulatorMap.get(namespace.getClass());
    if (populator == null) {
      throw new ISE("Cannot find populator for namespace [%s]", namespace);
    }
    return new Entry<>(namespace, populator);
  }
}
