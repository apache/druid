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

package org.apache.druid.segment.loading.external;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.common.asyncresource.SettableAsyncResource;
import org.apache.druid.error.DruidException;
import org.apache.druid.io.FilePopulator;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.loading.CacheEntry;
import org.apache.druid.segment.loading.StorageLoadingThreadPool;
import org.apache.druid.segment.loading.StorageLocation;
import org.apache.druid.segment.loading.StorageLocationSelectorStrategy;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * Default implementation of VirtualStorageManager that delegates to StorageLocation.
 * Uses weak reservations for all cached files, making them eligible for eviction.
 */
public class StorageLocationVirtualStorageManager implements VirtualStorageManager
{
  private static final EmittingLogger log = new EmittingLogger(StorageLocationVirtualStorageManager.class);

  private final StorageLocationSelectorStrategy strategy;
  private final StorageLoadingThreadPool loadingThreadPool;

  /**
   * Per-identifier locks to ensure only one thread populates a given identifier.
   * Once a location is resolved, it's stored in the lock so subsequent threads can check it.
   */
  private final ConcurrentHashMap<String, PopulationLock> populationLocks = new ConcurrentHashMap<>();

  @Inject
  public StorageLocationVirtualStorageManager(
      List<StorageLocation> locations,
      StorageLocationSelectorStrategy strategy,
      StorageLoadingThreadPool loadingThreadPool
  )
  {
    this.strategy = strategy;
    this.loadingThreadPool = loadingThreadPool;
    log.info("Initialized VirtualStorageManager with [%d] storage locations", locations.size());
  }

  @Nullable
  @Override
  public CachedFile get(String identifier)
  {
    // We only consult the lock to discover which location the entry was resolved to. No need for synchronization,
    // because we don't intend to populate it.
    final PopulationLock lock = populationLocks.get(identifier);
    if (lock == null) {
      return null;
    }

    final StorageLocation resolvedLocation = lock.getResolvedLocation();
    if (resolvedLocation == null) {
      // Population is still in flight: the lock exists but doesn't have a location yet.
      // Return that the file doesn't exist instead of blocking.
      return null;
    }

    final StorageLocation.ReservationHold<CacheEntry> hold =
        resolvedLocation.addWeakReservationHoldIfExists(new StringCacheEntryIdentifier(identifier));
    return hold == null ? null : new CachedFile(hold, identifier);
  }

  @Override
  public CachedFile reserveAndPopulate(
      String identifier,
      LongSupplier sizeSupplier,
      FilePopulator populator
  )
  {
    // Get or create lock for this identifier
    final PopulationLock lock = populationLocks.computeIfAbsent(identifier, ignored -> new PopulationLock());

    synchronized (lock) {
      final StringCacheEntryIdentifier cacheId = new StringCacheEntryIdentifier(identifier);

      // If multiple threads are trying to reserve the same location, the first one will update the state on the lock
      // so check that first.
      final StorageLocation resolvedLocation = lock.getResolvedLocation();
      if (resolvedLocation == null) {
        // Determining the size often requires an external system call, so we use this supplier to defer it until
        // we absolutely need it
        final long sizeBytes = sizeSupplier.getAsLong();

        // Try to reserve in each location according to strategy
        final Iterator<StorageLocation> locationIter = strategy.getLocations();
        Throwable lastException = null;

        while (locationIter.hasNext()) {
          final StorageLocation location = locationIter.next();
          final File locationFile = location.getPath();
          try {
            // Reserve space and acquire a hold, using a cache entry that will call the populator on mount.
            final StorageLocation.ReservationHold<CacheEntry> hold = location.addWeakReservationHold(
                cacheId,
                () -> new DownloadableCacheEntry(cacheId, sizeBytes, populator, locationFile)
                {
                  final AtomicBoolean mounted = new AtomicBoolean(false);

                  @Override
                  public void mount(StorageLocation location)
                  {
                    super.mount(location);
                    if (mounted.compareAndSet(false, true)) {
                      location.trackWeakLoad(getSize());
                    }
                  }

                  @Override
                  public void unmount()
                  {
                    if (mounted.get()) {
                      populationLocks.remove(identifier, lock);
                    }
                    super.unmount();
                  }
                }
            );

            if (hold == null) {
              log.debug(
                  "Failed to reserve [%d] bytes for [%s] in location [%s], trying next",
                  sizeBytes,
                  identifier,
                  locationFile
              );
              continue;
            }

            // Mount the entry (calls populator)
            try {
              hold.getEntry().mount(location);
            }
            catch (Throwable e) {
              try {
                hold.close();
              }
              catch (Throwable e2) {
                e.addSuppressed(e2);
              }
              throw e;
            }

            // Store the resolved location for anything that is waiting on this same lock
            lock.setResolvedLocation(location);

            return new CachedFile(hold, identifier);
          }
          catch (Throwable e) {
            lastException = e;
            log.debug(e, "Failed to reserve and populate in location [%s], trying next", locationFile);
          }
        }
        // We have exited the loop, which should have returned, nothing must've worked...

        if (lastException != null) {
          throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                              .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                              .build(
                                  lastException,
                                  "Failed to reserve and populate entry [%s] in any location",
                                  identifier
                              );
        } else {
          throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                              .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                              .build(
                                  "No space available to reserve [%,d] bytes for entry [%s]",
                                  sizeBytes,
                                  identifier
                              );
        }
      } else {
        // Try to get from the resolved location first
        StorageLocation.ReservationHold<CacheEntry> hold = resolvedLocation.addWeakReservationHoldIfExists(cacheId);

        if (hold != null) {
          return new CachedFile(hold, identifier);
        } else {
          // hold == null means the entry was evicted before we had a chance to add our hold.
          // Drop the stale lock and re-resolve.
          populationLocks.remove(identifier, lock);
          return reserveAndPopulate(identifier, sizeSupplier, populator);
        }
      }
    }
  }

  @Override
  public AsyncResource<CachedFile> reserveAndPopulateAsync(
      String identifier,
      LongSupplier sizeSupplier,
      FilePopulator populator
  )
  {
    final CachedFile cachedFile = get(identifier);
    if (cachedFile != null) {
      return AsyncResources.ofCloseable(cachedFile);
    }

    if (loadingThreadPool.isAvailable()) {
      final SettableAsyncResource<CachedFile> resource = new SettableAsyncResource<>();
      final ListenableFuture<?> future = loadingThreadPool.getExecutorService().submit(
          () -> {
            try {
              final Semaphore loadingPermits = loadingThreadPool.getPermits();
              if (loadingPermits != null) {
                loadingPermits.acquire();
              }
              try {
                final CachedFile theCachedFile = reserveAndPopulate(identifier, sizeSupplier, populator);
                if (!resource.set(ResourceHolder.fromCloseable(theCachedFile))) {
                  theCachedFile.close();
                }
              }
              finally {
                if (loadingPermits != null) {
                  loadingPermits.release();
                }
              }
            }
            catch (Throwable e) {
              resource.setException(e);
            }
          }
      );

      resource.setCanceler(() -> future.cancel(true));
      return resource;
    } else {
      final SettableAsyncResource<CachedFile> resource = new SettableAsyncResource<>();
      try {
        resource.set(ResourceHolder.fromCloseable(reserveAndPopulate(identifier, sizeSupplier, populator)));
      }
      catch (Throwable e) {
        resource.setException(e);
      }
      return resource;
    }
  }

  /**
   * Lock object that tracks which StorageLocation was used for a given identifier.
   * Once resolved, subsequent threads can check the resolved location first.
   */
  private static class PopulationLock
  {
    private final AtomicReference<StorageLocation> resolvedLocation = new AtomicReference<>();

    @Nullable
    StorageLocation getResolvedLocation()
    {
      return resolvedLocation.get();
    }

    void setResolvedLocation(StorageLocation location)
    {
      if (!resolvedLocation.compareAndSet(null, location)) {
        throw DruidException.defensive(
            "Resolved location already set to [%s], cannot change to [%s]",
            resolvedLocation.get().getPath(),
            location.getPath()
        );
      }
    }
  }
}
