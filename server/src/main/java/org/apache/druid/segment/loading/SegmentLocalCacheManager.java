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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

/**
 *
 */
public class SegmentLocalCacheManager implements SegmentCacheManager
{
  private static final String DROP_PATH = "__drop";

  @VisibleForTesting
  static final String DOWNLOAD_START_MARKER_FILE_NAME = "downloadStartMarker";

  private static final EmittingLogger log = new EmittingLogger(SegmentLocalCacheManager.class);

  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  private final List<StorageLocation> locations;

  /**
   * A map between segment and referenceCountingLocks.
   * <p>
   * These locks should be acquired whenever assigning a segment to a location. If different threads try to load
   * segments simultaneously, one of them creates a lock first using {@link #lock(DataSegment)}. And then, all threads
   * compete with each other to get the lock. Finally, the lock should be released using
   * {@link #unlock(DataSegment, ReferenceCountingLock)}. A lock must be acquired any time a {@link SegmentCacheEntry}
   * (either {@link CompleteSegmentCacheEntry} or {@link PartialSegmentMetadataCacheEntry}) needs to be assigned to a
   * {@link StorageLocation}.
   * <p>
   * An example usage is:
   * <p>
   * final ReferenceCountingLock lock = lock(dataSegment);
   * synchronized (lock) {
   *   try {
   *     // assign location
   *     ...
   *   }
   *   finally {
   *     unlock(dataSegment, lock);
   *   }
   * }
   */

  private final ConcurrentHashMap<DataSegment, ReferenceCountingLock> segmentLocks = new ConcurrentHashMap<>();

  private final StorageLocationSelectorStrategy strategy;

  private final IndexIO indexIO;

  private final StorageLoadingThreadPool virtualStorageLoadingThreadPool;
  private ExecutorService loadOnBootstrapExec = null;
  private ExecutorService loadOnDownloadExec = null;

  @Inject
  public SegmentLocalCacheManager(
      List<StorageLocation> locations,
      SegmentLoaderConfig config,
      StorageLoadingThreadPool virtualStorageLoadingThreadPool,
      @Nonnull StorageLocationSelectorStrategy strategy,
      IndexIO indexIO,
      @Json ObjectMapper mapper
  )
  {
    this.locations = locations;
    this.config = config;
    this.virtualStorageLoadingThreadPool = virtualStorageLoadingThreadPool;
    this.strategy = strategy;
    this.indexIO = indexIO;
    this.jsonMapper = mapper;

    log.info("Using storage location strategy[%s].", this.strategy.getClass().getSimpleName());

    if (config.isVirtualStorage()) {
      if (config.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload() > 0) {
        throw DruidException.defensive(
            "Invalid configuration: virtualStorage is incompatible with numThreadsToLoadSegmentsIntoPageCacheOnDownload");
      }
      if (config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap() > 0) {
        throw DruidException.defensive(
            "Invalid configuration: virtualStorage is incompatible with numThreadsToLoadSegmentsIntoPageCacheOnBootstrap");
      }
    } else {
      log.info(
          "Number of threads to load segments into page cache - on bootstrap: [%d], on download: [%d].",
          config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap(),
          config.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload()
      );

      if (config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap() > 0) {
        loadOnBootstrapExec = Execs.multiThreaded(
            config.getNumThreadsToLoadSegmentsIntoPageCacheOnBootstrap(),
            "Load-SegmentsIntoPageCacheOnBootstrap-%s"
        );
      }

      if (config.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload() > 0) {
        loadOnDownloadExec = Executors.newFixedThreadPool(
            config.getNumThreadsToLoadSegmentsIntoPageCacheOnDownload(),
            Execs.makeThreadFactory("LoadSegmentsIntoPageCacheOnDownload-%s")
        );
      }
    }
  }

  @Override
  public boolean canHandleSegments()
  {
    final boolean isLocationsValid = !(locations == null || locations.isEmpty());
    final boolean isLocationsConfigValid = !(config.getLocations() == null || config.getLocations().isEmpty());
    return isLocationsValid || isLocationsConfigValid;
  }

  @Override
  public boolean canLoadSegmentsOnDemand()
  {
    return config.isVirtualStorage();
  }

  @Override
  public boolean canLoadSegmentOnDemand(DataSegment dataSegment)
  {
    return config.isVirtualStorage();
  }

  @Override
  public List<DataSegment> getCachedSegments() throws IOException
  {
    if (!canHandleSegments()) {
      throw DruidException.defensive(
          "canHandleSegments() is false. getCachedSegments() must be invoked only when canHandleSegments() returns true."
      );
    }

    // clean up any dropping files
    for (StorageLocation location : locations) {
      File dropFiles = new File(location.getPath(), DROP_PATH);
      if (dropFiles.exists()) {
        final File[] dropping = dropFiles.listFiles();
        if (dropping != null) {
          log.debug("cleaning up[%s] segments in[%s]", dropping.length, dropFiles);
          for (File droppedFile : dropping) {
            try {
              FileUtils.deleteDirectory(droppedFile);
            }
            catch (Exception e) {
              log.warn(e, "Unable to remove dropped segment directory[%s]", droppedFile);
            }
          }
        }
      }
    }

    final ConcurrentLinkedQueue<DataSegment> cachedSegments = new ConcurrentLinkedQueue<>();
    final File[] segmentsToLoad = retrieveSegmentMetadataFiles();
    final CountDownLatch latch = new CountDownLatch(segmentsToLoad.length);

    // If there is no dedicated bootstrap executor, perform the loading sequentially on the current thread.
    final boolean isLoadingSegmentsSequentially = loadOnBootstrapExec == null;
    final ExecutorService executorService = isLoadingSegmentsSequentially
                                            ? MoreExecutors.newDirectExecutorService()
                                            : loadOnBootstrapExec;

    AtomicInteger ignoredFilesCounter = new AtomicInteger(0);

    Stopwatch stopwatch = Stopwatch.createStarted();
    log.info("Loading [%d] segments from disk to cache.", segmentsToLoad.length);

    for (File file : segmentsToLoad) {
      executorService.submit(() -> {
        try {
          addFilesToCachedSegments(file, ignoredFilesCounter, cachedSegments);
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to load segment from segment cache file.")
             .addData("file", file)
             .emit();
        }
        finally {
          latch.countDown();
        }
      });
    }

    try {
      latch.await();
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      log.noStackTrace().error(e, "Interrupted when trying to retrieve cached segment metadata files");
    }

    stopwatch.stop();
    log.info("Loaded [%d/%d] cached segments in [%d]ms.", cachedSegments.size(), segmentsToLoad.length, stopwatch.millisElapsed());

    if (isLoadingSegmentsSequentially) {
      // Shutdown the direct executor service we created previously in this method.
      executorService.shutdown();
    }

    if (ignoredFilesCounter.get() > 0) {
      log.makeAlert("Ignored misnamed segment cache files on startup.")
         .addData("numIgnored", ignoredFilesCounter.get())
         .emit();
    }

    return List.copyOf(cachedSegments);
  }

  private void addFilesToCachedSegments(
      File file,
      AtomicInteger ignored,
      ConcurrentLinkedQueue<DataSegment> cachedSegments
  ) throws IOException
  {
    final DataSegment segment = jsonMapper.readValue(file, DataSegment.class);
    if (!segment.getId().toString().equals(file.getName())) {
      log.warn("Ignoring cache file[%s] for segment[%s].", file.getPath(), segment.getId());
      ignored.incrementAndGet();
      return;
    }

    boolean removeInfo = true;

    final CompleteSegmentCacheEntry cacheEntry = new CompleteSegmentCacheEntry(segment);
    for (StorageLocation location : locations) {
      // check for migrate from old nested local storage path format
      final File legacyPath = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment, false));
      if (legacyPath.exists()) {
        final File destination = cacheEntry.toPotentialLocation(location.getPath());
        FileUtils.mkdirp(destination);
        final File[] oldFiles = legacyPath.listFiles();
        final File[] newFiles = destination.listFiles();
        // make sure old files exist and new files do not exist
        if (oldFiles != null && oldFiles.length > 0 && newFiles != null && newFiles.length == 0) {
          Files.move(legacyPath.toPath(), destination.toPath(), StandardCopyOption.ATOMIC_MOVE);
        }
        cleanupLegacyCacheLocation(location.getPath(), legacyPath);
      }

      // Partial-segment layout is signaled by a {targetFilename}.header file in the segment dir
      final File partialDir = cacheEntry.toPotentialLocation(location.getPath());
      if (partialDir.exists()
          && PartialSegmentCacheBootstrap.isPartialSegmentLayout(partialDir, IndexIO.V10_FILE_NAME)) {
        if (!config.isVirtualStoragePartialDownloadsEnabled()) {
          // Partial downloads are disabled but a partial-load layout (header + sparse containers) is on disk, e.g. the
          // operator toggled druid.segmentCache.virtualStoragePartialDownloadsEnabled off. The eager path can't serve
          // this layout, so reclaim it now and let the segment be re-downloaded in full on next load, rather than
          // reserving it and failing when a query later tries to acquire it (see acquireExistingSegment). Leave
          // removeInfo true so the segment is treated as uncached and re-loaded. Use the same move-then-delete as the
          // other cache-dir removals; safe without a segment lock here because bootstrap runs before the node serves,
          // so nothing concurrently mounts or drops this entry.
          log.info(
              "Deleting on-disk partial-load layout for segment[%s] in [%s] because partial downloads are disabled; "
              + "it will be re-loaded via full download on next access.",
              segment.getId(),
              partialDir
          );
          atomicMoveAndDeleteCacheEntryDirectory(partialDir);
          continue;
        }
        SegmentRangeReader rangeReader;
        try {
          rangeReader = tryOpenRangeReader(segment);
        }
        catch (Exception e) {
          log.warn(e, "Failed to open a range reader for partial segment[%s] during bootstrap", segment.getId());
          rangeReader = null;
        }
        if (rangeReader == null) {
          // Anomalous: a layout on disk means range reads worked when it was written, so this should not happen (the
          // loadSpec is now non-range-capable, or no longer converts to a known type). Reclaim it and let the segment
          // re-load fresh on next access rather than failing bootstrap or reserving an entry that could never fetch.
          // Leave removeInfo true so it's treated as uncached.
          log.warn(
              "On-disk partial-load layout for segment[%s] in [%s] has no usable range reader (this should not "
              + "happen); deleting it so bootstrap can continue.",
              segment.getId(),
              partialDir
          );
          atomicMoveAndDeleteCacheEntryDirectory(partialDir);
          continue;
        }
        removeInfo = false;
        try {
          PartialSegmentCacheBootstrap.reserveFromDisk(
              segment.getId(),
              partialDir,
              IndexIO.V10_FILE_NAME,
              List.of(),
              rangeReader,
              jsonMapper,
              virtualStorageLoadingThreadPool,
              location
          );
          cachedSegments.add(segment);
        }
        catch (Throwable t) {
          // Reservation failed (header missing, location full, etc.)
          log.warn(t, "Failed to reserve partial segment[%s] from disk; cold fetch on next access", segment.getId());
        }
        // do not fall through to 'complete' path since this was a partial
        continue;
      }

      if (cacheEntry.checkExists(location.getPath())) {
        removeInfo = false;
        final boolean reserveResult;
        if (config.isVirtualStorage()) {
          reserveResult = location.reserveWeak(cacheEntry);
        } else {
          reserveResult = location.reserve(cacheEntry);
        }
        if (!reserveResult) {
          log.makeAlert(
              "storage[%s:%,d] has more segments than it is allowed. Currently loading Segment[%s:%,d]. Please increase druid.segmentCache.locations maxSize param",
              location.getPath(),
              location.availableSizeBytes(),
              segment.getId(),
              segment.getSize()
          ).emit();
        }
        cachedSegments.add(segment);
      }
    }

    if (removeInfo) {
      final SegmentId segmentId = segment.getId();
      log.warn("Unable to find cache file for segment[%s]. Deleting lookup entry.", segmentId);
      removeInfoFile(segment);
    }
  }

  private File[] retrieveSegmentMetadataFiles() throws IOException
  {
    final File infoDir = getEffectiveInfoDir();
    FileUtils.mkdirp(infoDir);
    File[] files = infoDir.listFiles();
    return files == null ? new File[0] : files;
  }

  @Override
  public void storeInfoFile(final DataSegment segment) throws IOException
  {
    final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), segment.getId().toString());
    if (!segmentInfoCacheFile.exists()) {
      FileUtils.mkdirp(segmentInfoCacheFile.getParentFile());
      FileUtils.writeAtomically(
          segmentInfoCacheFile,
          out -> {
            jsonMapper.writeValue(out, segment);
            return null;
          }
      );
    }
  }

  @Override
  public void removeInfoFile(final DataSegment segment)
  {
    final SegmentCacheEntryIdentifier entryId = new SegmentCacheEntryIdentifier(segment.getId());
    boolean isCached = false;
    // defer deleting until the unmount operation of the cache entry, if possible, so that if the process stops before
    // the segment files are deleted, they can be properly managed on startup (since the info entry still exists)
    for (StorageLocation location : locations) {
      final SegmentCacheEntry entry = location.getCacheEntry(entryId);
      // gate isCached on isMounted() because an unmounted but reserved entry has no on-disk state worth deferring for;
      // we want isCached = true exactly when there is real cached state that the unmount hook should clean up.
      if (entry != null && entry.isMounted()) {
        entry.setOnUnmount(() -> deleteSegmentInfoFile(segment));
        isCached = true;
      }
    }

    // otherwise we are probably deleting for cleanup reasons, so try it anyway if it wasn't present in any location
    if (!isCached) {
      deleteSegmentInfoFile(segment);
    }
  }

  private void deleteSegmentInfoFile(DataSegment segment)
  {
    final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), segment.getId().toString());
    if (!segmentInfoCacheFile.delete()) {
      log.warn("Unable to delete cache file[%s] for segment[%s].", segmentInfoCacheFile, segment.getId());
    }
  }

  @Override
  public Optional<Segment> acquireCachedSegment(final SegmentId segmentId, final AcquireMode acquireMode)
  {
    // PARTIAL accepts a mounted-but-not-fully-downloaded entry; FULL (and PARTIAL when partial downloads are disabled)
    // requires a behaviorally fully-materialized entry.
    final boolean requireFullyDownloaded =
        acquireMode == AcquireMode.FULL || !config.isVirtualStoragePartialDownloadsEnabled();
    return acquireCachedInternal(segmentId, requireFullyDownloaded);
  }

  @Override
  public AcquireSegmentAction acquireSegment(final DataSegment dataSegment, final AcquireMode acquireMode)
  {
    // Partial-eligible segments route through the partial machinery: PARTIAL mounts lazily (header only, bundles load
    // on demand), FULL force-downloads everything up front. Partial-ineligible segments fall through to the eager path
    // below for both modes.
    final SegmentRangeReader rangeReader = tryOpenRangeReader(dataSegment);
    if (rangeReader != null) {
      return acquirePartialInternal(dataSegment, rangeReader, acquireMode == AcquireMode.FULL);
    }

    final SegmentCacheEntryIdentifier identifier = new SegmentCacheEntryIdentifier(dataSegment.getId());
    final AcquireSegmentAction acquireExisting = acquireExistingSegment(identifier);
    if (acquireExisting != null) {
      return acquireExisting;
    }

    if (!config.isVirtualStorage()) {
      return AcquireSegmentAction.missingSegment();
    }

    final ReferenceCountingLock lock = lock(dataSegment);
    synchronized (lock) {
      try {
        final AcquireSegmentAction retryAcquireExisting = acquireExistingSegment(identifier);
        if (retryAcquireExisting != null) {
          return retryAcquireExisting;
        }

        final Iterator<StorageLocation> iterator = strategy.getLocations();
        while (iterator.hasNext()) {
          final StorageLocation location = iterator.next();
          final StorageLocation.ReservationHold<CompleteSegmentCacheEntry> hold = location.addWeakReservationHold(
              identifier,
              () -> new CompleteSegmentCacheEntry(dataSegment)
          );
          try {
            if (hold != null) {
              // write the segment info file if it doesn't exist. this can happen if we are loading after a drop
              final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), dataSegment.getId().toString());
              if (!segmentInfoCacheFile.exists()) {
                FileUtils.mkdirp(getEffectiveInfoDir());
                FileUtils.writeAtomically(segmentInfoCacheFile, out -> {
                  jsonMapper.writeValue(out, dataSegment);
                  return null;
                });
                hold.getEntry().setOnUnmount(() -> deleteSegmentInfoFile(dataSegment));
              }

              return new AcquireSegmentAction(
                  makeOnDemandLoadSupplier(hold.getEntry(), location),
                  hold
              );
            }
          }
          catch (Throwable t) {
            throw CloseableUtils.closeAndWrapInCatch(t, hold);
          }
        }
        throw DruidException.forPersona(DruidException.Persona.USER)
                            .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                            .build(
                                "Unable to load segment[%s] on demand, ensure enough disk space has been allocated to load all segments involved in the query",
                                dataSegment.getId()
                            );
      }
      finally {
        unlock(dataSegment, lock);
      }
    }
  }

  /**
   * Shared implementation for {@link #acquireCachedSegment}. Walks the storage locations checking for existing entries.
   * <p>
   * When {@code requireFullyDownloaded} is {@code true} the entry must also report
   * {@link SegmentCacheEntry#isFullyDownloaded()} ({@link AcquireMode#FULL}), which never hands back an entry that
   * isn't behaviorally fully-materialized. When {@code false} a mounted entry is sufficient
   * ({@link AcquireMode#PARTIAL}'s lazy-mount contract), even if some bundles are still not yet on disk. In either case
   * {@link SegmentCacheEntry#acquireReference(Closeable)} composes the weak-entry hold into the returned segment's close
   * lifecycle.
   */
  private Optional<Segment> acquireCachedInternal(SegmentId segmentId, boolean requireFullyDownloaded)
  {
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segmentId);
    for (StorageLocation location : locations) {
      final SegmentCacheEntry staticEntry = location.getStaticCacheEntry(id);
      if (staticEntry != null) {
        if (staticEntry.isMounted() && (!requireFullyDownloaded || staticEntry.isFullyDownloaded())) {
          return staticEntry.acquireReference();
        }
        return Optional.empty();
      }
      if (!config.isVirtualStorage()) {
        continue;
      }
      final StorageLocation.ReservationHold<SegmentCacheEntry> hold = location.addWeakReservationHoldIfExists(id);
      if (hold != null) {
        final SegmentCacheEntry entry = hold.getEntry();
        if (entry.isMounted() && (!requireFullyDownloaded || entry.isFullyDownloaded())) {
          if (requireFullyDownloaded && entry instanceof PartialSegmentMetadataCacheEntry partial) {
            // A FULL reference on a partial segment must pin every bundle for the segment's lifetime (the sync cursor
            // factory requires isFullyDownloaded(), which a metadata-only hold can't guarantee under SIEVE eviction).
            // acquireFullReference folds `hold` into the segment close on success, or closes it and returns empty when
            // a bundle is no longer resident — in which case we fall through to the downloading acquireSegment path.
            return partial.acquireFullReference(hold);
          }
          return entry.acquireReference(hold);
        }
        hold.close();
        return Optional.empty();
      }
    }
    return Optional.empty();
  }

  /**
   * Shared scaffolding for the partial-eligible branch of {@link #acquireSegment}. {@code fullDownload=false} powers
   * {@link AcquireMode#PARTIAL} (lazy mount, header bytes only; bundles mount on demand at query time);
   * {@code fullDownload=true} powers {@link AcquireMode#FULL} (mount +
   * {@link PartialSegmentFileMapperV10#ensureAllDownloaded} so the returned segment is fully-materialized).
   * <p>
   * Fast path: {@link #findExistingPartialWithHold} locates an existing entry across locations under a hold. If the
   * entry is already usable (mounted, and fully-downloaded when required), return an immediate-future action whose
   * {@code loadCleanup} is the hold (the supplier mints fresh segments per call, each with its own metadata
   * reference, so no separate cleanup is needed).
   * <p>
   * Slow path: under the per-segment lock, {@link #findOrReservePartial} reuses an existing not-yet-usable entry or
   * reserves a fresh weak one, and the action submits mount (+ optional ensureAllDownloaded) to
   * {@link #virtualStorageLoadingThreadPool} so callers that yield on the future never block a processing thread on
   * deep-storage I/O.
   */
  private AcquireSegmentAction acquirePartialInternal(
      DataSegment dataSegment,
      SegmentRangeReader rangeReader,
      boolean fullDownload
  )
  {
    final ReservedPartial existing = findExistingPartialWithHold(dataSegment.getId());
    if (existing != null) {
      final PartialSegmentMetadataCacheEntry partial = existing.metadata;
      if (!fullDownload && partial.isMounted()) {
        // Lazy/PARTIAL contract: hand back a metadata-anchored segment; bundles mount on demand at query time via the
        // async cursor factory, so a metadata-only hold is correct here. The full-download fast path is intentionally
        // omitted: a FULL segment must hold every bundle for its lifetime (the sync cursor factory requires
        // isFullyDownloaded(), which a metadata-only hold can't guarantee under SIEVE eviction), so it goes through
        // the slow path's bundle-holding + ensureAllDownloaded dance. The resident full case is already served without
        // an executor hop upstream by acquireCachedSegment -> acquireCachedInternal -> acquireFullReference.
        return new AcquireSegmentAction(
            () -> Futures.immediateFuture(AcquireSegmentResult.cached(partial::acquireReference)),
            existing.hold
        );
      }
      // Entry exists but isn't usable on the fast path (not mounted, or a full download is required). Release the
      // fast-path hold and let the slow path re-find with a fresh hold and drive mount on the executor.
      CloseableUtils.closeAndSuppressExceptions(existing.hold, ignored -> {});
    }

    final ReferenceCountingLock lock = lock(dataSegment);
    synchronized (lock) {
      try {
        // Find an existing entry (in any state: mounted, not-yet-mounted, partially-downloaded) under a cache hold,
        // or reserve a fresh weak entry with a hold if none exists. The metadata entry's mount-future dedup makes a
        // no-op cheap when the entry is already mounted (the typical re-find case after the fast path closed its hold).
        final ReservedPartial reserved = findOrReservePartial(dataSegment, rangeReader);
        // holdHolder holds the metadata reservation hold immediately; the full-download path adds a hold for every
        // bundle it mounts, all released together when the AcquireSegmentAction closes.
        final HoldHolder holdHolder = new HoldHolder(reserved.hold);
        return new AcquireSegmentAction(
            // Memoized so repeat getSegmentFuture() calls return the same future rather than scheduling duplicate
            // executor tasks. The entry's own mount-future dedup would prevent the actual work from being duplicated,
            // but the executor scheduling and timing capture would still be wasted.
            Suppliers.memoize(() -> {
              // Capture submit time on first invocation of getSegmentFuture(), so waitTime measures the queue delay
              // until the executor picks up the task. loadTime then covers mount (+ ensureAllDownloaded for the
              // full-download path).
              final long submitNanos = System.nanoTime();
              return virtualStorageLoadingThreadPool.getExecutorService().submit(() -> {
                // The executor bounds concurrency itself (permit acquired inside the task on the worker thread), so
                // waitNanos measures both the queue delay and any permit wait until this task actually starts.
                final long taskStartNanos = System.nanoTime();
                final long waitNanos = taskStartNanos - submitNanos;
                final boolean wasMounted = reserved.metadata.isMounted();
                // mount() is idempotent via PartialSegmentMetadataCacheEntry's mount-future dedup; already-mounted
                // returns immediately, a concurrent mount is awaited, a fresh entry is mounted. The weak entry's
                // hold-release runnable removes a never-mounted entry from the cache when our loadCleanup hold
                // closes, so no explicit rollback is needed on failure.
                try {
                  reserved.metadata.mount(reserved.location);
                }
                catch (IOException e) {
                  throw DruidException.defensive(
                      e,
                      "Failed to mount partial metadata for segment[%s]",
                      dataSegment.getId()
                  );
                }
                // Pin the metadata across the rest of the task
                final Closeable taskMetadataRef;
                try {
                  taskMetadataRef = reserved.metadata.acquireMetadataReference();
                }
                catch (DruidException raceLost) {
                  throw DruidException.defensive(
                      raceLost,
                      "Partial metadata for segment[%s] was dropped before %s task could complete",
                      dataSegment.getId(),
                      fullDownload ? "full-download" : "lazy mount"
                  );
                }
                try {
                  final PartialSegmentFileMapperV10 mapper = reserved.metadata.getFileMapper();
                  final long loadSizeBytes;
                  if (fullDownload) {
                    // Delta of internal-file bytes downloaded by this task
                    final long downloadedBefore = mapper.getDownloadedBytes();
                    // Mount every bundle so the containers it owns are reserved on the location
                    for (String bundleName : PartialSegmentBundleCacheEntry.bundleNames(mapper)) {
                      holdHolder.add(reserved.metadata.getBundleAcquirer().acquire(bundleName));
                    }
                    mapper.ensureAllDownloaded();
                    loadSizeBytes = mapper.getDownloadedBytes() - downloadedBefore;
                  } else {
                    // Lazy mount: the header bytes when this task caused the mount; 0 when the entry was already
                    // mounted (a concurrent acquirer or earlier query did the load).
                    loadSizeBytes = wasMounted ? 0L : mapper.getOnDiskHeaderSize();
                  }
                  final long loadNanos = System.nanoTime() - taskStartNanos;
                  return new AcquireSegmentResult(
                      reserved.metadata::acquireReference,
                      loadSizeBytes,
                      waitNanos,
                      loadNanos
                  );
                }
                finally {
                  CloseableUtils.closeAndSuppressExceptions(taskMetadataRef, ignored -> {});
                }
              });
            }),
            holdHolder
        );
      }
      finally {
        unlock(dataSegment, lock);
      }
    }
  }

  /**
   * Locate an existing partial metadata entry across storage locations and attach a eviction-protective hold to it.
   * Returns {@code null} when no entry exists at any location. Race-safe (the hold prevents eviction from picking
   * the entry between this lookup and the caller's subsequent use). Caller must close the returned hold (typically
   * via {@link AcquireSegmentAction#loadCleanup} or by closing it directly when falling through to a reserve path).
   */
  @Nullable
  private ReservedPartial findExistingPartialWithHold(SegmentId segmentId)
  {
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segmentId);
    for (StorageLocation location : locations) {
      final StorageLocation.ReservationHold<SegmentCacheEntry> hold = location.addWeakReservationHoldIfExists(id);
      if (hold == null) {
        continue;
      }
      try {
        if (hold.getEntry() instanceof PartialSegmentMetadataCacheEntry partial) {
          return new ReservedPartial(partial, location, hold);
        }
        throw DruidException.defensive(
            "Unexpected non-partial cache entry[%s] at id[%s] on location[%s]",
            hold.getEntry().getClass().getSimpleName(),
            id,
            location.getPath()
        );
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, hold);
      }
    }
    return null;
  }

  /**
   * If any storage location has a non-{@link PartialSegmentMetadataCacheEntry} cache entry at {@code segmentId},
   * attempt to evict it via {@link StorageLocation#removeUnheldWeakEntry}. This handles the config-flip scenario
   * where a segment with a {@link PartialLoadSpec} wrapper was previously queried while
   * {@code virtualStoragePartialDownloadsEnabled=false} ({@link #acquireSegment} created a
   * {@link CompleteSegmentCacheEntry} at this id, and a subsequent partial-load rule cannot reserve here without
   * evicting the stale entry first).
   * <p>
   * {@link StorageLocation#removeUnheldWeakEntry} is a no-op when the entry is held (in-flight query), so we
   * detect via {@link StorageLocation#getCacheEntry} whether eviction actually happened. If not, throw a retryable
   * {@link SegmentLoadingException}: the coordinator's load queue retries on the next sync, and by then the query
   * should have released the hold and eviction will succeed.
   */
  private void evictStaleNonPartialWeakEntry(SegmentId segmentId) throws SegmentLoadingException
  {
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segmentId);
    for (StorageLocation location : locations) {
      final CacheEntry entry = location.getCacheEntry(id);
      if (entry == null || entry instanceof PartialSegmentMetadataCacheEntry) {
        continue;
      }
      location.removeUnheldWeakEntry(id);
      final CacheEntry stillThere = location.getCacheEntry(id);
      if (stillThere != null) {
        throw new SegmentLoadingException(
            "Stale non-partial cache entry[%s] at id[%s] on location[%s] blocks partial-load reservation and is "
            + "currently held; the coordinator's load queue will retry",
            stillThere.getClass().getSimpleName(),
            id,
            location.getPath()
        );
      }
      log.info(
          "Evicted stale non-partial cache entry[%s] at location[%s] to make room for partial-load rule on "
          + "segment[%s]",
          entry.getClass().getSimpleName(),
          location.getPath(),
          segmentId
      );
    }
  }

  /**
   * Try to open a range reader for the given segment's {@link LoadSpec}; returns {@code null} when the backend
   * doesn't support range reads (e.g. zipped storage), or when partial downloads are disabled via
   * {@link SegmentLoaderConfig#isVirtualStoragePartialDownloadsEnabled}. Used to gate the partial-eligible branch of
   * {@link #acquireSegment}; a null result causes that call site to fall through to the eager extraction path.
   */
  @Nullable
  private SegmentRangeReader tryOpenRangeReader(DataSegment dataSegment)
  {
    if (!config.isVirtualStoragePartialDownloadsEnabled()) {
      return null;
    }
    try {
      final LoadSpec loadSpec = jsonMapper.convertValue(dataSegment.getLoadSpec(), LoadSpec.class);
      return loadSpec.openRangeReader();
    }
    catch (IOException e) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build(e, "Failed to open range reader for segment[%s]", dataSegment.getId());
    }
  }

  /**
   * Synchronously reserve a partial metadata cache entry as a weak entry on the first storage location with capacity,
   * placing a eviction-protective hold so it can't be evicted before the caller's {@link AcquireSegmentAction} is
   * closed. Writes the segment info file so bootstrap can see this segment on restart.
   * <p>
   * Uses {@link StorageLocation#addWeakReservationHold} which is atomic: it returns an existing entry's hold if one
   * raced in (rare since callers hold the per-segment lock), or installs the freshly-built entry and returns its
   * hold. On info-file write failure, releases the hold (which removes the never-mounted weak entry) and propagates.
   * Throws CAPACITY_EXCEEDED if no location accepts the reservation.
   */
  private ReservedPartial reservePartial(DataSegment dataSegment, SegmentRangeReader rangeReader)
  {
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(dataSegment.getId());
    final Iterator<StorageLocation> iterator = strategy.getLocations();
    while (iterator.hasNext()) {
      final StorageLocation location = iterator.next();
      final File partialDir = new File(location.getPath(), dataSegment.getId().toString());
      try {
        FileUtils.mkdirp(partialDir);
      }
      catch (IOException e) {
        // Location is unwritable, fall through to next location rather than failing the whole reservation
        log.warn(
            e,
            "Failed to create partial cache dir on location[%s] for segment[%s]; trying next location",
            location.getPath(),
            dataSegment.getId()
        );
        continue;
      }
      final StorageLocation.ReservationHold<SegmentCacheEntry> hold = location.addWeakReservationHold(
          id,
          () -> new PartialSegmentMetadataCacheEntry(
              dataSegment.getId(),
              partialDir,
              IndexIO.V10_FILE_NAME,
              List.of(),
              rangeReader,
              jsonMapper,
              virtualStorageLoadingThreadPool,
              config.getVirtualStorageMetadataReservationEstimate()
          )
      );
      if (hold == null) {
        atomicMoveAndDeleteCacheEntryDirectory(partialDir);
        continue;
      }
      try {
        if (!(hold.getEntry() instanceof PartialSegmentMetadataCacheEntry partial)) {
          throw DruidException.defensive(
              "Unexpected non-partial cache entry[%s] at id[%s] on location[%s]",
              hold.getEntry().getClass().getSimpleName(),
              id,
              location.getPath()
          );
        }
        // Unconditionally write the info file with the incoming DataSegment. An existing info file at this path
        // may carry a STALE loadSpec. Skipping the write when the file exists would preserve that stale wrapper on
        // disk, and a subsequent bootstrap would restore the segment via the wrong path (non-partial) or drift out of
        // sync with the coordinator's applied rule. writeAtomically does the file-rename dance so a concurrent read
        // sees either the old or new complete file, never a partial write.
        final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), dataSegment.getId().toString());
        FileUtils.mkdirp(getEffectiveInfoDir());
        FileUtils.writeAtomically(segmentInfoCacheFile, out -> {
          jsonMapper.writeValue(out, dataSegment);
          return null;
        });
        partial.setOnUnmount(() -> deleteSegmentInfoFile(dataSegment));
        return new ReservedPartial(partial, location, hold);
      }
      catch (Throwable t) {
        // Close the hold (removing the never-mounted weak entry), then nuke the on-disk dir.
        try {
          throw CloseableUtils.closeAndWrapInCatch(t, hold);
        }
        finally {
          atomicMoveAndDeleteCacheEntryDirectory(partialDir);
        }
      }
    }
    throw DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                        .build(
                            "Unable to reserve partial metadata for segment[%s]; ensure enough disk space has been allocated",
                            dataSegment.getId()
                        );
  }

  /**
   * Return a handle to a partial metadata entry for the given segment, paired with a eviction-protective hold: either
   * an existing entry (in any mount state, located by {@link #findExistingPartialWithHold}), or a fresh weak
   * reservation from {@link #reservePartial}. The caller is expected to drive
   * {@link PartialSegmentMetadataCacheEntry#mount} on the returned handle inside the
   * {@link AcquireSegmentAction}'s future; mount is idempotent via its mount-future dedup, so an already-mounted
   * entry's mount call is cheap. The hold rides in the action's {@code loadCleanup} and is released when the
   * action closes.
   */
  private ReservedPartial findOrReservePartial(DataSegment dataSegment, SegmentRangeReader rangeReader)
  {
    final ReservedPartial existing = findExistingPartialWithHold(dataSegment.getId());
    if (existing != null) {
      return existing;
    }
    return reservePartial(dataSegment, rangeReader);
  }

  /**
   * Pairing of a partial metadata entry (either pre-existing, discovered via {@link #findExistingPartialWithHold}, or
   * freshly reserved by {@link #reservePartial}) with the storage location it lives on plus a eviction-protective hold.
   * The hold rides in the {@link AcquireSegmentAction#loadCleanup} so the entry is protected from eviction across
   * the action's lifetime; closing the action releases the hold.
   */
  private record ReservedPartial(
      PartialSegmentMetadataCacheEntry metadata,
      StorageLocation location,
      StorageLocation.ReservationHold<SegmentCacheEntry> hold
  )
  {
  }

  /**
   * Apply (or reconcile) a partial-load rule for a segment coming through {@link #load}. The rule state lives on the
   * {@link PartialSegmentMetadataCacheEntry} itself; this method just drives it:
   * <ol>
   *   <li>Materialize the wrapper's {@link PartialLoadSpec} and open a range reader over the backend. If the backend
   *       can't do range reads, clear any prior rule on the segment and return; the segment falls back to the
   *       weak-full-load path at query time.</li>
   *   <li>Acquire (or reserve) the metadata entry via the shared on-demand path ({@link #findOrReservePartial}),
   *       take a transient eviction-protective hold across the rest of the flow.</li>
   *   <li>Mount the metadata entry (idempotent).</li>
   *   <li>Compute the rule's selected bundle names from the wrapper + parsed segment metadata, and call
   *       {@link PartialSegmentMetadataCacheEntry#applyRule} which installs the metadata self-hold and takes
   *       holds on any already-registered selected bundles.</li>
   *   <li>Kick off async eager downloads on the loading pool for any selected bundle not yet linked. Each async task
   *       drives {@link PartialSegmentMetadataCacheEntry#ensureBundleResidentForRule}: its bundle mount will call
   *       {@link PartialSegmentMetadataCacheEntry#registerBundle}, which acquires the rule-hold on that bundle.</li>
   *   <li>Release the transient hold; the metadata's self-hold keeps the entry resident under the applied rule.</li>
   * </ol>
   */
  private void loadPartial(DataSegment dataSegment) throws SegmentLoadingException
  {
    final PartialLoadSpec wrapper = materializePartialLoadSpec(dataSegment);
    final SegmentRangeReader rangeReader = openPartialRangeReader(dataSegment, wrapper);
    final ReferenceCountingLock lock = lock(dataSegment);
    synchronized (lock) {
      try {
        // If a stale non-partial cache entry sits at this segment id (a CompleteSegmentCacheEntry created by a prior
        // acquireSegment while virtualStoragePartialDownloadsEnabled=false, for example), evict it before any
        // partial-entry lookup or reservation; otherwise findExistingPartialWithHold's defensive type-check would
        // throw, and reservePartial's addWeakReservationHold would land on the incompatible entry. If the stale
        // entry is currently held (in-flight query), this throws a retryable SegmentLoadingException; the
        // coordinator's load queue retries on next sync, and by then the query should have released.
        evictStaleNonPartialWeakEntry(dataSegment.getId());

        if (rangeReader == null) {
          // Backend doesn't support range reads (e.g. zipped deep storage). The rule can't be honored as a partial
          // load; clear any prior rule so the segment falls through to the ordinary weak-full-load path at query
          // time.
          final ReservedPartial existing = findExistingPartialWithHold(dataSegment.getId());
          if (existing != null) {
            try {
              existing.metadata().clearRule();
              log.warn(
                  "Backend for segment[%s] does not support range reads; released rule[fingerprint=%s], segment "
                  + "will fall back to weak full-load at query time",
                  dataSegment.getId(),
                  wrapper.getFingerprint()
              );
            }
            finally {
              CloseableUtils.closeAndSuppressExceptions(
                  existing.hold(),
                  t -> log.warn(t, "Failed to release transient hold on partial metadata for segment[%s]",
                                dataSegment.getId())
              );
            }
          } else {
            // No prior entry AND range-reader is null on this fresh load: the coordinator asked for a partial rule
            // this historical can't honor. Log so operators can debug "why isn't my rule applied on historical X".
            log.warn(
                "Backend for segment[%s] does not support range reads and no prior partial entry exists; rule"
                + "[fingerprint=%s] cannot be applied on this historical",
                dataSegment.getId(),
                wrapper.getFingerprint()
            );
          }
          return;
        }

        final ReservedPartial reserved = findOrReservePartial(dataSegment, rangeReader);
        try {
          final PartialSegmentMetadataCacheEntry metadata = reserved.metadata();
          try {
            metadata.mount(reserved.location());
          }
          catch (IOException e) {
            throw new SegmentLoadingException(
                e,
                "Failed to mount partial metadata for segment[%s]",
                dataSegment.getId()
            );
          }
          final PartialSegmentFileMapperV10 mapper = metadata.getFileMapper();
          if (mapper == null) {
            throw DruidException.defensive(
                "Partial metadata for segment[%s] mounted without a file mapper",
                dataSegment.getId()
            );
          }
          // Register the info-file cleanup hook BEFORE anything that could throw. Any throw between mount and
          // awaitEagerDownloadsOrClearRule leaves the metadata entry weak-reserved with the hook attached; when
          // cache eventually reclaims, the info file gets deleted along with the entry.
          metadata.setOnUnmount(() -> deleteSegmentInfoFile(dataSegment));
          final Set<String> selected = Set.copyOf(
              wrapper.getSelectedBundleNames(dataSegment, mapper.getSegmentFileMetadata())
          );
          final String priorFingerprint = metadata.getRuleFingerprint();
          metadata.applyRule(wrapper.getFingerprint(), selected);
          if (priorFingerprint != null && !priorFingerprint.equals(wrapper.getFingerprint())) {
            log.info(
                "Reconciled partial-load rule for segment[%s]: fingerprint transitioned [%s] → [%s]",
                dataSegment.getId(),
                priorFingerprint,
                wrapper.getFingerprint()
            );
          }
          // Block until every eager download completes so the announcement fingerprint reflects reality: any failure
          // clears the rule state (releasing self-hold + all bundle rule-holds) and propagates as a load failure so
          // the coordinator's load queue can retry on its next sync. The announced fingerprint == "rule fully
          // realized" contract stays intact.
          awaitEagerDownloadsOrClearRule(dataSegment, metadata, selected);
        }
        finally {
          CloseableUtils.closeAndSuppressExceptions(
              reserved.hold(),
              t -> log.warn(t, "Failed to release transient hold on partial metadata for segment[%s]",
                            dataSegment.getId())
          );
        }
      }
      finally {
        unlock(dataSegment, lock);
      }
    }
  }

  /**
   * Submit eager-download tasks to the loading pool for every rule-selected bundle not yet registered with
   * {@code metadata}, then block until every task completes. On any failure the rule state is cleared
   * and a {@link SegmentLoadingException} is thrown so the caller treats the load as failed and retries.
   */
  private void awaitEagerDownloadsOrClearRule(
      DataSegment dataSegment,
      PartialSegmentMetadataCacheEntry metadata,
      Set<String> selected
  ) throws SegmentLoadingException
  {
    final List<Future<?>> pending = new ArrayList<>();
    Throwable firstFailure = null;
    try {
      for (String bundleName : selected) {
        // Skip only when the bundle is BOTH rule-held AND fully downloaded (every container's files present on disk).
        if (metadata.isBundleRuleHeld(bundleName) && metadata.isBundleFullyDownloaded(bundleName)) {
          continue;
        }
        pending.add(virtualStorageLoadingThreadPool.getExecutorService().submit(() -> {
          metadata.ensureBundleResidentForRule(bundleName);
          return null;
        }));
      }
    }
    catch (RejectedExecutionException e) {
      // Pool shutting down or queue saturated. Any already-submitted futures may still run; track the rejection as
      // firstFailure and fall through to the await/cancel + clearRule path so state is always cleaned up before we
      // throw.
      firstFailure = e;
    }

    for (Future<?> f : pending) {
      if (firstFailure != null) {
        // Cancel remaining futures on any failure so we don't wait for tasks whose result we no longer intend to
        // commit. cancel(false) — NOT cancel(true) — because a running task is mid-NIO/FileChannel read via
        // mapper.ensureBundleDownloaded, and Thread.interrupt() on an in-flight NIO op raises
        // ClosedByInterruptException that closes the mapper's shared underlying FD, breaking still-mounted bundles
        // for other queries reading the same segment. Not-yet-started tasks are marked CANCELLED; running tasks
        // continue on their own timeline, observe ruleSelectedBundleNames == {} after clearRule, and release
        // their transient bundleAcquirer holds without acquiring rule-holds.
        f.cancel(false);
        // Still drain the future so an ExecutionException from a task that completed-with-failure BEFORE we
        // canceled is logged instead of silently swallowed.
        try {
          f.get();
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        catch (ExecutionException e) {
          final Throwable cause = e.getCause() != null ? e.getCause() : e;
          log.warn(
              cause,
              "Eager download of a rule-selected bundle for segment[%s] failed after the primary failure",
              dataSegment.getId()
          );
        }
        catch (CancellationException e) {
          // task never started; expected on the cancel path
        }
        continue;
      }
      try {
        f.get();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        firstFailure = e;
        // Signal cancel WITHOUT interrupt — see cancel(false) comment above; interrupting mid-NIO closes the
        // shared mapper FD.
        f.cancel(false);
      }
      catch (ExecutionException e) {
        firstFailure = e.getCause() != null ? e.getCause() : e;
      }
      catch (CancellationException e) {
        // Defensive: shouldn't reach here on the first-failure branch (we only cancel after firstFailure is set),
        // but if it does, treat it uniformly.
        firstFailure = e;
      }
    }

    if (firstFailure != null) {
      // Any late-completing pool task that still succeeds after we've cleared the rule will call registerBundle →
      // observing ruleSelectedBundleNames == {} and skipping the rule-hold acquire.
      metadata.clearRule();
      throw new SegmentLoadingException(
          firstFailure,
          "Failed eager download of rule-selected bundles for segment[%s]; cleared partial-load rule",
          dataSegment.getId()
      );
    }
  }

  /**
   * Materialize the segment's wrapped load spec to a {@link PartialLoadSpec}.
   */
  private PartialLoadSpec materializePartialLoadSpec(DataSegment dataSegment) throws SegmentLoadingException
  {
    final LoadSpec materializedLoadSpec;
    try {
      materializedLoadSpec = jsonMapper.convertValue(dataSegment.getLoadSpec(), LoadSpec.class);
    }
    catch (Exception e) {
      throw new SegmentLoadingException(
          e,
          "Failed to materialize partial load spec for segment[%s]",
          dataSegment.getId()
      );
    }
    if (!(materializedLoadSpec instanceof PartialLoadSpec wrapper)) {
      throw DruidException.defensive(
          "Segment[%s] load spec was detected as partial but materialized to non-partial type[%s]",
          dataSegment.getId(),
          materializedLoadSpec.getClass().getSimpleName()
      );
    }
    return wrapper;
  }

  /**
   * Open a range reader for the segment's deep storage via the wrapper's {@link PartialLoadSpec#openRangeReader}
   * (which delegates to the inner load spec), wrapping any {@link IOException} as a {@link SegmentLoadingException}.
   * Returns {@code null} if the backend doesn't support range reads.
   */
  @Nullable
  private SegmentRangeReader openPartialRangeReader(DataSegment dataSegment, PartialLoadSpec wrapper)
      throws SegmentLoadingException
  {
    try {
      return wrapper.openRangeReader();
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Failed to open range reader for segment[%s]", dataSegment.getId());
    }
  }

  /**
   * The fingerprint of the currently-applied partial-load rule for {@code segmentId}, or {@code null} if no rule is
   * currently applied (or the segment has no partial metadata entry (e.g. it was eager-loaded as a complete cache
   * entry). Test-only. Delegates to {@link PartialSegmentMetadataCacheEntry#getRuleFingerprint} on the segment's
   * cache entry.
   */
  @VisibleForTesting
  @Nullable
  String getRuleFingerprintForSegment(SegmentId segmentId)
  {
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segmentId);
    for (StorageLocation location : locations) {
      final CacheEntry entry = location.getCacheEntry(id);
      if (entry instanceof PartialSegmentMetadataCacheEntry partial) {
        return partial.getRuleFingerprint();
      }
    }
    return null;
  }

  @Nullable
  private AcquireSegmentAction acquireExistingSegment(SegmentCacheEntryIdentifier identifier)
  {
    final Closer safetyNet = Closer.create();
    for (StorageLocation location : locations) {
      try {
        final StorageLocation.ReservationHold<SegmentCacheEntry> hold = safetyNet.register(
            location.addWeakReservationHoldIfExists(identifier)
        );
        if (hold != null) {
          if (!(hold.getEntry() instanceof CompleteSegmentCacheEntry complete)) {
            // The eager (complete) acquire path found a non-complete entry under this id. Defensive backstop: when
            // partial downloads are disabled, getCachedSegments now deletes any on-disk partial layout at bootstrap
            // rather than reserving it, so a partial entry should not exist on this path. If one somehow does (e.g. a
            // bootstrap delete failed), surface a clear operator error rather than a ClassCastException.
            throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                                .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                                .build(
                                    "Segment[%s] has partial-load cache state on disk but partial downloads are "
                                    + "disabled; clear the segment cache directory or re-enable "
                                    + "druid.segmentCache.virtualStoragePartialDownloadsEnabled",
                                    identifier
                                );
          }
          if (complete.isMounted()) {
            return new AcquireSegmentAction(
                () -> Futures.immediateFuture(AcquireSegmentResult.cached(complete.referenceProvider)),
                hold
            );
          } else {
            // go ahead and mount it, someone else is probably trying this as well, but mount is done under a segment
            // lock and is a no-op if already mounted, and if we win we need it to be mounted
            return new AcquireSegmentAction(
                makeOnDemandLoadSupplier(complete, location),
                hold
            );
          }
        }
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, safetyNet);
      }
    }
    return null;
  }

  @Override
  public void load(final DataSegment dataSegment) throws SegmentLoadingException
  {
    if (config.isVirtualStorage()) {
      if (config.isVirtualStorageEphemeral()) {
        throw DruidException.defensive(
            "load() should not be called when virtualStorageIsEphemeral is true"
        );
      }
      // Partial-load-rule routing: a wrapped load spec carrying a fingerprint + per-spec selection (cluster groups,
      // projections, ...) means the coordinator wants this segment loaded with rule holds pinning the selected
      // bundles + metadata. Install the holds via loadPartial; other segments (no wrapper, or partials disabled)
      // take the existing weak/no-op path below.
      if (config.isVirtualStoragePartialDownloadsEnabled()
          && PartialLoadSpec.detectPartialLoadSpec(dataSegment.getLoadSpec())) {
        loadPartial(dataSegment);
        return;
      }
      // virtual storage doesn't do anything with loading immediately, but check to see if the segment is already cached
      // and if so, clear out the onUnmount action
      final ReferenceCountingLock lock = lock(dataSegment);
      synchronized (lock) {
        try {
          final SegmentCacheEntryIdentifier cacheEntryIdentifier = new SegmentCacheEntryIdentifier(dataSegment.getId());
          for (StorageLocation location : locations) {
            final SegmentCacheEntry cacheEntry = location.getCacheEntry(cacheEntryIdentifier);
            if (cacheEntry != null) {
              cacheEntry.setOnUnmount(null);
            }
          }
        }
        finally {
          unlock(dataSegment, lock);
        }
      }
      return;
    }
    final CompleteSegmentCacheEntry cacheEntry = new CompleteSegmentCacheEntry(dataSegment);
    final ReferenceCountingLock lock = lock(dataSegment);
    synchronized (lock) {
      try {
        final CompleteSegmentCacheEntry entry = assignLocationAndMount(cacheEntry, SegmentLazyLoadFailCallback.NOOP);
        if (loadOnDownloadExec != null) {
          loadOnDownloadExec.submit(entry::loadIntoPageCache);
        }
      }
      finally {
        unlock(dataSegment, lock);
      }
    }
  }

  @Override
  public void bootstrap(
      final DataSegment dataSegment,
      final SegmentLazyLoadFailCallback loadFailed
  ) throws SegmentLoadingException
  {
    if (config.isVirtualStorage()) {
      if (config.isVirtualStorageEphemeral()) {
        throw DruidException.defensive(
            "bootstrap() should not be called when virtualStorageIsEphemeral is true"
        );
      }
      // during bootstrap, check if the segment exists in a location and mount it; getCachedSegments already
      // did the reserving for us
      final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(dataSegment.getId());
      final ReferenceCountingLock lock = lock(dataSegment);
      synchronized (lock) {
        try {
          for (StorageLocation location : locations) {
            final CacheEntry entry = location.getCacheEntry(id);
            if (entry == null) {
              continue;
            }
            if (entry instanceof CompleteSegmentCacheEntry complete) {
              complete.lazyLoadCallback = loadFailed;
              complete.setOnUnmount(null);
              complete.mount(location);
            } else if (entry instanceof PartialSegmentMetadataCacheEntry partial) {
              try {
                partial.mount(location);
              }
              catch (IOException e) {
                throw new SegmentLoadingException(
                    e,
                    "Failed to mount partial metadata for segment[%s]",
                    dataSegment.getId()
                );
              }
              // If the persisted DataSegment's loadSpec is a partial-load-rule wrapper, reapply the rule so the
              // segment continues to be protected from eviction across the restart.
              //
              // Strict failure handling: reapplyRuleFromInfoFile throws on eager-download failure (and clears the
              // rule state before throwing). The exception propagates up through SegmentManager.loadSegmentOnBootstrap,
              // which calls cacheManager.drop and rethrows, so SegmentCacheBootstrapper marks the segment as failed
              // and skips its announcement.
              if (config.isVirtualStoragePartialDownloadsEnabled()
                  && PartialLoadSpec.detectPartialLoadSpec(dataSegment.getLoadSpec())) {
                reapplyRuleFromInfoFile(dataSegment, partial);
              }
            } else {
              throw DruidException.defensive(
                  "Unexpected cache entry type[%s] for segment[%s] during bootstrap",
                  entry.getClass().getSimpleName(),
                  dataSegment.getId()
              );
            }
          }
        }
        finally {
          unlock(dataSegment, lock);
        }
      }
      return;
    }
    final ReferenceCountingLock lock = lock(dataSegment);
    synchronized (lock) {
      try {
        final CompleteSegmentCacheEntry entry = assignLocationAndMount(new CompleteSegmentCacheEntry(dataSegment), loadFailed);
        if (loadOnBootstrapExec != null) {
          loadOnBootstrapExec.submit(entry::loadIntoPageCache);
        }
      }
      finally {
        unlock(dataSegment, lock);
      }
    }
  }

  @Nullable
  @Override
  public File getSegmentFiles(final DataSegment segment)
  {
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segment.getId());
    final ReferenceCountingLock lock = lock(segment);
    synchronized (lock) {
      try {
        for (StorageLocation location : locations) {
          final CacheEntry entry = location.getCacheEntry(id);
          if (entry == null) {
            continue;
          }
          if (entry instanceof CompleteSegmentCacheEntry complete) {
            return complete.storageDir;
          }
          // The only caller of SegmentCacheManager#getSegmentFiles is DruidSegmentInputEntity (native batch ingest),
          // which constructs its cache manager with virtualStorage=false, so a partial entry showing up here is a
          // programming error, not a runtime configuration issue. Fail loudly rather than silently returning null,
          // which would lead to a confusing "missing segment file" error downstream.
          throw DruidException.defensive(
              "getSegmentFiles[%s] called on a partial-segment cache entry (type[%s]); this API is only supported "
              + "for complete cache entries (callers should not enable virtual storage on their cache manager)",
              segment.getId(),
              entry.getClass().getSimpleName()
          );
        }
      }
      finally {
        unlock(segment, lock);
      }
    }
    return null;
  }

  @Override
  public void drop(final DataSegment segment)
  {
    final ReferenceCountingLock lock = lock(segment);
    synchronized (lock) {
      try {
        // partial-load-rule cleanup: if the segment's cache entry is a partial metadata entry with an applied rule,
        // clear it
        final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segment.getId());
        for (StorageLocation location : locations) {
          final CacheEntry entry = location.getCacheEntry(id);
          if (entry instanceof PartialSegmentMetadataCacheEntry partial) {
            partial.clearRule();
            // Force synchronous cleanup: clearRule releases the self-hold, but the info-file-deletion hook only
            // fires on doActualUnmount which is triggered by the phaser hitting zero. removeUnheldWeakEntry
            // unlinks the weak entry from cache and terminates the phaser now, firing the hook (and the mapper
            // teardown) on the caller's thread. No-op if a concurrent query still holds the entry.
            location.removeUnheldWeakEntry(id);
          }
        }
        // full-segment drop path: release any {@link CompleteSegmentCacheEntry} still reserved.
        for (StorageLocation location : locations) {
          final CacheEntry entry = location.getCacheEntry(id);
          if (entry != null) {
            location.release(entry);
          }
        }
      }
      finally {
        unlock(segment, lock);
      }
    }
  }

  /**
   * Reapply the persisted partial-load rule to a bootstrap-restored metadata entry. Reads the wrapper from the
   * segment's info-file {@code loadSpec}, resolves the selected bundle names against the just-parsed on-disk
   * metadata header, calls {@link PartialSegmentMetadataCacheEntry#applyRule}, then drives eager downloads for any
   * selected bundle that wasn't restored from disk (via {@link #awaitEagerDownloadsOrClearRule}). On failure the
   * exception marks the segment as failed → doesn't announce it → the coordinator's next sync re-issues load.
   */
  private void reapplyRuleFromInfoFile(DataSegment dataSegment, PartialSegmentMetadataCacheEntry partial)
      throws SegmentLoadingException
  {
    // evict any stale non-partial cache entry at this segment id before touching the partial state.
    // Bootstrap ordering today produces a partial entry for the passed-in `partial`, so this is expected
    // to be a no-op except during a config switch.
    evictStaleNonPartialWeakEntry(dataSegment.getId());

    final PartialLoadSpec wrapper = materializePartialLoadSpec(dataSegment);
    final PartialSegmentFileMapperV10 mapper = partial.getFileMapper();
    if (mapper == null) {
      throw DruidException.defensive(
          "Bootstrap-restored partial metadata for segment[%s] has no file mapper", dataSegment.getId()
      );
    }
    // Register the info-file cleanup hook BEFORE anything that could throw. reserveFromDisk does NOT install an
    // onUnmount hook on bootstrap-restored entries, so without this ordering a throw from getSelectedBundleNames
    // or applyRule would leave the entry with no cleanup path. Setting the hook first ensures every teardown path
    // deletes the info file consistently.
    partial.setOnUnmount(() -> deleteSegmentInfoFile(dataSegment));
    final Set<String> selected = Set.copyOf(
        wrapper.getSelectedBundleNames(dataSegment, mapper.getSegmentFileMetadata())
    );
    partial.applyRule(wrapper.getFingerprint(), selected);
    awaitEagerDownloadsOrClearRule(dataSegment, partial, selected);
  }

  @Override
  public void shutdownBootstrap()
  {
    if (loadOnBootstrapExec == null) {
      return;
    }
    loadOnBootstrapExec.shutdown();
  }

  @Override
  public void shutdown()
  {
    if (loadOnDownloadExec != null) {
      loadOnDownloadExec.shutdown();
    }
  }

  @VisibleForTesting
  public ConcurrentHashMap<DataSegment, ReferenceCountingLock> getSegmentLocks()
  {
    return segmentLocks;
  }

  @Override
  public List<StorageLocation> getLocations()
  {
    return locations;
  }

  @Override
  public StorageLoadingThreadPool getLoadingThreadPool()
  {
    return virtualStorageLoadingThreadPool;
  }

  /**
   * Checks whether a segment is already cached. This method does not confirm if the segment is actually mounted in
   * the location, or even that the segment files in some location are valid, just that some files exist in the
   * specified location
   */
  @VisibleForTesting
  boolean isSegmentCached(final DataSegment segment)
  {
    final CompleteSegmentCacheEntry cacheEntry = new CompleteSegmentCacheEntry(segment);
    for (StorageLocation location : locations) {
      if (cacheEntry.checkExists(location.getPath())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Testing use only please, any callers that want to do stuff with segments should use
   * {@link #acquireCachedSegment(SegmentId, AcquireMode)} or {@link #acquireSegment(DataSegment, AcquireMode)} instead.
   * Does not hold locks and so is not really safe to use while the cache manager is active
   */
  @VisibleForTesting
  @Nullable
  public ReferenceCountedSegmentProvider getSegmentReferenceProvider(DataSegment segment)
  {
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segment.getId());
    for (StorageLocation location : locations) {
      final CompleteSegmentCacheEntry entry = checkComplete(location.getCacheEntry(id), id);
      if (entry != null) {
        return entry.referenceProvider;
      }
    }
    return null;
  }

  /**
   * Returns the effective segment info directory based on the configuration settings.
   * The directory is selected based on the following configurations injected into this class:
   * <ul>
   *   <li>{@link SegmentLoaderConfig#getInfoDir()} - If {@code infoDir} is set, it is used as the info directory.</li>
   *   <li>{@link SegmentLoaderConfig#getLocations()} - If the info directory is not set, the first location from this list is used.</li>
   *   <li>List of {@link StorageLocation}s injected - If both the info directory and locations list are not set, the
   *   first storage location is used.</li>
   * </ul>
   *
   * @throws DruidException if none of the configurations are set, and the info directory cannot be determined.
   */
  private File getEffectiveInfoDir()
  {
    final File infoDir;
    if (config.getInfoDir() != null) {
      infoDir = config.getInfoDir();
    } else if (!config.getLocations().isEmpty()) {
      infoDir = new File(config.getLocations().get(0).getPath(), "info_dir");
    } else if (!locations.isEmpty()) {
      infoDir = new File(locations.get(0).getPath(), "info_dir");
    } else {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build("Could not determine infoDir. Make sure 'druid.segmentCache.infoDir' "
                                 + "or 'druid.segmentCache.locations' is set correctly.");
    }
    return infoDir;
  }

  private Supplier<ListenableFuture<AcquireSegmentResult>> makeOnDemandLoadSupplier(
      final CompleteSegmentCacheEntry entry,
      final StorageLocation location
  )
  {
    return Suppliers.memoize(
        () -> {
          final long startTime = System.nanoTime();
          return virtualStorageLoadingThreadPool.getExecutorService().submit(
              () -> {
                final long execStartTime = System.nanoTime();
                final long waitTime = execStartTime - startTime;
                entry.mount(location);
                return new AcquireSegmentResult(
                    entry.referenceProvider,
                    entry.dataSegment.getSize(),
                    waitTime,
                    System.nanoTime() - execStartTime
                );
              }
          );
        }
    );
  }

  private ReferenceCountingLock lock(final DataSegment dataSegment)
  {
    return segmentLocks.compute(
        dataSegment,
        (segment, lock) -> {
          final ReferenceCountingLock nonNullLock;
          if (lock == null) {
            nonNullLock = new ReferenceCountingLock();
          } else {
            nonNullLock = lock;
          }
          nonNullLock.increment();
          return nonNullLock;
        }
    );
  }

  private void unlock(final DataSegment dataSegment, final ReferenceCountingLock lock)
  {
    segmentLocks.compute(
        dataSegment,
        (segment, existingLock) -> {
          if (existingLock == null) {
            throw new ISE("Lock has already been removed");
          } else if (existingLock != lock) {
            throw new ISE("Different lock instance");
          } else {
            if (existingLock.numReferences == 1) {
              return null;
            } else {
              existingLock.decrement();
              return existingLock;
            }
          }
        }
    );
  }

  private CompleteSegmentCacheEntry assignLocationAndMount(
      final CompleteSegmentCacheEntry cacheEntry,
      final SegmentLazyLoadFailCallback segmentLoadFailCallback
  ) throws SegmentLoadingException
  {
    try {
      for (StorageLocation location : locations) {
        if (cacheEntry.checkExists(location.getPath())) {
          if (location.isReserved(cacheEntry.id) || location.reserve(cacheEntry)) {
            final CompleteSegmentCacheEntry entry = checkComplete(location.getCacheEntry(cacheEntry.id), cacheEntry.id);
            if (entry != null) {
              entry.lazyLoadCallback = segmentLoadFailCallback;
              entry.setOnUnmount(null);
              entry.mount(location);
              return entry;
            }
          } else {
            // entry is not reserved, clean it up
            atomicMoveAndDeleteCacheEntryDirectory(cacheEntry.toPotentialLocation(location.getPath()));
          }
        }
      }
    }
    catch (SegmentLoadingException e) {
      log.warn(e, "Failed to load segment[%s] in existing location, trying new location", cacheEntry.id);
    }
    final Iterator<StorageLocation> locationsIterator = strategy.getLocations();
    while (locationsIterator.hasNext()) {
      final StorageLocation location = locationsIterator.next();
      if (location.reserve(cacheEntry)) {
        try {
          final CompleteSegmentCacheEntry entry = checkComplete(location.getCacheEntry(cacheEntry.id), cacheEntry.id);
          if (entry != null) {
            entry.lazyLoadCallback = segmentLoadFailCallback;
            entry.setOnUnmount(null);
            entry.mount(location);
            return entry;
          }
        }
        catch (SegmentLoadingException e) {
          log.warn(e, "Failed to load segment[%s] in location[%s], trying next location", cacheEntry.id, location.getPath());
        }
      }
    }
    throw new SegmentLoadingException("Failed to load segment[%s] in all locations.", cacheEntry.id);
  }

  /**
   * Narrow a {@link CacheEntry} to {@link CompleteSegmentCacheEntry} with a defensive check. Returns null when the
   * entry is missing. Throws when a non-complete entry (e.g. {@link PartialSegmentMetadataCacheEntry}) is registered
   * under the same identifier. Callers of {@link #assignLocationAndMount} only operate on the non-virtual-storage
   * eager path, so encountering a partial entry here is a programming error.
   */
  @Nullable
  private static CompleteSegmentCacheEntry checkComplete(@Nullable CacheEntry entry, SegmentCacheEntryIdentifier id)
  {
    if (entry == null) {
      return null;
    }
    if (entry instanceof CompleteSegmentCacheEntry complete) {
      return complete;
    }
    throw DruidException.defensive(
        "Expected a complete cache entry for [%s], got [%s].",
        id,
        entry.getClass().getSimpleName()
    );
  }

  /**
   * Performs an atomic move to a sibling {@link #DROP_PATH} directory, and then deletes the directory and logs about
   * it. This method should only be called under the lock of a {@link #segmentLocks}.
   */
  private static void atomicMoveAndDeleteCacheEntryDirectory(final File path)
  {
    final File parent = path.getParentFile();
    final File tempLocation = new File(parent, DROP_PATH);
    try {
      if (!tempLocation.exists()) {
        FileUtils.mkdirp(tempLocation);
      }
      final File tempPath = new File(tempLocation, path.getName());
      log.debug("moving[%s] to temp location[%s]", path, tempLocation);
      Files.move(path.toPath(), tempPath.toPath(), StandardCopyOption.ATOMIC_MOVE);
      log.info("Deleting directory[%s]", path);
      FileUtils.deleteDirectory(tempPath);
    }
    catch (Exception e) {
      log.error(e, "Unable to remove directory[%s]", path);
    }
  }

  /**
   * Calls {@link FileUtils#deleteDirectory(File)} and then checks parent path if it is empty, and recursively
   * continues until a non-empty directory or the base path is reached. This method is not thread-safe, and should only
   * be used by a single caller.
   */
  private static void cleanupLegacyCacheLocation(final File baseFile, final File cacheFile)
  {
    try {
      log.info("Deleting migrated segment directory[%s]", cacheFile);
      FileUtils.deleteDirectoryAndEmptyAncestors(cacheFile, baseFile);
    }
    catch (Exception e) {
      log.warn(e, "Unable to remove directory[%s]", cacheFile);
    }
  }

  /**
   * check if segment data is possibly corrupted.
   * @param dir segments cache dir
   * @return true means segment files may be damaged.
   */
  private static boolean isPossiblyCorrupted(final File dir)
  {
    return hasStartMarker(dir);
  }

  /**
   * If {@link #DOWNLOAD_START_MARKER_FILE_NAME} exists in the path, the segment files might be damaged because this
   * file is typically deleted after the segment is pulled from deep storage.
   */
  private static boolean hasStartMarker(final File localStorageDir)
  {
    final File downloadStartMarker = new File(localStorageDir.getPath(), DOWNLOAD_START_MARKER_FILE_NAME);
    return downloadStartMarker.exists();
  }

  private static final class ReferenceCountingLock
  {
    private int numReferences;

    private void increment()
    {
      ++numReferences;
    }

    private void decrement()
    {
      --numReferences;
    }
  }

  private final class CompleteSegmentCacheEntry implements SegmentCacheEntry
  {
    private final SegmentCacheEntryIdentifier id;
    private final DataSegment dataSegment;
    private final String relativePathString;
    private SegmentLazyLoadFailCallback lazyLoadCallback = SegmentLazyLoadFailCallback.NOOP;
    private StorageLocation location;
    private File storageDir;
    private ReferenceCountedSegmentProvider referenceProvider;
    private final AtomicReference<Runnable> onUnmount = new AtomicReference<>();
    // switched from synchronized to use a ReentrantLock to avoid pinning virtual threads to platform threads until
    // https://openjdk.org/jeps/491, we could consider switching back after java 24+ is the minimum version
    private final ReentrantLock entryLock = new ReentrantLock();

    private CompleteSegmentCacheEntry(final DataSegment dataSegment)
    {
      this.dataSegment = dataSegment;
      this.id = new SegmentCacheEntryIdentifier(dataSegment.getId());
      this.relativePathString = dataSegment.getId().toString();
    }

    @Override
    public SegmentCacheEntryIdentifier getId()
    {
      return id;
    }

    @Override
    public long getSize()
    {
      return dataSegment.getSize();
    }

    @Override
    public boolean isMounted()
    {
      entryLock.lock();
      try {
        return referenceProvider != null;
      }
      finally {
        entryLock.unlock();
      }
    }

    @Override
    public void mount(StorageLocation mountLocation) throws SegmentLoadingException
    {
      // check to see if we should still be mounting by making sure we are still reserved in the location
      // this is not done under a lock of the location, and that is ok.. we will check again at the end to prevent any
      // orphaned files
      if (!mountLocation.isReserved(this.id) && !mountLocation.isWeakReserved(this.id)) {
        log.debug(
            "aborting mount in location[%s] since entry[%s] is no longer reserved",
            mountLocation.getPath(),
            this.id
        );
        return;
      }

      try {
        entryLock.lock();
        try {
          if (location != null) {
            log.debug(
                "already mounted [%s] in location[%s], but asked to load in [%s], unmounting old location",
                id,
                location.getPath(),
                mountLocation.getPath()
            );
            if (!location.equals(mountLocation)) {
              throw DruidException.defensive(
                  "already mounted[%s] in location[%s] which is different from requested[%s]",
                  id,
                  location.getPath(),
                  mountLocation.getPath()
              );
            } else if (referenceProvider != null) {
              log.debug("already mounted [%s] in location[%s]", id, mountLocation.getPath());
              return;
            }
          }
          location = mountLocation;
          storageDir = new File(location.getPath(), relativePathString);
          boolean needsLoad = true;
          if (storageDir.exists()) {
            if (isPossiblyCorrupted(storageDir)) {
              log.warn(
                  "[%s] may be damaged. Delete all the segment files and pull from DeepStorage again.",
                  storageDir.getAbsolutePath()
              );
              atomicMoveAndDeleteCacheEntryDirectory(storageDir);
            } else {
              needsLoad = false;
            }
          }
          if (needsLoad) {
            loadInLocationWithStartMarker(dataSegment, storageDir);
          }
          final SegmentizerFactory factory = getSegmentFactory(storageDir);

          @SuppressWarnings("ObjectEquality")
          final boolean lazy = config.isLazyLoadOnStart() && lazyLoadCallback != SegmentLazyLoadFailCallback.NOOP;
          final Segment segment = factory.factorize(dataSegment, storageDir, lazy, lazyLoadCallback);
          // wipe load callback after calling
          lazyLoadCallback = SegmentLazyLoadFailCallback.NOOP;
          referenceProvider = ReferenceCountedSegmentProvider.of(segment);
        }
        finally {
          entryLock.unlock();
        }

        // since we do not hold a lock on the location while mounting, make sure that we actually are reserved and
        // should have mounted, otherwise unmount so we don't leave any orphaned files. These checks acquire the
        // location lock, so they must run with entryLock released to avoid deadlocking.
        final boolean isWeak = mountLocation.isWeakReserved(this.id);
        final boolean isStatic = !isWeak && mountLocation.isReserved(this.id);
        if (!isWeak && !isStatic) {
          log.debug(
              "aborting mount in location[%s] since entry[%s] is no longer reserved",
              mountLocation.getPath(),
              this.id
          );
          unmount();
        } else if (isWeak) {
          mountLocation.trackWeakLoad(dataSegment.getSize());
        } else {
          mountLocation.trackStaticLoad(dataSegment.getSize());
        }

        if (config.isVirtualStorageEphemeral()) {
          setOnUnmount(() -> deleteSegmentInfoFile(dataSegment));
        }
      }
      catch (SegmentLoadingException e) {
        try {
          log.makeAlert(
              e,
              "Failed to load segment in current location [%s], try next location if any",
              location.getPath().getAbsolutePath()
          ).addData("location", location.getPath().getAbsolutePath()).emit();

          throw new SegmentLoadingException(
              "Failed to load segment[%s] in reserved location[%s]",
              dataSegment.getId(),
              location.getPath().getAbsolutePath()
          );
        }
        finally {
          unmount();
        }
      }
      catch (Throwable t) {
        unmount();
        throw t;
      }
    }

    @Override
    public void unmount()
    {
      final Lock locationLock;
      entryLock.lock();
      try {
        if (location == null) {
          return;
        }
        locationLock = location.getLock().readLock();
      }
      finally {
        entryLock.unlock();
      }
      locationLock.lock();
      try {
        entryLock.lock();
        try {
          if (referenceProvider != null) {
            ReferenceCountedSegmentProvider provider = referenceProvider;
            referenceProvider = null;
            provider.close();
          }
          if (!config.isDeleteOnRemove()) {
            return;
          }
          if (storageDir != null) {
            if (storageDir.exists()) {
              atomicMoveAndDeleteCacheEntryDirectory(storageDir);
            }
            storageDir = null;
            location = null;
          }

          final Runnable onUnmountRunnable = onUnmount.get();
          if (onUnmountRunnable != null) {
            onUnmountRunnable.run();
          }
        }
        finally {
          entryLock.unlock();
        }
      }
      finally {
        locationLock.unlock();
      }
    }

    @Override
    public SegmentId getSegmentId()
    {
      return dataSegment.getId();
    }

    @Override
    public Optional<Segment> acquireReference()
    {
      entryLock.lock();
      try {
        if (referenceProvider == null) {
          return Optional.empty();
        }
        return referenceProvider.acquireReference();
      }
      finally {
        entryLock.unlock();
      }
    }

    @Override
    public void setOnUnmount(@Nullable Runnable hook)
    {
      entryLock.lock();
      try {
        onUnmount.set(hook);
      }
      finally {
        entryLock.unlock();
      }
    }

    @Override
    public boolean isFullyDownloaded()
    {
      // Complete entries extract every byte at mount time, so by definition a mounted entry is fully downloaded.
      return isMounted();
    }

    public void loadIntoPageCache()
    {
      if (!isMounted()) {
        return;
      }
      entryLock.lock();
      try {
        final File[] children = storageDir.listFiles();
        if (children != null) {
          for (File child : children) {
            try (InputStream in = Files.newInputStream(child.toPath())) {
              IOUtils.copy(in, NullOutputStream.NULL_OUTPUT_STREAM);
              log.info("Loaded [%s] into page cache.", child.getAbsolutePath());
            }
            catch (Exception e) {
              log.error(e, "Failed to load [%s] into page cache", child.getAbsolutePath());
            }
          }
        }
      }
      finally {
        entryLock.unlock();
      }
    }

    public boolean checkExists(final File location)
    {
      return toPotentialLocation(location).exists();
    }

    public File toPotentialLocation(final File location)
    {
      return new File(location, relativePathString);
    }

    @GuardedBy("entryLock")
    private void loadInLocationWithStartMarker(final DataSegment segment, final File storageDir)
        throws SegmentLoadingException
    {
      // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
      // the parent directories of the segment are removed
      final File downloadStartMarker = new File(storageDir, DOWNLOAD_START_MARKER_FILE_NAME);
      try {
        FileUtils.mkdirp(storageDir);

        if (!downloadStartMarker.createNewFile()) {
          throw new SegmentLoadingException("Was not able to create new download marker for [%s]", storageDir);
        }
        loadInLocation(segment, storageDir);

        if (!downloadStartMarker.delete()) {
          throw new SegmentLoadingException("Unable to remove marker file for [%s]", storageDir);
        }
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to create marker file for [%s]", storageDir);
      }
    }

    @GuardedBy("entryLock")
    private void loadInLocation(final DataSegment segment, final File storageDir)
        throws SegmentLoadingException
    {
      // LoadSpec isn't materialized until here so that any system can interpret Segment without having to have all the
      // LoadSpec dependencies.
      final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
      final LoadSpec.LoadSpecResult result = loadSpec.loadSegment(storageDir);
      if (result.getSize() != segment.getSize()) {
        log.warn(
            "Segment [%s] is different than expected size. Expected [%d] found [%d]",
            segment.getId(),
            segment.getSize(),
            result.getSize()
        );
      }
    }

    @GuardedBy("entryLock")
    private SegmentizerFactory getSegmentFactory(final File segmentFiles) throws SegmentLoadingException
    {
      final File factoryJson = new File(segmentFiles, "factory.json");
      final SegmentizerFactory factory;

      if (factoryJson.exists()) {
        try {
          factory = jsonMapper.readValue(factoryJson, SegmentizerFactory.class);
        }
        catch (IOException e) {
          throw new SegmentLoadingException(e, "Failed to get segment factory for %s", e.getMessage());
        }
      } else {
        factory = new MMappedQueryableSegmentizerFactory(indexIO);
      }
      return factory;
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      CompleteSegmentCacheEntry that = (CompleteSegmentCacheEntry) o;
      return Objects.equals(dataSegment, that.dataSegment);
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(dataSegment);
    }
  }

  /**
   * The {@link AcquireSegmentAction#close()} cleanup for a partial acquire: a set of cache holds that keep the
   * acquired segment's entries resident for the action's lifetime. Seeded with the metadata reservation hold at
   * construction; the full-download path {@link #add adds} a hold per bundle it mounts while the load future runs.
   * Closing releases everything (LIFO).
   * <p>
   * Thread-safe because the future that {@link #add}s bundle holds runs on the load executor while a different thread
   * may close the action (query cancel / timeout racing a blocked {@code getSegmentFuture().get()}). A hold added
   * after the action has already been closed is closed immediately rather than leaked.
   */
  private static final class HoldHolder implements Closeable
  {
    @GuardedBy("this")
    private final Closer holds = Closer.create();
    @GuardedBy("this")
    private boolean closed = false;

    private HoldHolder(Closeable initialHold)
    {
      holds.register(initialHold);
    }

    private void add(Closeable hold)
    {
      final boolean alreadyClosed;
      synchronized (this) {
        alreadyClosed = closed;
        if (!alreadyClosed) {
          holds.register(hold);
        }
      }
      if (alreadyClosed) {
        // the action was closed while the load future was still running; release the late hold rather than leak it
        CloseableUtils.closeAndSuppressExceptions(hold, ignored -> {});
      }
    }

    @Override
    public synchronized void close() throws IOException
    {
      closed = true;
      holds.close();
    }
  }
}
