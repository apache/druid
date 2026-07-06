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
import org.apache.druid.segment.file.SegmentFileMetadata;
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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
        // When the segment's loadSpec is a partial-load-rule wrapper, parse the on-disk header and resolve the
        // rule-selected bundle names so the bootstrap restore can reserve metadata + selected bundles as static.
        // The fingerprint from the persisted wrapper is threaded through so a later load() call can detect a rule
        // change and reconcile via drop-and-reload.
        Set<String> staticBundleNames = Set.of();
        String fingerprint = null;
        if (PartialLoadSpec.detectPartialLoadSpec(segment.getLoadSpec())) {
          try {
            final LoadSpec materializedLoadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
            if (materializedLoadSpec instanceof PartialLoadSpec wrapper) {
              final File headerFile = new File(
                  partialDir,
                  IndexIO.V10_FILE_NAME + PartialSegmentFileMapperV10.METADATA_HEADER_SUFFIX
              );
              final SegmentFileMetadata parsed = PartialSegmentFileMapperV10.parseHeaderFile(
                  headerFile,
                  jsonMapper
              ).getMetadata();
              staticBundleNames = Set.copyOf(wrapper.getSelectedBundleNames(segment, parsed));
              fingerprint = wrapper.getFingerprint();
            }
          }
          catch (Throwable t) {
            // ANY failure here means we can't honor bootstrap's "restore from disk" contract for this segment, so nuke
            // everything and the coordinator will re-issue load and handle everything.
            if (t instanceof DruidException) {
              log.makeAlert(
                  t,
                  "Partial-load rule contract violation for segment[%s] at bootstrap; deleting on-disk cache state",
                  segment.getId()
              ).emit();
            } else {
              log.warn(
                  t,
                  "Failed to resolve partial-load wrapper for segment[%s] at bootstrap; deleting on-disk cache state",
                  segment.getId()
              );
            }
            atomicMoveAndDeleteCacheEntryDirectory(partialDir);
            removeInfo = true;
            continue;
          }
        }
        try {
          PartialSegmentCacheBootstrap.reserveFromDisk(
              segment.getId(),
              partialDir,
              IndexIO.V10_FILE_NAME,
              List.of(),
              staticBundleNames,
              fingerprint,
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
        throw DruidException.defensive(e, "Failed to create partial cache dir for segment[%s]", dataSegment.getId());
      }
      final StorageLocation.ReservationHold<SegmentCacheEntry> hold = location.addWeakReservationHold(
          id,
          () -> new PartialSegmentMetadataCacheEntry(
              dataSegment.getId(),
              partialDir,
              IndexIO.V10_FILE_NAME,
              List.of(),
              Set.of(),
              null,
              rangeReader,
              jsonMapper,
              virtualStorageLoadingThreadPool,
              config.getVirtualStorageMetadataReservationEstimate()
          )
      );
      if (hold == null) {
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
        final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), dataSegment.getId().toString());
        if (!segmentInfoCacheFile.exists()) {
          FileUtils.mkdirp(getEffectiveInfoDir());
          FileUtils.writeAtomically(segmentInfoCacheFile, out -> {
            jsonMapper.writeValue(out, dataSegment);
            return null;
          });
        }
        partial.setOnUnmount(() -> deleteSegmentInfoFile(dataSegment));
        return new ReservedPartial(partial, location, hold);
      }
      catch (Throwable t) {
        throw CloseableUtils.closeAndWrapInCatch(t, hold);
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
   * Handle the partial-load-rule case at {@link #load} time: reserve a {@link PartialSegmentMetadataCacheEntry} as
   * static, persist the info file, mount the metadata header, and reserve+mount each rule-selected bundle as static
   * with its data eagerly downloaded. Static reservations are not subject to SIEVE eviction, so the rule-pinned
   * subset stays resident until the segment is explicitly dropped; non-selected bundles are downloaded on-demand at
   * query time via the existing weak path.
   * <p>
   * The entire flow runs synchronously on {@link #virtualStorageLoadingThreadPool} (via submit + get) so it respects
   * the pool's concurrency cap and virtual-thread runtime while giving
   * {@link org.apache.druid.server.coordination.SegmentLoadDropHandler} a truthful success/failure signal. On any
   * failure (mount, bundle reservation, download) the metadata + any partially-reserved bundles are cascade-released
   * before the {@link SegmentLoadingException} propagates, so no orphan static reservation survives the load call.
   */
  private void loadPartial(DataSegment dataSegment) throws SegmentLoadingException
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

    final SegmentRangeReader rangeReader;
    try {
      rangeReader = wrapper.openRangeReader();
    }
    catch (IOException e) {
      throw new SegmentLoadingException(
          e,
          "Failed to open range reader for segment[%s]",
          dataSegment.getId()
      );
    }
    if (rangeReader == null) {
      // Backend doesn't support range reads (e.g. zipped deep storage). The rule can't be honored as a partial load;
      // fall through to the on-demand weak-full-load path at query time. But if a prior loadPartial installed a
      // static entry with a different rule fingerprint, we cannot silently keep serving under the old rule —
      // reconcile the static side so the coordinator's rule change takes effect, then return so the next query
      // installs a weak entry via the on-demand path.
      log.warn(
          "Backend for segment[%s] does not support range reads; partial-load rule[fingerprint=%s] cannot be honored, "
          + "falling back to weak full-load at query time",
          dataSegment.getId(),
          wrapper.getFingerprint()
      );
      final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(dataSegment.getId());
      final ReferenceCountingLock lock = lock(dataSegment);
      synchronized (lock) {
        try {
          // Static-side reconciliation only. Weak entries (from bootstrap without a rule, or a prior on-demand
          // acquire) already represent the fallback behavior we're falling back to — leave them alone.
          boolean dropStaleStatic = false;
          for (StorageLocation location : locations) {
            if (!location.isReserved(id)) {
              continue;
            }
            final SegmentCacheEntry existing = location.getCacheEntry(id);
            if (existing instanceof PartialSegmentMetadataCacheEntry existingPartial
                && Objects.equals(existingPartial.getFingerprint(), wrapper.getFingerprint())) {
              // Same-rule re-issue: preserve the existing entry's info file across its eventual drop by clearing
              // the onUnmount hook, matching the fingerprint-match short-circuit in the main reconciliation branch.
              existing.setOnUnmount(null);
              return;
            }
            dropStaleStatic = true;
          }
          if (dropStaleStatic) {
            log.info(
                "Dropping stale static partial entry for segment[%s]: current rule cannot be applied without range reads",
                dataSegment.getId()
            );
            drop(dataSegment);
            // drop() only deleted tracked files; no reserve follows on this path, so nuke leftover per-segment dirs
            // across every location or an empty dir would linger indefinitely.
            nukeStaleSegmentDirsOnAllLocations(dataSegment);
          }
        }
        finally {
          unlock(dataSegment, lock);
        }
      }
      return;
    }

    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(dataSegment.getId());
    final ReferenceCountingLock lock = lock(dataSegment);
    synchronized (lock) {
      try {
        // Reconcile against any existing entry. The end-state invariant this block establishes is: on any return
        // path below, there is exactly one static partial entry for this segment (matching the wrapper fingerprint)
        // and no weak entry alongside it.
        //
        //  - Same-fingerprint static partial with no weak entry alongside: idempotent re-load. Clear onUnmount so the
        //    info file isn't re-deleted, return.
        //  - Any other state (different-fingerprint static partial, non-partial static entry, weak partial from
        //    bootstrap without a persisted fingerprint, weak full entry from a prior on-demand acquireSegment): drop
        //    the static (cascade-releases linked bundles + fires the info-file cleanup hook) AND evict the weak
        //    entry, then fall through to a fresh reserve.
        //
        // The weak eviction step is critical: {@link #drop} only touches {@link StorageLocation#staticCacheEntries},
        // so without it a lingering weak entry from bootstrap or on-demand load would coexist with the new static
        // entry — same on-disk directory, both counted against capacity, and the eventual bundle downloads would
        // race the weak entry's own reads. When the weak entry has active holds we cannot evict it synchronously,
        // so throw and let the coordinator retry once the running query completes. drop() takes no per-segment lock
        // so calling it from inside our synchronized(lock) block is safe.
        PartialSegmentMetadataCacheEntry sameFingerprintStatic = null;
        final List<SegmentCacheEntry> oldStaticEntries = new ArrayList<>();
        for (StorageLocation location : locations) {
          if (!location.isReserved(id)) {
            continue;
          }
          final SegmentCacheEntry existing = location.getCacheEntry(id);
          if (existing == null) {
            continue;
          }
          oldStaticEntries.add(existing);
          if (existing instanceof PartialSegmentMetadataCacheEntry existingPartial
              && Objects.equals(existingPartial.getFingerprint(), wrapper.getFingerprint())) {
            sameFingerprintStatic = existingPartial;
          }
        }
        final boolean weakExists = locations.stream().anyMatch(loc -> loc.isWeakReserved(id));
        if (sameFingerprintStatic != null && !weakExists && oldStaticEntries.size() == 1) {
          sameFingerprintStatic.setOnUnmount(null);
          return;
        }
        if (!oldStaticEntries.isEmpty()) {
          // Precheck BEFORE drop: refuse the reconciliation if any old partial metadata entry (or any of its linked
          // bundles) has an external reference. drop() would remove entries from staticCacheEntries synchronously
          // but leave their doActualUnmount (onUnmount hook + deleteHeaderFiles + evictContainer) deferred until
          // the pinning query releases. Between the throw and the next coordinator retry, other threads (queries,
          // on-demand acquires) would observe the segment as absent, install a fresh partial entry via the acquire
          // path, and race the deferred cleanup on the shared per-segment on-disk directory. Checking BEFORE drop
          // keeps the old entry fully installed on the failure path so a subsequent retry finds it via
          // reconciliation logic as if this load never happened.
          //
          // cascadeReleaseWouldDeferCleanup subtracts the known cross-entry structure (bundle→metadata refs,
          // parent-bundle→child holds) from raw reference counts to isolate external consumers; those are the only
          // refs drop's cascade cannot release.
          for (SegmentCacheEntry old : oldStaticEntries) {
            if (old instanceof PartialSegmentMetadataCacheEntry oldPartial
                && oldPartial.cascadeReleaseWouldDeferCleanup()) {
              throw new SegmentLoadingException(
                  "Cannot reconcile partial-load entry for segment[%s]: outstanding references on the old entry "
                  + "would defer drop's cleanup and race the new entry's on-disk state; coordinator will retry",
                  dataSegment.getId()
              );
            }
          }
          log.info(
              "Reconciling partial-load entry for segment[%s]: fingerprint or type changed, dropping and reloading",
              dataSegment.getId()
          );
          drop(dataSegment);
        }
        for (StorageLocation location : locations) {
          if (!location.isWeakReserved(id)) {
            continue;
          }
          location.removeUnheldWeakEntry(id);
          if (location.isWeakReserved(id)) {
            throw new SegmentLoadingException(
                "Cannot install partial-load rule for segment[%s]: an existing weak entry has active holds; "
                + "coordinator will retry",
                dataSegment.getId()
            );
          }
        }

        // After any reconciliation drop/eviction above, nuke leftover per-segment directories on every location.
        // The drop cascade deleted TRACKED files (header via metadata's doActualUnmount, containers via each bundle's
        // evictContainer), but not the enclosing per-segment directory nor any orphan files. If the new reserve below
        // lands on a DIFFERENT location than the dropped entry, the old location's now-empty dir would accumulate on
        // disk indefinitely; if the new reserve lands on the SAME location, the mkdirp inside the reserve loop
        // recreates the dir fresh.
        nukeStaleSegmentDirsOnAllLocations(dataSegment);

        // Reserve the metadata entry as static on the first location that accepts BOTH the reservation and a
        // per-segment mkdirp. mkdirp failures are location-local (inode exhaustion, permission mismatch, transient
        // EIO on one disk) — a different location may accept, so release-and-continue rather than abort. Collect
        // per-location failures to attach as suppressed exceptions on the final alert.
        StorageLocation reservedLocation = null;
        PartialSegmentMetadataCacheEntry metadata = null;
        final List<Throwable> perLocationFailures = new ArrayList<>();
        final Iterator<StorageLocation> iterator = strategy.getLocations();
        while (iterator.hasNext()) {
          final StorageLocation candidate = iterator.next();
          final File partialDir = new File(candidate.getPath(), dataSegment.getId().toString());
          final PartialSegmentMetadataCacheEntry candidateEntry = new PartialSegmentMetadataCacheEntry(
              dataSegment.getId(),
              partialDir,
              IndexIO.V10_FILE_NAME,
              List.of(),
              // staticBundleNames left empty: the fresh-load path below resolves selected bundles AFTER metadata mount
              // (since the bundle names depend on the parsed header) and reserves them static directly via
              // reserveAndMountStaticBundle. The field is only consulted by bootstrap restore, which has a parsed
              // on-disk header available before constructing the entry.
              Set.of(),
              wrapper.getFingerprint(),
              rangeReader,
              jsonMapper,
              virtualStorageLoadingThreadPool,
              config.getVirtualStorageMetadataReservationEstimate()
          );
          if (!candidate.reserve(candidateEntry)) {
            continue;
          }
          try {
            FileUtils.mkdirp(partialDir);
          }
          catch (IOException mkdirpFailure) {
            candidate.release(candidateEntry);
            log.warn(
                mkdirpFailure,
                "Failed to create partial cache dir on location[%s] for segment[%s]; trying next location",
                candidate.getPath(),
                dataSegment.getId()
            );
            perLocationFailures.add(mkdirpFailure);
            continue;
          }
          reservedLocation = candidate;
          metadata = candidateEntry;
          break;
        }

        if (reservedLocation == null) {
          final SegmentLoadingException noCapacity = new SegmentLoadingException(
              "Unable to reserve static partial metadata entry for segment[%s] on any location; ensure enough disk "
              + "space and that per-segment cache directories can be created",
              dataSegment.getId()
          );
          perLocationFailures.forEach(noCapacity::addSuppressed);
          log.makeAlert(
              noCapacity,
              "Failed to reserve partial-load metadata on any location for segment[%s]",
              dataSegment.getId()
          ).emit();
          throw noCapacity;
        }

        // Persist the info file so bootstrap can rediscover this segment on restart. The info dir is shared across
        // locations (single configured path), so failure here is not a per-disk problem — no fall-through, alert
        // and abort.
        try {
          storeInfoFile(dataSegment);
        }
        catch (IOException e) {
          reservedLocation.release(metadata);
          final SegmentLoadingException storeFailure = new SegmentLoadingException(
              e,
              "Failed to write info file for segment[%s]",
              dataSegment.getId()
          );
          log.makeAlert(
              storeFailure,
              "Failed to persist partial-load info file for segment[%s]; check druid.segmentCache.infoDir",
              dataSegment.getId()
          ).emit();
          throw storeFailure;
        }
        metadata.setOnUnmount(() -> deleteSegmentInfoFile(dataSegment));

        // Mount metadata + reserve+mount+download each rule-selected bundle as static. Runs on the loading thread pool
        // (submit + get) so the flow respects the pool's concurrency cap while giving the calling thread a truthful
        // success/failure signal. Any exception cascade-releases the metadata + every bundle reserved so far, so no
        // orphan static reservation survives the load call, the reservation's onUnmount hook then deletes the info
        // file, mirroring drop() semantics.
        final StorageLocation theLocation = reservedLocation;
        final PartialSegmentMetadataCacheEntry theMetadata = metadata;
        // All rollback responsibility (bundles AND metadata) lives INSIDE the callable so no state mutation of
        // theMetadata crosses threads. This avoids the FutureTask cancel-vs-cleanup race: cancel(true) transitions
        // the future to CANCELLED immediately, so a subsequent get() returns CancellationException without waiting
        // for the callable's cleanup to unwind — if the caller then released theMetadata itself, that release would
        // race the still-running pool-thread rollback. With cleanup owned by the pool thread, the caller only ever
        // observes the outcome; there is no shared state to race.
        final Future<Void> future = virtualStorageLoadingThreadPool.getExecutorService().submit(() -> {
          final List<PartialSegmentBundleCacheEntry> mounted = new ArrayList<>();
          try {
            theMetadata.mount(theLocation);
            final PartialSegmentFileMapperV10 mapper = theMetadata.getFileMapper();
            if (mapper == null) {
              throw DruidException.defensive(
                  "Partial metadata for segment[%s] mounted without a file mapper",
                  dataSegment.getId()
              );
            }
            final List<String> selectedBundleNames = wrapper.getSelectedBundleNames(
                dataSegment,
                mapper.getSegmentFileMetadata()
            );
            // A rule-pinned bundle's dependencies must also be pinned: the bundle's own mount call acquires holds on
            // each dependency via addWeakReservationHoldIfExists, which requires the dependency to be registered.
            // Iterate in dependency-first order so children see their dependencies resident.
            for (String bundleName : theMetadata.bundlesInMountOrder(selectedBundleNames)) {
              mounted.add(reserveAndMountStaticBundle(theLocation, theMetadata, mapper, bundleName));
            }
            return null;
          }
          catch (Throwable t) {
            // Cascade rollback on the SAME thread: bundles first in reverse mount order, then the metadata (whose
            // release-triggered doActualUnmount hook deletes the info file).
            for (int i = mounted.size() - 1; i >= 0; i--) {
              try {
                theLocation.release(mounted.get(i));
              }
              catch (Throwable releaseFailure) {
                log.warn(
                    releaseFailure,
                    "Failed to release bundle during rollback for segment[%s]",
                    dataSegment.getId()
                );
              }
            }
            releaseMetadataQuietly(theLocation, theMetadata, dataSegment);
            throw t;
          }
        });

        try {
          future.get();
        }
        catch (InterruptedException e) {
          // Caller interrupted while waiting. Restore the interrupt flag and propagate, but do NOT cancel the pool
          // task: FutureTask.cancel(true) transitions to CANCELLED synchronously, but the callable's run() may still
          // be executing its catch/finally on the pool thread — so a subsequent metadata release from the caller
          // would race that cleanup. Leaving the task alone lets its own cleanup complete on the pool thread
          // naturally; on eventual shutdown the pool's shutdownNow will interrupt the worker and drive the same
          // rollback via the callable's catch. The coordinator's retry will find the segment in one of three
          // consistent end-states: fully mounted (task raced to success — reconciliation short-circuits), fully
          // rolled-back (task failed after interrupt — fresh reserve), or still in flight (retry throws until the
          // outstanding-references guard sees the task's own metadata release fire).
          Thread.currentThread().interrupt();
          throw new SegmentLoadingException(e, "Interrupted while loading partial segment[%s]", dataSegment.getId());
        }
        catch (Throwable t) {
          // Callable ran to a terminal state. On failure it already released metadata + bundles on the pool thread
          // via its catch block. We only propagate the diagnostic here; no cleanup remains for the caller. Unwrap
          // ExecutionException for a clean cause; handle a null cause defensively (rare, but permitted by
          // ExecutionException's contract). Restore the interrupt flag if the pool worker was interrupted mid-run
          // so downstream shutdown handling still observes it.
          final Throwable executionCause = (t instanceof ExecutionException) ? t.getCause() : t;
          final Throwable cause = (executionCause != null) ? executionCause : t;
          if (cause instanceof InterruptedException) {
            Thread.currentThread().interrupt();
          }
          throw new SegmentLoadingException(
              cause,
              "Failed to load partial segment[%s] with rule-pinned bundles",
              dataSegment.getId()
          );
        }
      }
      finally {
        unlock(dataSegment, lock);
      }
    }
  }

  /**
   * Reserve a {@link PartialSegmentBundleCacheEntry} as static on {@code location}, mount it (which initializes the
   * sparse containers and links it to the metadata entry), and eagerly download all of the bundle's containers via
   * the supplied {@link PartialSegmentFileMapperV10}. Returns the mounted bundle on success.
   * <p>
   * Any failure ({@code forBundle} throwing, capacity-exceeded on reserve, or a mid-mount / mid-download exception)
   * releases the bundle (if it was reserved) and rethrows so {@link #loadPartial}'s outer cascade can roll back the
   * metadata reservation.
   */
  private PartialSegmentBundleCacheEntry reserveAndMountStaticBundle(
      StorageLocation location,
      PartialSegmentMetadataCacheEntry metadata,
      PartialSegmentFileMapperV10 mapper,
      String bundleName
  ) throws IOException
  {
    final PartialSegmentBundleCacheEntry bundle = PartialSegmentBundleCacheEntry.forBundle(
        metadata,
        bundleName,
        metadata.inferBundleDependencies(bundleName)
    );
    if (!location.reserve(bundle)) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                          .build(
                              "Unable to reserve static bundle[%s] for segment[%s]; ensure enough disk space",
                              bundleName,
                              metadata.getSegmentId()
                          );
    }
    try {
      bundle.mount(location);
      mapper.ensureBundleDownloaded(bundleName);
      return bundle;
    }
    catch (Throwable t) {
      try {
        location.release(bundle);
      }
      catch (Throwable releaseFailure) {
        log.warn(releaseFailure, "Failed to release bundle[%s] during mount rollback", bundleName);
      }
      throw t;
    }
  }

  /**
   * Release a metadata entry, swallowing any failure with a warn log. Used from {@link #loadPartial}'s rollback path
   * where a release exception should not mask the original failure being propagated.
   */
  private void releaseMetadataQuietly(
      StorageLocation location,
      PartialSegmentMetadataCacheEntry metadata,
      DataSegment segment
  )
  {
    try {
      location.release(metadata);
    }
    catch (Throwable releaseFailure) {
      log.warn(releaseFailure, "Failed to release metadata during rollback for segment[%s]", segment.getId());
    }
  }

  /**
   * Move-and-delete any leftover per-segment cache directory for {@code dataSegment} on every configured location.
   * Called from the partial-load reconciliation paths after drop() so an empty partial dir on the old location doesn't
   * linger when the new reserve happens to land elsewhere. {@link #atomicMoveAndDeleteCacheEntryDirectory} is a no-op
   * on missing paths, so this walks all locations unconditionally.
   */
  private void nukeStaleSegmentDirsOnAllLocations(DataSegment dataSegment)
  {
    for (StorageLocation loc : locations) {
      atomicMoveAndDeleteCacheEntryDirectory(new File(loc.getPath(), dataSegment.getId().toString()));
    }
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
      // Partial-load rule routing: a wrapped load spec carrying a fingerprint + per-spec selection (cluster groups,
      // projections, ...) means the coordinator wants this segment loaded as a sticky partial. Reserve the metadata
      // entry as STATIC (never SIEVE-evicted) and eagerly download the rule-selected bundles as STATIC. Other bundles
      // are left for the existing weak/on-demand path at query time.
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
                // A mount failure on a bootstrap-restored partial entry must release the reservation so it doesn't
                // leak. doMount's own catch already calls removeUnheldWeakEntry for the weak variant; here we
                // additionally call location.release which handles the static variant introduced by the partial-load-
                // rule path (release is a no-op for weak entries but removes static ones from staticCacheEntries).
                // Without this, a static rule-pinned segment whose header parse fails at mount time would occupy the
                // metadata reservation forever.
                try {
                  location.release(partial);
                }
                catch (Throwable releaseFailure) {
                  log.warn(
                      releaseFailure,
                      "Failed to release partial metadata for segment[%s] after bootstrap mount failure",
                      dataSegment.getId()
                  );
                }
                throw new SegmentLoadingException(
                    e,
                    "Failed to mount partial metadata for segment[%s]",
                    dataSegment.getId()
                );
              }
              // Post-mount rule-integrity check: if any direct rule-pinned bundle didn't make it into the linked set
              // (JVM crash mid-loadPartial, or orphan cleanup deleted a bundle whose parent was missing), the segment
              // isn't fully restored to the rule's intent. Drop it entirely so the coordinator's reconciliation re-
              // issues load with a clean cold-fetch — operators don't drive drop/load directly, so a self-healing
              // pathway through the coordinator is the right recovery.
              final Set<String> directRulePins = partial.getStaticBundleNames();
              if (!directRulePins.isEmpty()) {
                final Set<String> mounted = new HashSet<>();
                for (PartialSegmentBundleCacheEntry b : partial.snapshotLinkedBundles()) {
                  mounted.add(b.getBundleName());
                }
                final List<String> missing = directRulePins.stream()
                                                           .filter(name -> !mounted.contains(name))
                                                           .toList();
                if (!missing.isEmpty()) {
                  log.makeAlert(
                      "Partial segment[%s] restored missing rule-pinned bundles [%s]; dropping so coordinator can"
                      + " re-issue load with a fresh cold-fetch",
                      dataSegment.getId(),
                      missing
                  ).emit();
                  // Cascade-release everything (linked bundles + metadata → fires info-file cleanup hook), then nuke
                  // the on-disk partial layout to catch any orphan container files that no bundle entry owned.
                  drop(dataSegment);
                  final File partialDir = new File(location.getPath(), dataSegment.getId().toString());
                  atomicMoveAndDeleteCacheEntryDirectory(partialDir);
                  throw new SegmentLoadingException(
                      "Partial segment[%s] rule-integrity check failed at bootstrap; dropped for re-load",
                      dataSegment.getId()
                  );
                }
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
    final SegmentCacheEntryIdentifier id = new SegmentCacheEntryIdentifier(segment.getId());
    for (StorageLocation location : locations) {
      final CacheEntry entry = location.getCacheEntry(id);
      if (entry == null) {
        continue;
      }
      // Cascade-release linked bundle entries before the metadata. {@link StorageLocation#release} only operates on
      // entries in {@code staticCacheEntries}, so this is a no-op for weak/on-demand bundles (they continue to ride
      // SIEVE eviction + hold-release cleanup); for static rule-pinned bundles installed by {@link #loadPartial},
      // this is the only path that removes them from the static map.
      if (entry instanceof PartialSegmentMetadataCacheEntry partial) {
        for (PartialSegmentBundleCacheEntry bundle : partial.snapshotLinkedBundles()) {
          location.release(bundle);
        }
      }
      location.release(entry);
    }
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
   * it. No-op if {@code path} does not exist — callers can invoke unconditionally without a preceding {@code exists()}
   * guard. Should only be called under the lock of a {@link #segmentLocks}.
   */
  private static void atomicMoveAndDeleteCacheEntryDirectory(final File path)
  {
    if (!path.exists()) {
      return;
    }
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
    if (cacheFile.equals(baseFile)) {
      return;
    }

    try {
      log.info("Deleting migrated segment directory[%s]", cacheFile);
      FileUtils.deleteDirectory(cacheFile);
    }
    catch (Exception e) {
      log.warn(e, "Unable to remove directory[%s]", cacheFile);
    }

    File parent = cacheFile.getParentFile();
    if (parent != null) {
      File[] children = parent.listFiles();
      if (children == null || children.length == 0) {
        cleanupLegacyCacheLocation(baseFile, parent);
      }
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
