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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.ReferenceCountedSegmentProvider;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentLazyLoadFailCallback;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 */
public class SegmentLocalCacheManager implements SegmentCacheManager
{
  @VisibleForTesting
  static final String DOWNLOAD_START_MARKER_FILE_NAME = "downloadStartMarker";

  private static final EmittingLogger log = new EmittingLogger(SegmentLocalCacheManager.class);

  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  private final List<StorageLocation> locations;

  private final ReadWriteLock directoryWriteRemoveLock = new ReentrantReadWriteLock();


  /**
   * A map between segment and referenceCountingLocks.
   *
   * These locks should be acquired whenever getting or deleting files for a segment.
   * If different threads try to get or delete files simultaneously, one of them creates a lock first using
   * {@link #lock(DataSegment)}. And then, all threads compete with each other to get the lock.
   * Finally, the lock should be released using {@link #unlock(DataSegment, ReferenceCountingLock)}.
   *
   * An example usage is:
   *
   * final SegmentCacheEntry entry = new SegmentCacheEntry(segment);
   * final ReferenceCountingLock lock = entry.lock();
   * synchronized (lock) {
   *   try {
   *     doSomething();
   *   }
   *   finally {
   *     entry.unlock(lock);
   *   }
   * }
   */

  private final ConcurrentHashMap<DataSegment, ReferenceCountingLock> segmentLocks = new ConcurrentHashMap<>();

  private final StorageLocationSelectorStrategy strategy;

  private final IndexIO indexIO;

  private ExecutorService loadOnBootstrapExec = null;
  private ExecutorService loadOnDownloadExec = null;
  private final ListeningExecutorService virtualStorageFabricLoadOnDemandExec;

  @Inject
  public SegmentLocalCacheManager(
      List<StorageLocation> locations,
      SegmentLoaderConfig config,
      @Nonnull StorageLocationSelectorStrategy strategy,
      IndexIO indexIO,
      @Json ObjectMapper mapper
  )
  {
    this.config = config;
    this.jsonMapper = mapper;
    this.locations = locations;
    this.strategy = strategy;
    this.indexIO = indexIO;

    log.info("Using storage location strategy[%s].", this.strategy.getClass().getSimpleName());
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

    if (config.isVirtualStorageFabric()) {
      log.info(
          "Using virtual storage fabric mode - on demand load threads: min[%d] max[%s] keepAlive[%d].",
          config.getMinVirtualStorageFabricLoadThreads(),
          config.getMaxVirtualStorageFabricLoadThreads(),
          config.getVirtualStorageFabricLoadThreadKeepaliveMillis()
      );
      virtualStorageFabricLoadOnDemandExec = MoreExecutors.listeningDecorator(
          Execs.newBlockingCached(
              "VirtualStorageFabricOnDemandLoadingThread-%s",
              config.getMinVirtualStorageFabricLoadThreads(),
              config.getMaxVirtualStorageFabricLoadThreads(),
              config.getVirtualStorageFabricLoadThreadKeepaliveMillis(),
              TimeUnit.MILLISECONDS,
              null
          )
      );
    } else {
      virtualStorageFabricLoadOnDemandExec = null;
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
  public List<DataSegment> getCachedSegments() throws IOException
  {
    if (!canHandleSegments()) {
      throw DruidException.defensive(
          "canHandleSegments() is false. getCachedSegments() must be invoked only when canHandleSegments() returns true."
      );
    }
    final File infoDir = getEffectiveInfoDir();
    FileUtils.mkdirp(infoDir);

    final List<DataSegment> cachedSegments = new ArrayList<>();
    final File[] segmentsToLoad = infoDir.listFiles();

    int ignored = 0;

    for (int i = 0; i < segmentsToLoad.length; i++) {
      final File file = segmentsToLoad[i];
      log.info("Loading segment cache file [%d/%d][%s].", i + 1, segmentsToLoad.length, file);
      try {
        final DataSegment segment = jsonMapper.readValue(file, DataSegment.class);
        boolean removeInfo = false;
        if (!segment.getId().toString().equals(file.getName())) {
          log.warn("Ignoring cache file[%s] for segment[%s].", file.getPath(), segment.getId());
          ignored++;
        } else {
          removeInfo = true;
          final SegmentCacheEntry cacheEntry = new SegmentCacheEntry(segment);
          for (StorageLocation location : locations) {
            if (cacheEntry.checkExists(location.getPath())) {
              removeInfo = false;
              final boolean reserveResult;
              if (config.isVirtualStorageFabric()) {
                reserveResult = location.reserveWeak(cacheEntry);
                // todo (clint): go ahead and mount for now, but this is wack... it should happen on the correct pool
                //  instead of this thread, but at least we aren't pulling from deep storage...
                location.getCacheEntry(cacheEntry.id).mount(location.getPath());
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
        }

        if (removeInfo) {
          final SegmentId segmentId = segment.getId();
          log.warn("Unable to find cache file for segment[%s]. Deleting lookup entry.", segmentId);
          removeInfoFile(segment);
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to load segment from segment cache file.")
           .addData("file", file)
           .emit();
      }
    }

    if (ignored > 0) {
      log.makeAlert("Ignored misnamed segment cache files on startup.")
         .addData("numIgnored", ignored)
         .emit();
    }

    return cachedSegments;
  }

  @Override
  public void storeInfoFile(DataSegment segment) throws IOException
  {
    final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), segment.getId().toString());
    if (!segmentInfoCacheFile.exists()) {
      jsonMapper.writeValue(segmentInfoCacheFile, segment);
    }
  }

  @Override
  public void removeInfoFile(DataSegment segment)
  {
    final File segmentInfoCacheFile = new File(getEffectiveInfoDir(), segment.getId().toString());
    if (!segmentInfoCacheFile.delete()) {
      log.warn("Unable to delete cache file[%s] for segment[%s].", segmentInfoCacheFile, segment.getId());
    }
  }

  @Override
  public Optional<Segment> acquireSegment(DataSegment dataSegment)
  {
    final SegmentCacheEntryIdentifier cacheEntryIdentifier = new SegmentCacheEntryIdentifier(dataSegment.getId());
    for (StorageLocation location : locations) {
      if (location.isReserved(cacheEntryIdentifier)) {
        final SegmentCacheEntry cacheEntry = location.getCacheEntry(cacheEntryIdentifier);
        return cacheEntry.referenceProvider.acquireReference();
      }
    }
    return Optional.empty();
  }

  @Override
  public AcquireSegmentAction acquireSegment(
      DataSegment dataSegment,
      SegmentDescriptor descriptor
  ) throws SegmentLoadingException
  {

    final ReferenceCountingLock lock = lock(dataSegment);
    final SegmentCacheEntryIdentifier identifier = new SegmentCacheEntryIdentifier(dataSegment.getId());

    final Closeable cleanup = () -> {
      // todo (clint): this can be better probably?
      for (StorageLocation location : locations) {
        location.finishWeakReservationHold(Collections.singletonList(identifier));
      }
    };

    synchronized (lock) {
      try {
        for (StorageLocation location : locations) {
          final SegmentCacheEntry entry = location.addWeakReservationHoldIfExists(identifier);
          if (entry != null && entry.referenceProvider != null) {
            return new AcquireSegmentAction(
                descriptor,
                () -> Futures.immediateFuture(entry.referenceProvider.acquireReference()),
                cleanup
            );
          }
        }
        final Iterator<StorageLocation> iterator = strategy.getLocations();
        while (iterator.hasNext()) {
          final StorageLocation location = iterator.next();
          final SegmentCacheEntry entry = location.addWeakReservationHold(
              identifier,
              () -> new SegmentCacheEntry(dataSegment)
          );
          if (entry != null) {
            return new AcquireSegmentAction(
                descriptor,
                () -> SegmentLocalCacheManager.this.virtualStorageFabricLoadOnDemandExec.submit(
                    () -> {
                      final ReferenceCountingLock threadLock = lock(dataSegment);
                      synchronized (threadLock) {
                        try {
                          entry.mount(location.getPath());
                          return entry.referenceProvider.acquireReference();
                        }
                        finally {
                          unlock(dataSegment, threadLock);
                        }
                      }
                    }
                ),
                cleanup
            );
          }
        }
        throw new SegmentLoadingException(
            "Unable to load segment[%s] on demand, ensure enough disk space has been allocated to load all segments involved in the query",
            dataSegment.getId()
        );
      }
      catch (SegmentLoadingException e) {
        CloseableUtils.closeAndWrapExceptions(cleanup);
        throw e;
      }
      catch (Throwable e) {
        throw CloseableUtils.closeAndWrapInCatch(e, cleanup);
      }
      finally {
        unlock(dataSegment, lock);
      }
    }
  }

  @Override
  public void load(final DataSegment dataSegment) throws SegmentLoadingException
  {
    if (config.isVirtualStorageFabric()) {
      // no-op, we'll do a load when someone asks for the segment
      return;
    }
    final SegmentCacheEntry cacheEntry = new SegmentCacheEntry(dataSegment);
    final ReferenceCountingLock lock = lock(dataSegment);
    synchronized (lock) {
      try {
        final SegmentCacheEntry entry = assignLocationAndMount(cacheEntry, SegmentLazyLoadFailCallback.NOOP);
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
    if (config.isVirtualStorageFabric()) {
      return;
    }
    final SegmentCacheEntry cacheEntry = new SegmentCacheEntry(dataSegment);
    final ReferenceCountingLock lock = lock(dataSegment);
    synchronized (lock) {
      try {
        final SegmentCacheEntry entry = assignLocationAndMount(cacheEntry, loadFailed);
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
  public File getSegmentFiles(DataSegment segment)
  {
    final SegmentCacheEntry cacheEntry = new SegmentCacheEntry(segment);
    final ReferenceCountingLock lock = lock(segment);
    synchronized (lock) {
      try {
        for (StorageLocation location : locations) {
          final SegmentCacheEntry entry = location.getCacheEntry(cacheEntry.id);
          if (entry != null) {
            return entry.storageDir;
          }
        }
      }
      finally {
        unlock(segment, lock);
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

  @Override
  public void drop(DataSegment segment)
  {
    final SegmentCacheEntry cacheEntry = new SegmentCacheEntry(segment);
    final ReferenceCountingLock lock = lock(segment);
    synchronized (lock) {
      try {
        // always unmount on cleanup to unmap the segment
        cacheEntry.unmount();
        if (!config.isDeleteOnRemove()) {
          return;
        }
        for (StorageLocation location : locations) {
          if (cacheEntry.checkExists(location.getPath())) {
            cleanupCacheFiles(location.getPath(), new File(location.getPath(), getSegmentDir(segment)));
            location.release(cacheEntry);
          }
        }
      }
      finally {
        unlock(segment, lock);
      }
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

  @VisibleForTesting
  public ConcurrentHashMap<DataSegment, ReferenceCountingLock> getSegmentLocks()
  {
    return segmentLocks;
  }

  @VisibleForTesting
  public List<StorageLocation> getLocations()
  {
    return locations;
  }

  /**
   * Checks whether a segment is already cached.
   */
  boolean isSegmentCached(final DataSegment segment)
  {
    final SegmentCacheEntry cacheEntry = new SegmentCacheEntry(segment);
    for (StorageLocation location : locations) {
      if (cacheEntry.checkExists(location.getPath())) {
        return true;
      }
    }
    return false;
  }

  private ReferenceCountingLock lock(DataSegment dataSegment)
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

  private void unlock(DataSegment dataSegment, ReferenceCountingLock lock)
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

  private SegmentCacheEntry assignLocationAndMount(
      SegmentCacheEntry cacheEntry,
      SegmentLazyLoadFailCallback segmentLoadFailCallback
  ) throws SegmentLoadingException
  {
    try {
      for (StorageLocation location : locations) {
        if (cacheEntry.checkExists(location.getPath())) {
          if (location.isReserved(cacheEntry.id) || location.reserve(cacheEntry)) {
            final SegmentCacheEntry entry = location.getCacheEntry(cacheEntry.id);
            entry.lazyLoadCallback = segmentLoadFailCallback;
            entry.mount(location.getPath());
            return entry;
          } else {
            cleanupCacheFiles(location.getPath(), cacheEntry.toPotentialLocation(location.getPath()));
          }
        }
      }
    }
    catch (IOException | SegmentLoadingException e) {
      log.warn(e, "Failed to load segment[%s] in existing location, trying new location", cacheEntry.id);
    }
    final Iterator<StorageLocation> locationsIterator = strategy.getLocations();
    while (locationsIterator.hasNext()) {
      final StorageLocation location = locationsIterator.next();
      if (location.reserve(cacheEntry)) {
        try {
          final SegmentCacheEntry entry = location.getCacheEntry(cacheEntry.id);
          entry.lazyLoadCallback = segmentLoadFailCallback;
          entry.mount(location.getPath());
          return entry;
        }
        catch (IOException | SegmentLoadingException e) {
          log.warn(e, "Failed to load segment[%s] in location[%s], trying next location", cacheEntry.id, location.getPath());
        }
      }
    }
    throw new SegmentLoadingException("Failed to load segment[%s] in all locations.", cacheEntry.id);
  }

  private void cleanupCacheFiles(File baseFile, File cacheFile)
  {
    if (cacheFile.equals(baseFile)) {
      return;
    }

    directoryWriteRemoveLock.writeLock().lock();
    try {
      log.info("Deleting directory[%s]", cacheFile);
      try {
        FileUtils.deleteDirectory(cacheFile);
      }
      catch (Exception e) {
        log.error(e, "Unable to remove directory[%s]", cacheFile);
      }

      File parent = cacheFile.getParentFile();
      if (parent != null) {
        File[] children = parent.listFiles();
        if (children == null || children.length == 0) {
          cleanupCacheFiles(baseFile, parent);
        }
      }
    }
    finally {
      directoryWriteRemoveLock.writeLock().unlock();
    }
  }

  private static String getSegmentDir(DataSegment segment)
  {
    return DataSegmentPusher.getDefaultStorageDir(segment, false);
  }

  /**
   * check if segment data is possibly corrupted.
   * @param dir segments cache dir
   * @return true means segment files may be damaged.
   */
  private static boolean isPossiblyCorrupted(File dir)
  {
    return hasStartMarker(dir);
  }

  /**
   * If {@link #DOWNLOAD_START_MARKER_FILE_NAME} exists in the path, the segment files might be damaged because this
   * file is typically deleted after the segment is pulled from deep storage.
   */
  private static boolean hasStartMarker(File localStorageDir)
  {
    final File downloadStartMarker = new File(localStorageDir.getPath(), DOWNLOAD_START_MARKER_FILE_NAME);
    return downloadStartMarker.exists();
  }

  private static class ReferenceCountingLock
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

  private class SegmentCacheEntry implements CacheEntry
  {
    private final SegmentCacheEntryIdentifier id;
    private final DataSegment dataSegment;
    private final String relativePathString;
    private SegmentLazyLoadFailCallback lazyLoadCallback = SegmentLazyLoadFailCallback.NOOP;
    private File locationRoot;
    private File storageDir;
    private ReferenceCountedSegmentProvider referenceProvider;

    private SegmentCacheEntry(DataSegment dataSegment)
    {
      this.dataSegment = dataSegment;
      this.id = new SegmentCacheEntryIdentifier(dataSegment.getId());
      this.relativePathString = getSegmentDir(dataSegment);
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
      return referenceProvider != null;
    }

    public boolean checkExists(File location)
    {
      return toPotentialLocation(location).exists();
    }

    public File toPotentialLocation(File location)
    {
      return new File(location, relativePathString);
    }

    @Override
    public void mount(File location) throws IOException, SegmentLoadingException
    {
      if (locationRoot != null) {
        log.debug("already mounted [%s] in location[%s], but asked to load in [%s], unmounting old location", id, locationRoot, location);
        if (!locationRoot.equals(location)) {
          unmount();
        } else {
          log.debug("already mounted [%s] in location[%s]", id, location);
          return;
        }
      }
      locationRoot = location;
      storageDir = new File(location, relativePathString);
      try {
        boolean needsLoad = true;
        if (storageDir.exists()) {
          if (isPossiblyCorrupted(storageDir)) {
            log.warn(
                "[%s] may be damaged. Delete all the segment files and pull from DeepStorage again.",
                storageDir.getAbsolutePath()
            );
            cleanupCacheFiles(locationRoot, storageDir);
          } else {
            needsLoad = false;
          }
        }
        if (needsLoad) {
          loadInLocationWithStartMarker(dataSegment, storageDir);
        }
        final SegmentizerFactory factory = getSegmentFactory(storageDir);

        final Segment segment = factory.factorize(dataSegment, storageDir, false, lazyLoadCallback);
        // wipe load callback after calling
        lazyLoadCallback = SegmentLazyLoadFailCallback.NOOP;
        referenceProvider = ReferenceCountedSegmentProvider.of(segment);
      }
      catch (SegmentLoadingException e) {
        try {
          log.makeAlert(
              e,
              "Failed to load segment in current location [%s], try next location if any",
              location.getAbsolutePath()
          ).addData("location", location.getAbsolutePath()).emit();

          throw new SegmentLoadingException(
              "Failed to load segment[%s] in reserved location[%s]", dataSegment.getId(), location.getAbsolutePath()
          );
        }
        finally {
          unmount();
        }
      }
    }

    @Override
    public void unmount()
    {
      if (referenceProvider != null) {
        referenceProvider.close();
        referenceProvider = null;
      }
      if (!config.isDeleteOnRemove()) {
        return;
      }
      if (storageDir != null) {
        cleanupCacheFiles(locationRoot, storageDir);
        storageDir = null;
        locationRoot = null;
      }
    }

    public void loadIntoPageCache()
    {
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

    private void loadInLocationWithStartMarker(DataSegment segment, File storageDir) throws SegmentLoadingException
    {
      // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
      // the parent directories of the segment are removed
      final File downloadStartMarker = new File(storageDir, DOWNLOAD_START_MARKER_FILE_NAME);
      directoryWriteRemoveLock.readLock().lock();
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
      finally {
        directoryWriteRemoveLock.readLock().unlock();
      }
    }

    private void loadInLocation(DataSegment segment, File storageDir) throws SegmentLoadingException
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
      SegmentCacheEntry that = (SegmentCacheEntry) o;
      return Objects.equals(dataSegment, that.dataSegment);
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(dataSegment);
    }
  }

}
