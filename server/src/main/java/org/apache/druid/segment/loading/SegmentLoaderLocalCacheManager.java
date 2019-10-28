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
import com.google.inject.Inject;
import org.apache.commons.io.FileUtils;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.Segment;
import org.apache.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class SegmentLoaderLocalCacheManager implements SegmentLoader
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoaderLocalCacheManager.class);

  private final IndexIO indexIO;
  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  private final List<StorageLocation> locations;

  // This directoryWriteRemoveLock is used when creating or removing a directory
  private final Object directoryWriteRemoveLock = new Object();

  /**
   * A map between segment and referenceCountingLocks.
   *
   * These locks should be acquired whenever getting or deleting files for a segment.
   * If different threads try to get or delete files simultaneously, one of them creates a lock first using
   * {@link #createOrGetLock}. And then, all threads compete with each other to get the lock.
   * Finally, the lock should be released using {@link #unlock}.
   *
   * An example usage is:
   *
   * final ReferenceCountingLock lock = createOrGetLock(segment);
   * synchronized (lock) {
   *   try {
   *     doSomething();
   *   }
   *   finally {
   *     unlock(lock);
   *   }
   * }
   */
  private final ConcurrentHashMap<DataSegment, ReferenceCountingLock> segmentLocks = new ConcurrentHashMap<>();

  private final StorageLocationSelectorStrategy strategy;

  // Note that we only create this via injection in historical and realtime nodes. Peons create these
  // objects via SegmentLoaderFactory objects, so that they can store segments in task-specific
  // directories rather than statically configured directories.
  @Inject
  public SegmentLoaderLocalCacheManager(
      IndexIO indexIO,
      SegmentLoaderConfig config,
      @Json ObjectMapper mapper
  )
  {
    this.indexIO = indexIO;
    this.config = config;
    this.jsonMapper = mapper;

    this.locations = new ArrayList<>();
    for (StorageLocationConfig locationConfig : config.getLocations()) {
      locations.add(
          new StorageLocation(
              locationConfig.getPath(),
              locationConfig.getMaxSize(),
              locationConfig.getFreeSpacePercent()
          )
      );
    }
    this.strategy = config.getStorageLocationSelectorStrategy(locations);
  }

  @Override
  public boolean isSegmentLoaded(final DataSegment segment)
  {
    return findStorageLocationIfLoaded(segment) != null;
  }

  private StorageLocation findStorageLocationIfLoaded(final DataSegment segment)
  {
    for (StorageLocation location : locations) {
      File localStorageDir = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment, false));
      if (localStorageDir.exists()) {
        return location;
      }
    }
    return null;
  }

  @Override
  public Segment getSegment(DataSegment segment) throws SegmentLoadingException
  {
    final ReferenceCountingLock lock = createOrGetLock(segment);
    final File segmentFiles;
    synchronized (lock) {
      try {
        segmentFiles = getSegmentFiles(segment);
      }
      finally {
        unlock(segment, lock);
      }
    }
    File factoryJson = new File(segmentFiles, "factory.json");
    final SegmentizerFactory factory;

    if (factoryJson.exists()) {
      try {
        factory = jsonMapper.readValue(factoryJson, SegmentizerFactory.class);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "%s", e.getMessage());
      }
    } else {
      factory = new MMappedQueryableSegmentizerFactory(indexIO);
    }

    return factory.factorize(segment, segmentFiles);
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        StorageLocation loc = findStorageLocationIfLoaded(segment);
        String storageDir = DataSegmentPusher.getDefaultStorageDir(segment, false);

        if (loc == null) {
          loc = loadSegmentWithRetry(segment, storageDir);
        }
        return new File(loc.getPath(), storageDir);
      }
      finally {
        unlock(segment, lock);
      }
    }
  }

  /**
   * location may fail because of IO failure, most likely in two cases:<p>
   * 1. druid don't have the write access to this location, most likely the administrator doesn't config it correctly<p>
   * 2. disk failure, druid can't read/write to this disk anymore
   *
   * Locations are fetched using {@link StorageLocationSelectorStrategy}.
   */
  private StorageLocation loadSegmentWithRetry(DataSegment segment, String storageDirStr) throws SegmentLoadingException
  {
    Iterator<StorageLocation> locationsIterator = strategy.getLocations();

    while (locationsIterator.hasNext()) {

      StorageLocation loc = locationsIterator.next();

      File storageDir = loc.reserve(storageDirStr, segment);
      if (storageDir != null) {
        try {
          loadInLocationWithStartMarker(segment, storageDir);
          return loc;
        }
        catch (SegmentLoadingException e) {
          try {
            log.makeAlert(
                e,
                "Failed to load segment in current location [%s], try next location if any",
                loc.getPath().getAbsolutePath()
            ).addData("location", loc.getPath().getAbsolutePath()).emit();
          }
          finally {
            loc.removeSegmentDir(storageDir, segment);
            cleanupCacheFiles(loc.getPath(), storageDir);
          }
        }
      }
    }
    throw new SegmentLoadingException("Failed to load segment %s in all locations.", segment.getId());
  }

  private void loadInLocationWithStartMarker(DataSegment segment, File storageDir) throws SegmentLoadingException
  {
    // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
    // the parent directories of the segment are removed
    final File downloadStartMarker = new File(storageDir, "downloadStartMarker");
    synchronized (directoryWriteRemoveLock) {
      if (!storageDir.mkdirs()) {
        log.debug("Unable to make parent file[%s]", storageDir);
      }
      try {
        if (!downloadStartMarker.createNewFile()) {
          throw new SegmentLoadingException("Was not able to create new download marker for [%s]", storageDir);
        }
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to create marker file for [%s]", storageDir);
      }
    }
    loadInLocation(segment, storageDir);

    if (!downloadStartMarker.delete()) {
      throw new SegmentLoadingException("Unable to remove marker file for [%s]", storageDir);
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

  @Override
  public void cleanup(DataSegment segment)
  {
    if (!config.isDeleteOnRemove()) {
      return;
    }

    final ReferenceCountingLock lock = createOrGetLock(segment);
    synchronized (lock) {
      try {
        StorageLocation loc = findStorageLocationIfLoaded(segment);

        if (loc == null) {
          log.warn("Asked to cleanup something[%s] that didn't exist.  Skipping.", segment);
          return;
        }

        // If storageDir.mkdirs() success, but downloadStartMarker.createNewFile() failed,
        // in this case, findStorageLocationIfLoaded() will think segment is located in the failed storageDir which is actually not.
        // So we should always clean all possible locations here
        for (StorageLocation location : locations) {
          File localStorageDir = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment, false));
          if (localStorageDir.exists()) {
            // Druid creates folders of the form dataSource/interval/version/partitionNum.
            // We need to clean up all these directories if they are all empty.
            cleanupCacheFiles(location.getPath(), localStorageDir);
            location.removeSegmentDir(localStorageDir, segment);
          }
        }
      }
      finally {
        unlock(segment, lock);
      }
    }
  }

  private void cleanupCacheFiles(File baseFile, File cacheFile)
  {
    if (cacheFile.equals(baseFile)) {
      return;
    }

    synchronized (directoryWriteRemoveLock) {
      log.info("Deleting directory[%s]", cacheFile);
      try {
        FileUtils.deleteDirectory(cacheFile);
      }
      catch (Exception e) {
        log.error(e, "Unable to remove directory[%s]", cacheFile);
      }
    }

    File parent = cacheFile.getParentFile();
    if (parent != null) {
      File[] children = parent.listFiles();
      if (children == null || children.length == 0) {
        cleanupCacheFiles(baseFile, parent);
      }
    }
  }

  private ReferenceCountingLock createOrGetLock(DataSegment dataSegment)
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

  @SuppressWarnings("ObjectEquality")
  private void unlock(DataSegment dataSegment, ReferenceCountingLock lock)
  {
    segmentLocks.compute(
        dataSegment,
        (segment, existingLock) -> {
          if (existingLock == null) {
            throw new ISE("WTH? the given lock has already been removed");
          } else if (existingLock != lock) {
            throw new ISE("WTH? Different lock instance");
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

  @VisibleForTesting
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
}
