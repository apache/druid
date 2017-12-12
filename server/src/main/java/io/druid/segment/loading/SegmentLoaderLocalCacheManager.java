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

package io.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.guice.annotations.Json;
import io.druid.segment.IndexIO;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 */
public class SegmentLoaderLocalCacheManager implements SegmentLoader
{
  private static final EmittingLogger log = new EmittingLogger(SegmentLoaderLocalCacheManager.class);

  private final IndexIO indexIO;
  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  private final List<StorageLocation> locations;

  private final Object lock = new Object();

  private static final Comparator<StorageLocation> COMPARATOR = new Comparator<StorageLocation>()
  {
    @Override public int compare(StorageLocation left, StorageLocation right)
    {
      return Longs.compare(right.available(), left.available());
    }
  };

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

    this.locations = Lists.newArrayList();
    for (StorageLocationConfig locationConfig : config.getLocations()) {
      locations.add(new StorageLocation(locationConfig.getPath(), locationConfig.getMaxSize()));
    }
  }

  public SegmentLoaderLocalCacheManager withConfig(SegmentLoaderConfig config)
  {
    return new SegmentLoaderLocalCacheManager(indexIO, config, jsonMapper);
  }

  @Override
  public boolean isSegmentLoaded(final DataSegment segment)
  {
    return findStorageLocationIfLoaded(segment) != null;
  }

  public StorageLocation findStorageLocationIfLoaded(final DataSegment segment)
  {
    for (StorageLocation location : getSortedList(locations)) {
      File localStorageDir = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment));
      if (localStorageDir.exists()) {
        return location;
      }
    }
    return null;
  }

  @Override
  public Segment getSegment(DataSegment segment) throws SegmentLoadingException
  {
    File segmentFiles = getSegmentFiles(segment);
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
    StorageLocation loc = findStorageLocationIfLoaded(segment);
    String storageDir = DataSegmentPusher.getDefaultStorageDir(segment);

    if (loc == null) {
      loc = loadSegmentWithRetry(segment, storageDir);
    }
    loc.addSegment(segment);
    return new File(loc.getPath(), storageDir);
  }

  /**
   * location may fail because of IO failure, most likely in two cases:<p>
   * 1. druid don't have the write access to this location, most likely the administrator doesn't config it correctly<p>
   * 2. disk failure, druid can't read/write to this disk anymore
   */
  private StorageLocation loadSegmentWithRetry(DataSegment segment, String storageDirStr) throws SegmentLoadingException
  {
    for (StorageLocation loc : getSortedList(locations)) {
      // locIter is ordered from empty to full
      if (!loc.canHandle(segment.getSize())) {
        throw new ISE(
            "Segment[%s:%,d] too large for storage[%s:%,d].",
            segment.getIdentifier(), segment.getSize(), loc.getPath(), loc.available()
        );
      }
      File storageDir = new File(loc.getPath(), storageDirStr);

      try {
        loadInLocationWithStartMarker(segment, storageDir);
        return loc;
      }
      catch (SegmentLoadingException e) {
        log.makeAlert(
            e,
            "Failed to load segment in current location %s, try next location if any",
            loc.getPath().getAbsolutePath()
        )
           .addData("location", loc.getPath().getAbsolutePath())
           .emit();

        try {
          cleanupCacheFiles(loc.getPath(), storageDir);
        }
        catch (IOException e1) {
          log.error(e1, "Failed to cleanup location " + storageDir.getAbsolutePath());
        }
      }
    }
    throw new SegmentLoadingException("Failed to load segment %s in all locations.", segment.getIdentifier());
  }

  private void loadInLocationWithStartMarker(DataSegment segment, File storageDir) throws SegmentLoadingException
  {
    // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
    // the parent directories of the segment are removed
    final File downloadStartMarker = new File(storageDir, "downloadStartMarker");
    synchronized (lock) {
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
          segment.getIdentifier(),
          segment.getSize(),
          result.getSize()
      );
    }
  }

  @Override
  public void cleanup(DataSegment segment) throws SegmentLoadingException
  {
    if (!config.isDeleteOnRemove()) {
      return;
    }

    StorageLocation loc = findStorageLocationIfLoaded(segment);

    if (loc == null) {
      log.info("Asked to cleanup something[%s] that didn't exist.  Skipping.", segment);
      return;
    }

    try {
      // If storageDir.mkdirs() success, but downloadStartMarker.createNewFile() failed,
      // in this case, findStorageLocationIfLoaded() will think segment is located in the failed storageDir which is actually not.
      // So we should always clean all possible locations here
      for (StorageLocation location : getSortedList(locations)) {
        File localStorageDir = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment));
        if (localStorageDir.exists()) {
          // Druid creates folders of the form dataSource/interval/version/partitionNum.
          // We need to clean up all these directories if they are all empty.
          File cacheFile = new File(location.getPath(), DataSegmentPusher.getDefaultStorageDir(segment));
          cleanupCacheFiles(location.getPath(), cacheFile);
          location.removeSegment(segment);
        }
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  public void cleanupCacheFiles(File baseFile, File cacheFile) throws IOException
  {
    if (cacheFile.equals(baseFile)) {
      return;
    }

    synchronized (lock) {
      log.info("Deleting directory[%s]", cacheFile);
      try {
        FileUtils.deleteDirectory(cacheFile);
      }
      catch (Exception e) {
        log.error("Unable to remove file[%s]", cacheFile);
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

  public List<StorageLocation> getSortedList(List<StorageLocation> locs)
  {
    List<StorageLocation> locations = new ArrayList<>(locs);
    Collections.sort(locations, COMPARATOR);

    return locations;
  }
}
