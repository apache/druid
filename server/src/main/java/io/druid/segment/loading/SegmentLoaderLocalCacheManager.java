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
import io.druid.guice.annotations.Json;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.segment.IndexIO;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
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
    @Override
    public int compare(StorageLocation left, StorageLocation right)
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
      locations.add(new StorageLocation(
          locationConfig.getPath().toPath(),
          locationConfig.getMaxSize(),
          locationConfig.getFreeSpacePercent()
      ));
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

  private StorageLocation findStorageLocationIfLoaded(final DataSegment segment)
  {
    for (StorageLocation location : getSortedList(locations)) {
      Path localStorageDir = location.getPath().resolve(DataSegmentPusher.getDefaultStorageDir(segment, false));
      if (Files.exists(localStorageDir)) {
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
    String storageDir = DataSegmentPusher.getDefaultStorageDir(segment, false);

    if (loc == null) {
      loc = loadSegmentWithRetry(segment, storageDir);
    }
    loc.addSegment(segment);
    return loc.getPath().resolve(storageDir).toFile();
  }

  /**
   * location may fail because of IO failure, most likely in two cases:<p>
   * 1. druid don't have the write access to this location, most likely the administrator doesn't config it correctly<p>
   * 2. disk failure, druid can't read/write to this disk anymore
   */
  private StorageLocation loadSegmentWithRetry(DataSegment segment, String storageDirStr) throws SegmentLoadingException
  {
    for (StorageLocation loc : getSortedList(locations)) {
      if (loc.canHandle(segment)) {
        Path storageDir = loc.getPath().resolve(storageDirStr);

        try {
          loadInLocationWithStartMarker(segment, storageDir);
          return loc;
        }
        catch (SegmentLoadingException e) {
          log.makeAlert(
              e,
              "Failed to load segment in current location %s, try next location if any",
              loc.getPath().toAbsolutePath()
          )
             .addData("location", loc.getPath().toAbsolutePath())
             .emit();

          cleanupCacheFiles(loc.getPath(), storageDir);
        }
      }
    }
    throw new SegmentLoadingException("Failed to load segment %s in all locations.", segment.getIdentifier());
  }

  private void loadInLocationWithStartMarker(DataSegment segment, Path storageDir) throws SegmentLoadingException
  {
    // We use a marker to prevent the case where a segment is downloaded, but before the download completes,
    // the parent directories of the segment are removed
    final Path downloadStartMarker = storageDir.resolve("downloadStartMarker");
    synchronized (lock) {
      try {
        Files.createDirectories(storageDir);
      }
      catch (IOException e) {
        log.warn(e, "Unable to make parent file[%s]", storageDir);
      }
      try {
        Files.createFile(downloadStartMarker);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to create marker file for [%s]", storageDir);
      }
    }
    loadInLocation(segment, storageDir);

    try {
      Files.delete(downloadStartMarker);
    }
    catch (IOException e) {
      throw new SegmentLoadingException("Unable to remove marker file for [%s]", storageDir);
    }
  }

  private void loadInLocation(DataSegment segment, Path storageDir) throws SegmentLoadingException
  {
    // LoadSpec isn't materialized until here so that any system can interpret Segment without having to have all the
    // LoadSpec dependencies.
    final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
    final LoadSpec.LoadSpecResult result = loadSpec.loadSegment(storageDir.toFile());
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
  public void cleanup(DataSegment segment)
  {
    if (!config.isDeleteOnRemove()) {
      return;
    }

    StorageLocation loc = findStorageLocationIfLoaded(segment);

    if (loc == null) {
      log.info("Asked to cleanup something[%s] that didn't exist.  Skipping.", segment);
      return;
    }

    // If storageDir.mkdirs() success, but downloadStartMarker.createNewFile() failed,
    // in this case, findStorageLocationIfLoaded() will think segment is located in the failed storageDir which is actually not.
    // So we should always clean all possible locations here
    for (StorageLocation location : getSortedList(locations)) {
      Path localStorageDir = location.getPath().resolve(DataSegmentPusher.getDefaultStorageDir(segment, false));
      if (Files.exists(localStorageDir)) {
        // Druid creates folders of the form dataSource/interval/version/partitionNum.
        // We need to clean up all these directories if they are all empty.
        cleanupCacheFiles(location.getPath(), localStorageDir);
        location.removeSegment(segment);
      }
    }
  }

  private void cleanupCacheFiles(Path baseFile, Path cacheFile)
  {
    if (cacheFile.equals(baseFile)) {
      return;
    }

    synchronized (lock) {
      log.info("Deleting directory[%s]", cacheFile);
      try {
        Files.walkFileTree(cacheFile, new SimpleFileVisitor<Path>()
        {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException
          {
            Files.delete(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException
          {
            Files.delete(dir);
            return FileVisitResult.CONTINUE;
          }
        });
      }
      catch (IOException e) {
        log.error("Unable to remove directory[%s]", cacheFile);
      }
    }

    Path parent = cacheFile.getParent();
    if (parent != null) {
      long children;
      try {
        children = Files.list(parent).count();
      }
      catch (IOException e) {
        children = 0;
      }
      if (children == 0) {
        cleanupCacheFiles(baseFile, parent);
      }
    }
  }

  private List<StorageLocation> getSortedList(List<StorageLocation> locs)
  {
    List<StorageLocation> locations = new ArrayList<>(locs);
    Collections.sort(locations, COMPARATOR);

    return locations;
  }
}
