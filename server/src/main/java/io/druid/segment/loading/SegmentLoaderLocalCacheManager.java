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
import com.google.inject.Inject;

import io.druid.guice.annotations.Json;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 */
public class SegmentLoaderLocalCacheManager implements SegmentLoader
{
  private static final Logger log = new Logger(SegmentLoaderLocalCacheManager.class);

  private final QueryableIndexFactory factory;
  private final SegmentLoaderConfig config;
  private final ObjectMapper jsonMapper;

  private final List<StorageLocation> locations;

  private final Object lock = new Object();

  @Inject
  public SegmentLoaderLocalCacheManager(
      QueryableIndexFactory factory,
      SegmentLoaderConfig config,
      @Json ObjectMapper mapper
  )
  {
    this.factory = factory;
    this.config = config;
    this.jsonMapper = mapper;

    this.locations = Lists.newArrayList();
    for (StorageLocationConfig locationConfig : config.getLocations()) {
      locations.add(new StorageLocation(locationConfig.getPath(), locationConfig.getMaxSize()));
    }
  }

  public SegmentLoaderLocalCacheManager withConfig(SegmentLoaderConfig config)
  {
    return new SegmentLoaderLocalCacheManager(factory, config, jsonMapper);
  }

  @Override
  public boolean isSegmentLoaded(final DataSegment segment)
  {
    return findStorageLocationIfLoaded(segment) != null;
  }

  public StorageLocation findStorageLocationIfLoaded(final DataSegment segment)
  {
    for (StorageLocation location : locations) {
      File localStorageDir = new File(location.getPath(), DataSegmentPusherUtil.getStorageDir(segment));
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
    final QueryableIndex index = factory.factorize(segmentFiles);

    return new QueryableIndexSegment(segment.getIdentifier(), index);
  }

  @Override
  public File getSegmentFiles(DataSegment segment) throws SegmentLoadingException
  {
    StorageLocation loc = findStorageLocationIfLoaded(segment);

    final File retVal;

    if (loc == null) {
      Iterator<StorageLocation> locIter = locations.iterator();
      loc = locIter.next();
      while (locIter.hasNext()) {
        loc = loc.mostEmpty(locIter.next());
      }

      if (!loc.canHandle(segment.getSize())) {
        throw new ISE(
            "Segment[%s:%,d] too large for storage[%s:%,d].",
            segment.getIdentifier(), segment.getSize(), loc.getPath(), loc.available()
        );
      }

      File storageDir = new File(loc.getPath(), DataSegmentPusherUtil.getStorageDir(segment));

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

      // LoadSpec isn't materialized until here so that any system can interpret Segment without having to have all the LoadSpec dependencies.
      final LoadSpec loadSpec = jsonMapper.convertValue(segment.getLoadSpec(), LoadSpec.class);
      final LoadSpec.LoadSpecResult result = loadSpec.loadSegment(storageDir);
      if(result.getSize() != segment.getSize()){
        log.warn("Segment [%s] is different than expected size. Expected [%d] found [%d]", segment.getIdentifier(), segment.getSize(), result.getSize());
      }

      if (!downloadStartMarker.delete()) {
        throw new SegmentLoadingException("Unable to remove marker file for [%s]", storageDir);
      }

      retVal = storageDir;
    } else {
      retVal = new File(loc.getPath(), DataSegmentPusherUtil.getStorageDir(segment));
    }

    loc.addSegment(segment);

    return retVal;
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
      // Druid creates folders of the form dataSource/interval/version/partitionNum.
      // We need to clean up all these directories if they are all empty.
      File cacheFile = new File(loc.getPath(), DataSegmentPusherUtil.getStorageDir(segment));
      cleanupCacheFiles(loc.getPath(), cacheFile);
      loc.removeSegment(segment);
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

    if (cacheFile.getParentFile() != null && cacheFile.getParentFile().listFiles().length == 0) {
      cleanupCacheFiles(baseFile, cacheFile.getParentFile());
    }
  }
}
