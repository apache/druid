/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.segment.loading;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.Segment;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class OmniSegmentLoader implements SegmentLoader
{
  private static final Logger log = new Logger(OmniSegmentLoader.class);

  private final Map<String, DataSegmentPuller> pullers;
  private final QueryableIndexFactory factory;
  private final SegmentLoaderConfig config;

  private final List<StorageLocation> locations;

  @Inject
  public OmniSegmentLoader(
      Map<String, DataSegmentPuller> pullers,
      QueryableIndexFactory factory,
      SegmentLoaderConfig config
  )
  {
    this.pullers = pullers;
    this.factory = factory;
    this.config = config;

    this.locations = Lists.newArrayList();
    for (StorageLocationConfig locationConfig : config.getLocations()) {
      locations.add(new StorageLocation(locationConfig.getPath(), locationConfig.getMaxSize()));
    }
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
      if (!storageDir.mkdirs()) {
        log.debug("Unable to make parent file[%s]", storageDir);
      }

      getPuller(segment.getLoadSpec()).getSegmentFiles(segment, storageDir);
      loc.addSegment(segment);

      retVal = storageDir;
    }
    else {
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
      File cacheFile = new File(loc.getPath(), DataSegmentPusherUtil.getStorageDir(segment));
      log.info("Deleting directory[%s]", cacheFile);
      FileUtils.deleteDirectory(cacheFile);
      loc.removeSegment(segment);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  private DataSegmentPuller getPuller(Map<String, Object> loadSpec) throws SegmentLoadingException
  {
    String type = MapUtils.getString(loadSpec, "type");
    DataSegmentPuller loader = pullers.get(type);

    if (loader == null) {
      throw new SegmentLoadingException("Unknown loader type[%s].  Known types are %s", type, pullers.keySet());
    }

    return loader;
  }
}
