/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.loading;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.QueryableIndex;
import com.metamx.druid.index.QueryableIndexSegment;
import com.metamx.druid.index.Segment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * TODO: Kill this along with the Guicification of the IndexingService stuff
 */
@Deprecated
public class SingleSegmentLoader implements SegmentLoader
{
  private static final Logger log = new Logger(SingleSegmentLoader.class);

  private final DataSegmentPuller dataSegmentPuller;
  private final QueryableIndexFactory factory;

  private final List<StorageLocation> locations;

  @Inject
  public SingleSegmentLoader(
      DataSegmentPuller dataSegmentPuller,
      QueryableIndexFactory factory,
      SegmentLoaderConfig config
  )
  {
    this.dataSegmentPuller = dataSegmentPuller;
    this.factory = factory;

    final ImmutableList.Builder<StorageLocation> locBuilder = ImmutableList.builder();

    // TODO
    // This is a really, really stupid way of getting this information.  Splitting on commas and bars is error-prone
    // We should instead switch it up to be a JSON Array of JSON Object or something and cool stuff like that
    // But, that'll have to wait for some other day.
    for (String dirSpec : config.getLocations().split(",")) {
      String[] dirSplit = dirSpec.split("\\|");
      if (dirSplit.length == 1) {
        locBuilder.add(new StorageLocation(new File(dirSplit[0]), Integer.MAX_VALUE));
      }
      else if (dirSplit.length == 2) {
        final Long maxSize = Longs.tryParse(dirSplit[1]);
        if (maxSize == null) {
          throw new IAE("Size of a local segment storage location must be an integral number, got[%s]", dirSplit[1]);
        }
        locBuilder.add(new StorageLocation(new File(dirSplit[0]), maxSize));
      }
      else {
        throw new ISE(
            "Unknown segment storage location[%s]=>[%s], config[%s].",
            dirSplit.length, dirSpec, config.getLocations()
        );
      }
    }
    locations = locBuilder.build();

    Preconditions.checkArgument(locations.size() > 0, "Must have at least one segment cache directory.");
    log.info("Using storage locations[%s]", locations);
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

      dataSegmentPuller.getSegmentFiles(segment, storageDir);
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

  private static class StorageLocation
  {
    private final File path;
    private final long maxSize;
    private final Set<DataSegment> segments;

    private volatile long currSize = 0;

    StorageLocation(
        File path,
        long maxSize
    )
    {
      this.path = path;
      this.maxSize = maxSize;

      this.segments = Sets.newHashSet();
    }

    private File getPath()
    {
      return path;
    }

    private Long getMaxSize()
    {
      return maxSize;
    }

    private synchronized void addSegment(DataSegment segment)
    {
      if (segments.add(segment)) {
        currSize += segment.getSize();
      }
    }

    private synchronized void removeSegment(DataSegment segment)
    {
      if (segments.remove(segment)) {
        currSize -= segment.getSize();
      }
    }

    private boolean canHandle(long size)
    {
      return available() > size;
    }

    private synchronized long available()
    {
      return maxSize - currSize;
    }

    private StorageLocation mostEmpty(StorageLocation other)
    {
      return available() > other.available() ? this : other;
    }

    @Override
    public String toString()
    {
      return "StorageLocation{" +
             "path=" + path +
             ", maxSize=" + maxSize +
             '}';
    }
  }
}
