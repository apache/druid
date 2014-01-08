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

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 */
public class ZkCoordinator extends BaseZkCoordinator
{
  private static final EmittingLogger log = new EmittingLogger(ZkCoordinator.class);

  private final ObjectMapper jsonMapper;
  private final SegmentLoaderConfig config;
  private final DataSegmentAnnouncer announcer;
  private final ServerManager serverManager;

  @Inject
  public ZkCoordinator(
      ObjectMapper jsonMapper,
      SegmentLoaderConfig config,
      ZkPathsConfig zkPaths,
      DruidServerMetadata me,
      DataSegmentAnnouncer announcer,
      CuratorFramework curator,
      ServerManager serverManager
  )
  {
    super(jsonMapper, zkPaths, me, curator);

    this.jsonMapper = jsonMapper;
    this.config = config;
    this.announcer = announcer;
    this.serverManager = serverManager;
  }

  @Override
  public void loadLocalCache()
  {
    final long start = System.currentTimeMillis();
    File baseDir = config.getInfoDir();
    if (!baseDir.exists() && !config.getInfoDir().mkdirs()) {
      return;
    }

    List<DataSegment> cachedSegments = Lists.newArrayList();
    for (File file : baseDir.listFiles()) {
      log.info("Loading segment cache file [%s]", file);
      try {
        DataSegment segment = jsonMapper.readValue(file, DataSegment.class);
        if (serverManager.isSegmentCached(segment)) {
          cachedSegments.add(segment);
        } else {
          log.warn("Unable to find cache file for %s. Deleting lookup entry", segment.getIdentifier());

          File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
          if (!segmentInfoCacheFile.delete()) {
            log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
          }
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to load segment from segmentInfo file")
           .addData("file", file)
           .emit();
      }
    }

    addSegments(
        cachedSegments,
        new DataSegmentChangeCallback()
        {
          @Override
          public void execute()
          {
            log.info("Cache load took %,d ms", System.currentTimeMillis() - start);
          }
        }
    );
  }

  @Override
  public DataSegmentChangeHandler getDataSegmentChangeHandler()
  {
    return ZkCoordinator.this;
  }

  @Override
  public void addSegment(DataSegment segment, DataSegmentChangeCallback callback)
  {
    try {
      log.info("Loading segment %s", segment.getIdentifier());

      final boolean loaded;
      try {
        loaded = serverManager.loadSegment(segment);
      }
      catch (Exception e) {
        removeSegment(segment, callback);
        throw new SegmentLoadingException(e, "Exception loading segment[%s]", segment.getIdentifier());
      }

      if (loaded) {
        File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
        if (!segmentInfoCacheFile.exists()) {
          try {
            jsonMapper.writeValue(segmentInfoCacheFile, segment);
          }
          catch (IOException e) {
            removeSegment(segment, callback);
            throw new SegmentLoadingException(
                e, "Failed to write to disk segment info cache file[%s]", segmentInfoCacheFile
            );
          }
        }

        try {
          announcer.announceSegment(segment);
        }
        catch (IOException e) {
          throw new SegmentLoadingException(e, "Failed to announce segment[%s]", segment.getIdentifier());
        }
      }
    }
    catch (SegmentLoadingException e) {
      log.makeAlert(e, "Failed to load segment for dataSource")
         .addData("segment", segment)
         .emit();
    }
    finally {
      callback.execute();
    }
  }

  public void addSegments(Iterable<DataSegment> segments, DataSegmentChangeCallback callback)
  {
    try {
      final List<String> segmentFailures = Lists.newArrayList();
      final List<DataSegment> validSegments = Lists.newArrayList();

      for (DataSegment segment : segments) {
        log.info("Loading segment %s", segment.getIdentifier());

        final boolean loaded;
        try {
          loaded = serverManager.loadSegment(segment);
        }
        catch (Exception e) {
          log.error(e, "Exception loading segment[%s]", segment.getIdentifier());
          removeSegment(segment, callback);
          segmentFailures.add(segment.getIdentifier());
          continue;
        }

        if (loaded) {
          File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
          if (!segmentInfoCacheFile.exists()) {
            try {
              jsonMapper.writeValue(segmentInfoCacheFile, segment);
            }
            catch (IOException e) {
              log.error(e, "Failed to write to disk segment info cache file[%s]", segmentInfoCacheFile);
              removeSegment(segment, callback);
              segmentFailures.add(segment.getIdentifier());
              continue;
            }
          }

          validSegments.add(segment);
        }
      }

      try {
        announcer.announceSegments(validSegments);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Failed to announce segments[%s]", segments);
      }

      if (!segmentFailures.isEmpty()) {
        for (String segmentFailure : segmentFailures) {
          log.error("%s failed to load", segmentFailure);
        }
        throw new SegmentLoadingException("%,d errors seen while loading segments", segmentFailures.size());
      }
    }
    catch (SegmentLoadingException e) {
      log.makeAlert(e, "Failed to load segments for dataSource")
         .addData("segments", segments)
         .emit();
    }
    finally {
      callback.execute();
    }
  }


  @Override
  public void removeSegment(DataSegment segment, DataSegmentChangeCallback callback)
  {
    try {
      serverManager.dropSegment(segment);

      File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
      if (!segmentInfoCacheFile.delete()) {
        log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
      }

      announcer.unannounceSegment(segment);
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to remove segment")
         .addData("segment", segment)
         .emit();
    }
    finally {
      callback.execute();
    }
  }
}
