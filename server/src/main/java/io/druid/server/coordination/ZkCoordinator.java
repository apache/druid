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
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 */
public class ZkCoordinator implements DataSegmentChangeHandler
{
  private static final EmittingLogger log = new EmittingLogger(ZkCoordinator.class);

  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final SegmentLoaderConfig config;
  private final ZkPathsConfig zkPaths;
  private final DruidServerMetadata me;
  private final DataSegmentAnnouncer announcer;
  private final CuratorFramework curator;
  private final ServerManager serverManager;

  private volatile PathChildrenCache loadQueueCache;
  private volatile boolean started;

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
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.zkPaths = zkPaths;
    this.me = me;
    this.announcer = announcer;
    this.curator = curator;
    this.serverManager = serverManager;
  }

  @LifecycleStart
  public void start() throws IOException
  {
    log.info("Starting zkCoordinator for server[%s]", me);
    synchronized (lock) {
      if (started) {
        return;
      }

      final String loadQueueLocation = ZKPaths.makePath(zkPaths.getLoadQueuePath(), me.getName());
      final String servedSegmentsLocation = ZKPaths.makePath(zkPaths.getServedSegmentsPath(), me.getName());
      final String liveSegmentsLocation = ZKPaths.makePath(zkPaths.getLiveSegmentsPath(), me.getName());

      loadQueueCache = new PathChildrenCache(
          curator,
          loadQueueLocation,
          true,
          true,
          new ThreadFactoryBuilder().setDaemon(true).setNameFormat("ZkCoordinator-%s").build()
      );

      try {
        config.getInfoDir().mkdirs();

        curator.newNamespaceAwareEnsurePath(loadQueueLocation).ensure(curator.getZookeeperClient());
        curator.newNamespaceAwareEnsurePath(servedSegmentsLocation).ensure(curator.getZookeeperClient());
        curator.newNamespaceAwareEnsurePath(liveSegmentsLocation).ensure(curator.getZookeeperClient());

        loadCache();

        loadQueueCache.getListenable().addListener(
            new PathChildrenCacheListener()
            {
              @Override
              public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
              {
                final ChildData child = event.getData();
                switch (event.getType()) {
                  case CHILD_ADDED:
                    final String path = child.getPath();
                    final DataSegmentChangeRequest segment = jsonMapper.readValue(
                        child.getData(), DataSegmentChangeRequest.class
                    );

                    log.info("New node[%s] with segmentClass[%s]", path, segment.getClass());

                    try {
                      segment.go(ZkCoordinator.this);
                      curator.delete().guaranteed().forPath(path);

                      log.info("Completed processing for node[%s]", path);
                    }
                    catch (Exception e) {
                      try {
                        curator.delete().guaranteed().forPath(path);
                      }
                      catch (Exception e1) {
                        log.info(e1, "Failed to delete node[%s], but ignoring exception.", path);
                      }

                      log.makeAlert(e, "Segment load/unload: uncaught exception.")
                         .addData("node", path)
                         .addData("nodeProperties", segment)
                         .emit();
                    }

                    break;
                  case CHILD_REMOVED:
                    log.info("%s was removed", event.getData().getPath());
                    break;
                  default:
                    log.info("Ignoring event[%s]", event);
                }
              }
            }
        );
        loadQueueCache.start();
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw Throwables.propagate(e);
      }

      started = true;
    }
  }

  @LifecycleStop
  public void stop()
  {
    log.info("Stopping ZkCoordinator with config[%s]", config);
    synchronized (lock) {
      if (!started) {
        return;
      }


      try {
        loadQueueCache.close();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      finally {
        loadQueueCache = null;
        started = false;
      }
    }
  }

  private void loadCache()
  {
    File baseDir = config.getInfoDir();
    if (!baseDir.exists()) {
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

    addSegments(cachedSegments);
  }

  @Override
  public void addSegment(DataSegment segment)
  {
    try {
      log.info("Loading segment %s", segment.getIdentifier());

      final boolean loaded;
      try {
        loaded = serverManager.loadSegment(segment);
      }
      catch (Exception e) {
        removeSegment(segment);
        throw new SegmentLoadingException(e, "Exception loading segment[%s]", segment.getIdentifier());
      }

      if (loaded) {
        File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
        if (!segmentInfoCacheFile.exists()) {
          try {
            jsonMapper.writeValue(segmentInfoCacheFile, segment);
          }
          catch (IOException e) {
            removeSegment(segment);
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
  }

  public void addSegments(Iterable<DataSegment> segments)
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
          removeSegment(segment);
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
              removeSegment(segment);
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
  }


  @Override
  public void removeSegment(DataSegment segment)
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
  }

  public void removeSegments(Iterable<DataSegment> segments)
  {
    try {
      for (DataSegment segment : segments) {
        serverManager.dropSegment(segment);

        File segmentInfoCacheFile = new File(config.getInfoDir(), segment.getIdentifier());
        if (!segmentInfoCacheFile.delete()) {
          log.warn("Unable to delete segmentInfoCacheFile[%s]", segmentInfoCacheFile);
        }
      }

      announcer.unannounceSegments(segments);
    }
    catch (Exception e) {
      log.makeAlert(e, "Failed to remove segments")
         .addData("segments", segments)
         .emit();
    }
  }
}
