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

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
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
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class ZkCoordinator
{
  private static final EmittingLogger log = new EmittingLogger(ZkCoordinator.class);

  private final Object lock = new Object();

  private final ObjectMapper jsonMapper;
  private final ZkPathsConfig zkPaths;
  private final SegmentLoaderConfig config;
  private final DruidServerMetadata me;
  private final CuratorFramework curator;
  private final DataSegmentChangeHandler segmentChangeHandler;

  private volatile PathChildrenCache loadQueueCache;
  private volatile boolean started = false;

  @Inject
  public ZkCoordinator(
      ObjectMapper jsonMapper,
      SegmentLoaderConfig config,
      ZkPathsConfig zkPaths,
      DruidServerMetadata me,
      CuratorFramework curator,
      DataSegmentChangeHandler segmentChangeHandler
  )
  {
    this.jsonMapper = jsonMapper;
    this.zkPaths = zkPaths;
    this.config = config;
    this.me = me;
    this.curator = curator;
    this.segmentChangeHandler = segmentChangeHandler;
  }

  @LifecycleStart
  public void start() throws IOException
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Starting zkCoordinator for server[%s]", me.getName());

      final String loadQueueLocation = ZKPaths.makePath(zkPaths.getLoadQueuePath(), me.getName());
      final String servedSegmentsLocation = ZKPaths.makePath(zkPaths.getServedSegmentsPath(), me.getName());
      final String liveSegmentsLocation = ZKPaths.makePath(zkPaths.getLiveSegmentsPath(), me.getName());

      loadQueueCache = new PathChildrenCache(
          curator,
          loadQueueLocation,
          true,
          true,
          Execs.multiThreaded(config.getNumLoadingThreads(), "ZkCoordinator-%s")
      );

      try {
        curator.newNamespaceAwareEnsurePath(loadQueueLocation).ensure(curator.getZookeeperClient());
        curator.newNamespaceAwareEnsurePath(servedSegmentsLocation).ensure(curator.getZookeeperClient());
        curator.newNamespaceAwareEnsurePath(liveSegmentsLocation).ensure(curator.getZookeeperClient());

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
                    final DataSegmentChangeRequest request = jsonMapper.readValue(
                        child.getData(), DataSegmentChangeRequest.class
                    );

                    log.info("New request[%s] with zNode[%s].", request.asString(), path);

                    try {
                      request.go(
                          segmentChangeHandler,
                          new DataSegmentChangeCallback()
                          {
                            boolean hasRun = false;

                            @Override
                            public void execute()
                            {
                              try {
                                if (!hasRun) {
                                  curator.delete().guaranteed().forPath(path);
                                  log.info("Completed request [%s]", request.asString());
                                  hasRun = true;
                                }
                              }
                              catch (Exception e) {
                                try {
                                  curator.delete().guaranteed().forPath(path);
                                }
                                catch (Exception e1) {
                                  log.error(e1, "Failed to delete zNode[%s], but ignoring exception.", path);
                                }
                                log.error(e, "Exception while removing zNode[%s]", path);
                                throw Throwables.propagate(e);
                              }
                            }
                          }
                      );
                    }
                    catch (Exception e) {
                      try {
                        curator.delete().guaranteed().forPath(path);
                      }
                      catch (Exception e1) {
                        log.error(e1, "Failed to delete zNode[%s], but ignoring exception.", path);
                      }

                      log.makeAlert(e, "Segment load/unload: uncaught exception.")
                         .addData("node", path)
                         .addData("nodeProperties", request)
                         .emit();
                    }

                    break;
                  case CHILD_REMOVED:
                    log.info("zNode[%s] was removed", event.getData().getPath());
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
    log.info("Stopping ZkCoordinator for [%s]", me);
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

  public boolean isStarted()
  {
    return started;
  }


}
