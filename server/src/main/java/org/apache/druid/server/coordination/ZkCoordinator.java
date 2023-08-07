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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.initialization.ZkPathsConfig;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * Uses {@link SegmentLoadDropHandler} to perform load and unload of segments
 * based on requests sent via curator.
 *
 * @deprecated Curator-based segment loading has been deprecated and HTTP-based
 * segment loading using {@link org.apache.druid.server.http.SegmentListerResource}
 * is now the default.
 */
@Deprecated
public class ZkCoordinator
{
  private static final EmittingLogger log = new EmittingLogger(ZkCoordinator.class);

  private final Object lock = new Object();

  private final SegmentLoadDropHandler segmentLoadDropHandler;
  private final ObjectMapper jsonMapper;
  private final ZkPathsConfig zkPaths;
  private final DruidServerMetadata me;
  private final CuratorFramework curator;

  @Nullable
  private volatile PathChildrenCache loadQueueCache;
  private volatile boolean started = false;

  @Inject
  public ZkCoordinator(
      SegmentLoadDropHandler loadDropHandler,
      ObjectMapper jsonMapper,
      ZkPathsConfig zkPaths,
      DruidServerMetadata me,
      CuratorFramework curator
  )
  {
    this.segmentLoadDropHandler = loadDropHandler;
    this.jsonMapper = jsonMapper;
    this.zkPaths = zkPaths;
    this.me = me;
    this.curator = curator;
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
          Execs.singleThreaded("ZkCoordinator")
      );

      try {
        curator.newNamespaceAwareEnsurePath(loadQueueLocation).ensure(curator.getZookeeperClient());
        curator.newNamespaceAwareEnsurePath(servedSegmentsLocation).ensure(curator.getZookeeperClient());
        curator.newNamespaceAwareEnsurePath(liveSegmentsLocation).ensure(curator.getZookeeperClient());

        loadQueueCache.getListenable().addListener(
            (client, event) -> {
              final ChildData child = event.getData();
              switch (event.getType()) {
                case CHILD_ADDED:
                  childAdded(child);
                  break;
                case CHILD_REMOVED:
                  log.info("zNode[%s] was removed", event.getData().getPath());
                  break;
                default:
                  log.info("Ignoring event[%s]", event);
              }
            }

        );
        loadQueueCache.start();
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
      }

      started = true;
    }
  }

  private void childAdded(ChildData child)
  {
    final String path = child.getPath();
    try {
      final DataSegmentChangeRequest changeRequest = jsonMapper.readValue(
          child.getData(),
          DataSegmentChangeRequest.class
      );
      segmentLoadDropHandler.submitCuratorRequest(
          changeRequest,
          () -> {
            cleanupPath(path);
            log.info("Completed request[%s]", changeRequest.asString());
          }
      );
    }
    catch (Exception e) {
      // Something went wrong while deserializing the request
      cleanupPath(path);
      log.makeAlert(e, "Segment load/unload: uncaught exception.")
         .addData("node", path)
         .addData("nodeProperties", new SegmentChangeRequestNoop())
         .emit();
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
        throw new RuntimeException(e);
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

  private void cleanupPath(String path)
  {
    try {
      curator.delete().guaranteed().forPath(path);
    }
    catch (Exception e) {
      log.error(e, "Failed to delete zNode[%s], but ignoring exception.", path);
    }
  }
}
