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

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;

import java.io.IOException;

/**
 * Creates paths for announcing served segments on Zookeeper.
 *
 * @deprecated as Druid has already migrated to HTTP-based segment loading and
 * will soon migrate to HTTP-based inventory view using {@code SegmentListerResource}.
 *
 * @see org.apache.druid.server.http.SegmentListerResource
 */
@Deprecated
public class ZkCoordinator
{
  private static final EmittingLogger log = new EmittingLogger(ZkCoordinator.class);

  private final Object lock = new Object();

  private final ZkPathsConfig zkPaths;
  private final DruidServerMetadata me;
  private final CuratorFramework curator;
  private final BatchDataSegmentAnnouncerConfig announcerConfig;

  private volatile boolean started = false;

  @Inject
  public ZkCoordinator(
      ZkPathsConfig zkPaths,
      DruidServerMetadata me,
      CuratorFramework curator,
      BatchDataSegmentAnnouncerConfig announcerConfig
  )
  {
    this.zkPaths = zkPaths;
    this.me = me;
    this.curator = curator;
    this.announcerConfig = announcerConfig;
  }

  @LifecycleStart
  public void start() throws IOException
  {
    synchronized (lock) {
      if (started) {
        return;
      }

      log.info("Starting zkCoordinator for server[%s]", me.getName());

      if (announcerConfig.isSkipSegmentAnnouncementOnZk()) {
        log.info("Skipping zkPath creation as segment announcement on ZK is disabled.");
        started = true;
        return;
      }

      final String liveSegmentsLocation = ZKPaths.makePath(zkPaths.getLiveSegmentsPath(), me.getName());
      log.info("Creating zkPath[%s] for announcing live segments.", liveSegmentsLocation);

      try {
        curator.newNamespaceAwareEnsurePath(liveSegmentsLocation).ensure(curator.getZookeeperClient());
      }
      catch (Exception e) {
        Throwables.propagateIfPossible(e, IOException.class);
        throw new RuntimeException(e);
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

      started = false;
    }
  }
}
