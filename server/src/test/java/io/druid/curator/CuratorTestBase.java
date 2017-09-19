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

package io.druid.curator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.druid.client.DruidServer;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

/**
 */
public class CuratorTestBase
{
  protected TestingServer server;
  protected Timing timing;
  protected CuratorFramework curator;

  protected void setupServerAndCurator() throws Exception
  {
    server = new TestingServer();
    timing = new Timing();
    curator = CuratorFrameworkFactory
        .builder()
        .connectString(server.getConnectString())
        .sessionTimeoutMs(timing.session())
        .connectionTimeoutMs(timing.connection())
        .retryPolicy(new RetryOneTime(1))
        .compressionProvider(new PotentiallyGzippedCompressionProvider(true))
        .build();

  }

  protected void setupZNodeForServer(DruidServer server, ZkPathsConfig zkPathsConfig, ObjectMapper jsonMapper)
  {
    final String announcementsPath = zkPathsConfig.getAnnouncementsPath();
    final String inventoryPath = zkPathsConfig.getLiveSegmentsPath();

    try {
      curator.create()
             .creatingParentsIfNeeded()
             .forPath(
                 ZKPaths.makePath(announcementsPath, server.getHost()),
                 jsonMapper.writeValueAsBytes(server.getMetadata())
             );
      curator.create()
             .creatingParentsIfNeeded()
             .forPath(ZKPaths.makePath(inventoryPath, server.getHost()));
    }
    catch (KeeperException.NodeExistsException e) {
      /*
       * For some reason, Travis build sometimes fails here because of
       * org.apache.zookeeper.KeeperException$NodeExistsException: KeeperErrorCode = NodeExists, though it should never
       * happen because zookeeper should be in a clean state for each run of tests.
       * Address issue: https://github.com/druid-io/druid/issues/1512
       */
      try {
        curator.setData()
               .forPath(
                   ZKPaths.makePath(announcementsPath, server.getHost()),
                   jsonMapper.writeValueAsBytes(server.getMetadata())
               );
        curator.setData()
               .forPath(ZKPaths.makePath(inventoryPath, server.getHost()));
      }
      catch (Exception e1) {
        Throwables.propagate(e1);
      }
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  protected void announceSegmentForServer(
      DruidServer druidServer,
      DataSegment segment,
      ZkPathsConfig zkPathsConfig,
      ObjectMapper jsonMapper
  )
  {
    final String segmentAnnouncementPath = ZKPaths.makePath(ZKPaths.makePath(
        zkPathsConfig.getLiveSegmentsPath(),
        druidServer.getHost()
    ), segment.getIdentifier());

    try {
      curator.create()
             .compressed()
             .withMode(CreateMode.EPHEMERAL)
             .forPath(
                 segmentAnnouncementPath,
                 jsonMapper.writeValueAsBytes(
                     ImmutableSet.<DataSegment>of(segment)
                 )
             );
    }
    catch (KeeperException.NodeExistsException e) {
      try {
        curator.setData()
               .forPath(
                   segmentAnnouncementPath,
                   jsonMapper.writeValueAsBytes(ImmutableSet.<DataSegment>of(segment))
               );
      }
      catch (Exception e1) {
        Throwables.propagate(e1);
      }
    }
    catch (Exception e) {
      Throwables.propagate(e);
    }
  }

  protected void unannounceSegmentForServer(DruidServer druidServer, DataSegment segment, ZkPathsConfig zkPathsConfig)
      throws Exception
  {
    curator.delete().guaranteed().forPath(
        ZKPaths.makePath(
            ZKPaths.makePath(zkPathsConfig.getLiveSegmentsPath(), druidServer.getHost()),
            segment.getIdentifier()
        )
    );
  }

  protected void tearDownServerAndCurator()
  {
    try {
      curator.close();
      server.close();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

}

//Build at Tue Dec 22 21:30:00 CST 2015
