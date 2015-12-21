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
import com.metamx.common.guava.CloseQuietly;
import io.druid.client.DruidServer;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.curator.utils.ZKPaths;

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
      throws Exception
  {
    final String announcementsPath = zkPathsConfig.getAnnouncementsPath();
    final String inventoryPath = zkPathsConfig.getLiveSegmentsPath();

    final String zNodePathAnnounce = ZKPaths.makePath(announcementsPath, server.getHost());
    final String zNodePathSegment = ZKPaths.makePath(inventoryPath, server.getHost());

    /*
     * Explicitly check whether the zNodes we are about to create exist or not,
     * if exist, delete them to make sure we have a clean state on zookeeper.
     * Address issue: https://github.com/druid-io/druid/issues/1512
     */
    if (curator.checkExists().forPath(zNodePathAnnounce) != null) {
      curator.delete().guaranteed().forPath(zNodePathAnnounce);
    }
    if (curator.checkExists().forPath(zNodePathSegment) != null) {
      curator.delete().guaranteed().forPath(zNodePathSegment);
    }

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

  protected void tearDownServerAndCurator()
  {
    CloseQuietly.close(curator);
    try {
      server.stop();
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }
    CloseQuietly.close(server);
  }
}
