/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.client.BatchServerInventoryView;
import io.druid.client.DruidServer;
import io.druid.client.ServerView;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.announcement.Announcer;
import io.druid.db.DatabaseSegmentManager;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.realtime.DbSegmentPublisher;
import io.druid.server.DruidNode;
import io.druid.server.coordination.BatchDataSegmentAnnouncer;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class DruidClusterBridgeTest
{
  @Test
  public void testRun() throws Exception
  {
    TestingCluster localCluster = new TestingCluster(1);
    localCluster.start();

    CuratorFramework localCf = CuratorFrameworkFactory.builder()
                                                      .connectString(localCluster.getConnectString())
                                                      .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                                      .compressionProvider(
                                                          new PotentiallyGzippedCompressionProvider(
                                                              false
                                                          )
                                                      )
                                                      .build();
    localCf.start();


    TestingCluster remoteCluster = new TestingCluster(1);
    remoteCluster.start();

    CuratorFramework remoteCf = CuratorFrameworkFactory.builder()
                                                       .connectString(remoteCluster.getConnectString())
                                                       .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                                       .compressionProvider(
                                                           new PotentiallyGzippedCompressionProvider(
                                                               false
                                                           )
                                                       )
                                                       .build();
    remoteCf.start();

    ObjectMapper jsonMapper = new DefaultObjectMapper();
    DruidClusterBridgeConfig config = new DruidClusterBridgeConfig()
    {
      @Override
      public String getTier()
      {
        return DruidServer.DEFAULT_TIER;
      }

      @Override
      public Duration getStartDelay()
      {
        return new Duration(0);
      }

      @Override
      public Duration getPeriod()
      {
        return new Duration(Long.MAX_VALUE);
      }

      @Override
      public String getBrokerServiceName()
      {
        return "testz0rz";
      }

      @Override
      public int getPriority()
      {
        return 0;
      }
    };

    ScheduledExecutorFactory factory = ScheduledExecutors.createFactory(new Lifecycle());

    DruidNode me = new DruidNode(
        "me",
        "localhost",
        8080
    );

    AtomicReference<LeaderLatch> leaderLatch = new AtomicReference<>(new LeaderLatch(localCf, "/test"));

    ZkPathsConfig zkPathsConfig = new ZkPathsConfig()
    {
      @Override
      public String getZkBasePath()
      {
        return "/druid";
      }
    };
    DruidServerMetadata metadata = new DruidServerMetadata(
        "test",
        "localhost",
        1000,
        "bridge",
        DruidServer.DEFAULT_TIER,
        0
    );
    DbSegmentPublisher dbSegmentPublisher = EasyMock.createMock(DbSegmentPublisher.class);
    EasyMock.replay(dbSegmentPublisher);
    DatabaseSegmentManager databaseSegmentManager = EasyMock.createMock(DatabaseSegmentManager.class);
    EasyMock.replay(databaseSegmentManager);
    ServerView serverView = EasyMock.createMock(ServerView.class);
    EasyMock.replay(serverView);

    BridgeZkCoordinator bridgeZkCoordinator = new BridgeZkCoordinator(
        jsonMapper,
        zkPathsConfig,
        new SegmentLoaderConfig(),
        metadata,
        remoteCf,
        dbSegmentPublisher,
        databaseSegmentManager,
        serverView
    );

    Announcer announcer = new Announcer(remoteCf, Executors.newSingleThreadExecutor());
    announcer.start();
    announcer.announce(zkPathsConfig.getAnnouncementsPath() + "/" + me.getHost(), jsonMapper.writeValueAsBytes(me));

    BatchDataSegmentAnnouncer batchDataSegmentAnnouncer = EasyMock.createMock(BatchDataSegmentAnnouncer.class);
    BatchServerInventoryView batchServerInventoryView = EasyMock.createMock(BatchServerInventoryView.class);
    EasyMock.expect(batchServerInventoryView.getInventory()).andReturn(
        Arrays.asList(
            new DruidServer("1", "localhost", 117, "historical", DruidServer.DEFAULT_TIER, 0),
            new DruidServer("2", "localhost", 1, "historical", DruidServer.DEFAULT_TIER, 0)
        )
    );
    batchServerInventoryView.registerSegmentCallback(
        EasyMock.<Executor>anyObject(),
        EasyMock.<ServerView.SegmentCallback>anyObject()
    );
    batchServerInventoryView.registerServerCallback(
        EasyMock.<Executor>anyObject(),
        EasyMock.<ServerView.ServerCallback>anyObject()
    );
    EasyMock.expectLastCall();
    batchServerInventoryView.start();
    EasyMock.expectLastCall();
    batchServerInventoryView.stop();
    EasyMock.expectLastCall();
    EasyMock.replay(batchServerInventoryView);

    DruidClusterBridge bridge = new DruidClusterBridge(
        jsonMapper,
        config,
        factory,
        me,
        localCf,
        leaderLatch,
        bridgeZkCoordinator,
        announcer,
        batchDataSegmentAnnouncer,
        batchServerInventoryView
    );

    bridge.start();

    int retry = 0;
    while (!bridge.isLeader()) {
      if (retry > 5) {
        throw new ISE("Unable to become leader");
      }

      Thread.sleep(100);
      retry++;
    }

    String path = "/druid/announcements/localhost:8080";
    retry = 0;
    while (remoteCf.checkExists().forPath(path) == null) {
      if (retry > 5) {
        throw new ISE("Unable to announce");
      }

      Thread.sleep(100);
      retry++;
    }

    boolean verified = verifyUpdate(jsonMapper, path, remoteCf);
    retry = 0;
    while (!verified) {
      if (retry > 5) {
        throw new ISE("No updates to bridge node occurred");
      }

      Thread.sleep(100);
      retry++;

      verified = verifyUpdate(jsonMapper, path, remoteCf);
    }

    announcer.stop();
    bridge.stop();

    remoteCf.close();
    remoteCluster.close();
    localCf.close();
    localCluster.close();

    EasyMock.verify(batchServerInventoryView);
    EasyMock.verify(dbSegmentPublisher);
    EasyMock.verify(databaseSegmentManager);
    EasyMock.verify(serverView);
  }

  private boolean verifyUpdate(ObjectMapper jsonMapper, String path, CuratorFramework remoteCf) throws Exception
  {
    DruidServerMetadata announced = jsonMapper.readValue(
        remoteCf.getData().forPath(path),
        DruidServerMetadata.class
    );

    return (118 == announced.getMaxSize());
  }
}
