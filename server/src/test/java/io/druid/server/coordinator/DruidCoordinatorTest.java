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

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import io.druid.client.DruidServer;
import io.druid.client.ImmutableDruidServer;
import io.druid.client.SingleServerInventoryView;
import io.druid.curator.discovery.NoopServiceAnnouncer;
import io.druid.curator.inventory.InventoryManagerConfig;
import io.druid.metadata.MetadataSegmentManager;
import io.druid.server.DruidNode;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.timeline.DataSegment;
import org.apache.curator.framework.CuratorFramework;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.ConcurrentMap;

/**
 */
public class DruidCoordinatorTest
{
  private DruidCoordinator coordinator;
  private CuratorFramework curator;
  private LoadQueueTaskMaster taskMaster;
  private MetadataSegmentManager databaseSegmentManager;
  private SingleServerInventoryView serverInventoryView;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private DruidServer druidServer;
  private DruidServer druidServer2;
  private DataSegment segment;
  private ConcurrentMap<String, LoadQueuePeon> loadManagementPeons;
  private LoadQueuePeon loadQueuePeon;

  @Before
  public void setUp() throws Exception
  {
    druidServer = EasyMock.createMock(DruidServer.class);
    druidServer2 = EasyMock.createMock(DruidServer.class);
    segment = EasyMock.createNiceMock(DataSegment.class);
    loadQueuePeon = EasyMock.createNiceMock(LoadQueuePeon.class);
    loadManagementPeons = new MapMaker().makeMap();
    serverInventoryView = EasyMock.createMock(SingleServerInventoryView.class);

    databaseSegmentManager = EasyMock.createNiceMock(MetadataSegmentManager.class);
    EasyMock.replay(databaseSegmentManager);

    scheduledExecutorFactory = EasyMock.createNiceMock(ScheduledExecutorFactory.class);
    EasyMock.replay(scheduledExecutorFactory);

    coordinator = new DruidCoordinator(
        new DruidCoordinatorConfig()
        {
          @Override
          public Duration getCoordinatorStartDelay()
          {
            return null;
          }

          @Override
          public Duration getCoordinatorPeriod()
          {
            return null;
          }

          @Override
          public Duration getCoordinatorIndexingPeriod()
          {
            return null;
          }
        },
        new ZkPathsConfig()
        {

          @Override
          public String getBase()
          {
            return "";
          }
        },
        null,
        databaseSegmentManager,
        serverInventoryView,
        null,
        curator,
        new NoopServiceEmitter(),
        scheduledExecutorFactory,
        null,
        taskMaster,
        new NoopServiceAnnouncer(),
        new DruidNode("hey", "what", 1234),
        loadManagementPeons
    );
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(druidServer);
    EasyMock.verify(druidServer2);
    EasyMock.verify(loadQueuePeon);
    EasyMock.verify(serverInventoryView);
  }

  @Test
  public void testMoveSegment() throws Exception
  {
    EasyMock.expect(druidServer.toImmutableDruidServer()).andReturn(
        new ImmutableDruidServer(
            new DruidServerMetadata("from", null, 5L, null, null, 0),
            1L,
            null,
            ImmutableMap.of("dummySegment", segment)
        )
    ).atLeastOnce();
    EasyMock.replay(druidServer);

    EasyMock.expect(druidServer2.toImmutableDruidServer()).andReturn(
        new ImmutableDruidServer(
            new DruidServerMetadata("to", null, 5L, null, null, 0),
            1L,
            null,
            ImmutableMap.of("dummySegment2", segment)
        )
    ).atLeastOnce();
    EasyMock.replay(druidServer2);

    loadManagementPeons.put("from", loadQueuePeon);
    loadManagementPeons.put("to", loadQueuePeon);

    EasyMock.expect(loadQueuePeon.getLoadQueueSize()).andReturn(new Long(1));
    EasyMock.replay(loadQueuePeon);

    EasyMock.expect(serverInventoryView.getInventoryManagerConfig()).andReturn(
        new InventoryManagerConfig()
        {
          @Override
          public String getContainerPath()
          {
            return "";
          }

          @Override
          public String getInventoryPath()
          {
            return "";
          }
        }
    );
    EasyMock.replay(serverInventoryView);

    coordinator.moveSegment(
        druidServer.toImmutableDruidServer(),
        druidServer2.toImmutableDruidServer(),
        "dummySegment", null
    );
  }
}
