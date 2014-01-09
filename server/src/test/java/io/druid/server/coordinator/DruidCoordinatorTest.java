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

package io.druid.server.coordinator;

import com.google.common.collect.MapMaker;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import io.druid.client.DruidServer;
import io.druid.client.SingleServerInventoryView;
import io.druid.curator.discovery.NoopServiceAnnouncer;
import io.druid.curator.inventory.InventoryManagerConfig;
import io.druid.db.DatabaseSegmentManager;
import io.druid.server.DruidNode;
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
  private DatabaseSegmentManager databaseSegmentManager;
  private SingleServerInventoryView serverInventoryView;
  private ScheduledExecutorFactory scheduledExecutorFactory;
  private DruidServer druidServer;
  private DataSegment segment;
  private ConcurrentMap<String, LoadQueuePeon> loadManagementPeons;
  private LoadQueuePeon loadQueuePeon;

  @Before
  public void setUp() throws Exception
  {
    druidServer = EasyMock.createMock(DruidServer.class);
    segment = EasyMock.createNiceMock(DataSegment.class);
    loadQueuePeon = EasyMock.createNiceMock(LoadQueuePeon.class);
    loadManagementPeons = new MapMaker().makeMap();
    serverInventoryView = EasyMock.createMock(SingleServerInventoryView.class);

    databaseSegmentManager = EasyMock.createNiceMock(DatabaseSegmentManager.class);
    EasyMock.replay(databaseSegmentManager);

    scheduledExecutorFactory = EasyMock.createNiceMock(ScheduledExecutorFactory.class);
    EasyMock.replay(scheduledExecutorFactory);

    coordinator = new DruidCoordinator(
        new DruidCoordinatorConfig()
        {
          @Override
          public String getHost()
          {
            return null;
          }

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
        new ZkPathsConfig(){

          @Override
          public String getZkBasePath()
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
    EasyMock.verify(loadQueuePeon);
    EasyMock.verify(serverInventoryView);
  }

  @Test
  public void testMoveSegment() throws Exception
  {
    EasyMock.expect(druidServer.getSegment("dummySegment")).andReturn(segment);
    EasyMock.expect(druidServer.getMaxSize()).andReturn(new Long(5));
    EasyMock.expect(druidServer.getCurrSize()).andReturn(new Long(1)).atLeastOnce();
    EasyMock.expect(druidServer.getName()).andReturn("blah");
    EasyMock.replay(druidServer);

    loadManagementPeons.put("to", loadQueuePeon);
    loadManagementPeons.put("from", loadQueuePeon);

    EasyMock.expect(loadQueuePeon.getLoadQueueSize()).andReturn(new Long(1));
    EasyMock.replay(loadQueuePeon);

    EasyMock.expect(serverInventoryView.getInventoryValue("from")).andReturn(druidServer);
    EasyMock.expect(serverInventoryView.getInventoryValue("to")).andReturn(druidServer);
    EasyMock.expect(serverInventoryView.getInventoryManagerConfig()).andReturn(new InventoryManagerConfig()
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
    });
    EasyMock.replay(serverInventoryView);

    coordinator.moveSegment("from", "to", "dummySegment", null);
  }
}
