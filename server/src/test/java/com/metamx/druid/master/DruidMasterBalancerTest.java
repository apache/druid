/*
* Druid - a distributed column store.
* Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.master;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class DruidMasterBalancerTest
{
  private DruidMaster master;
  private DruidServer druidServerHigh;
  private DruidServer druidServerLow;
  private DataSegment segment1;
  private DataSegment segment2;
  private DataSegment segment3;
  private DataSegment segment4;
  private LoadQueuePeon peon;
  private DruidDataSource dataSource;

  @Before
  public void setUp() throws Exception
  {
    master = EasyMock.createMock(DruidMaster.class);
    druidServerHigh = EasyMock.createMock(DruidServer.class);
    druidServerLow = EasyMock.createMock(DruidServer.class);
    segment1 = EasyMock.createMock(DataSegment.class);
    segment2 = EasyMock.createMock(DataSegment.class);
    segment3 = EasyMock.createMock(DataSegment.class);
    segment4 = EasyMock.createMock(DataSegment.class);
    peon = EasyMock.createMock(LoadQueuePeon.class);
    dataSource = EasyMock.createMock(DruidDataSource.class);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(master);
    EasyMock.verify(druidServerHigh);
    EasyMock.verify(druidServerLow);
    EasyMock.verify(segment1);
    EasyMock.verify(segment2);
    EasyMock.verify(segment3);
    EasyMock.verify(segment4);
    EasyMock.verify(peon);
    EasyMock.verify(dataSource);
  }

  @Test
  public void testRun()
  {
    // Mock some servers of different usages
    EasyMock.expect(druidServerHigh.getName()).andReturn("from").atLeastOnce();
    EasyMock.expect(druidServerHigh.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServerHigh.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServerHigh.getDataSources()).andReturn(Arrays.asList(dataSource)).atLeastOnce();
    EasyMock.replay(druidServerHigh);

    EasyMock.expect(druidServerLow.getName()).andReturn("to").atLeastOnce();
    EasyMock.expect(druidServerLow.getTier()).andReturn("normal").atLeastOnce();
    EasyMock.expect(druidServerLow.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(druidServerLow.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServerLow.getDataSources()).andReturn(Arrays.asList(dataSource)).anyTimes();
    EasyMock.expect(druidServerLow.getSegment("segment1")).andReturn(null).anyTimes();
    EasyMock.expect(druidServerLow.getSegment("segment2")).andReturn(null).anyTimes();
    EasyMock.expect(druidServerLow.getSegment("segment3")).andReturn(null).anyTimes();
    EasyMock.expect(druidServerLow.getSegment("segment4")).andReturn(null).anyTimes();
    EasyMock.replay(druidServerLow);

    // Mock a datasource
    EasyMock.expect(dataSource.getSegments()).andReturn(
        Sets.<DataSegment>newHashSet(
            segment1,
            segment2,
            segment3,
            segment4
        )
    ).atLeastOnce();
    EasyMock.replay(dataSource);

    // Mock some segments of different sizes
    EasyMock.expect(segment1.getSize()).andReturn(11L).anyTimes();
    EasyMock.expect(segment1.getIdentifier()).andReturn("segment1").anyTimes();
    EasyMock.replay(segment1);
    EasyMock.expect(segment2.getSize()).andReturn(7L).anyTimes();
    EasyMock.expect(segment2.getIdentifier()).andReturn("segment2").anyTimes();
    EasyMock.replay(segment2);
    EasyMock.expect(segment3.getSize()).andReturn(4L).anyTimes();
    EasyMock.expect(segment3.getIdentifier()).andReturn("segment3").anyTimes();
    EasyMock.replay(segment3);
    EasyMock.expect(segment4.getSize()).andReturn(8L).anyTimes();
    EasyMock.expect(segment4.getIdentifier()).andReturn("segment4").anyTimes();
    EasyMock.replay(segment4);

    // Mock stuff that the master needs
    master.moveSegment(
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(master);

    EasyMock.expect(peon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(peon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.replay(peon);

    DruidMasterRuntimeParams params =
        DruidMasterRuntimeParams.newBuilder()
                                .withDruidCluster(
                                    new DruidCluster(
                                        ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                            "normal",
                                            MinMaxPriorityQueue.orderedBy(DruidMasterBalancer.percentUsedComparator)
                                                               .create(
                                                                   Arrays.asList(
                                                                       new ServerHolder(druidServerHigh, peon),
                                                                       new ServerHolder(druidServerLow, peon)
                                                                   )
                                                               )
                                        )
                                    )
                                )
                                .withLoadManagementPeons(ImmutableMap.of("from", peon, "to", peon))
                                .build();

    params = new DruidMasterBalancer(master, new BalancerAnalyzer()).run(params);
    Assert.assertTrue(params.getMasterStats().getPerTierStats().get("movedCount").get("normal").get() > 0);
  }
}
