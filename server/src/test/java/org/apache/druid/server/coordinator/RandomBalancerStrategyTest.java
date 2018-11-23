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


package org.apache.druid.server.coordinator;


import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RandomBalancerStrategyTest
{
  private static final Interval day = Intervals.of("2015-01-01T00/2015-01-01T01");
  private static final long MAX_SIZE_PER_HISTORICAL = 10000L;

  /**
   * Create Druid cluster with serverCount servers having maxSegments segments each with currentSize per server is 3000L,
   * and 1 server with (maxSegments - 2) segment with currentSize is 1000L
   * <p>
   * Random Balancer Strategy should rebalance the segment to the server with less PercentUsed.
   * <p>
   * Avg used percent is 0.281818182
   * Used percent of BEST_SERVER is 0.1
   */
  public static List<ServerHolder> setupDummyCluster(int serverCount, int maxSegments)
  {
    List<ServerHolder> serverHolderList = new ArrayList<>();
    // Create 10 servers with current size being 3K & max size being 10K
    // Each having having 20 segments
    for (int i = 0; i < serverCount; i++) {
      LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
      Map<String, DataSegment> segments = new HashMap<>();
      for (int j = 0; j < maxSegments; j++) {
        DataSegment segment = getSegment(j);
        segments.put(segment.getIdentifier(), segment);
      }

      serverHolderList.add(
          new ServerHolder(
              new ImmutableDruidServer(
                  new DruidServerMetadata(
                      "DruidServer_Name_" + i,
                      "localhost",
                      null,
                      MAX_SIZE_PER_HISTORICAL,
                      ServerType.HISTORICAL,
                      "hot",
                      1
                  ),
                  3000L,
                  ImmutableMap.of("DUMMY", EasyMock.createMock(ImmutableDruidDataSource.class)),
                  ImmutableMap.copyOf(segments)
              ),
              fromPeon
          ));
    }

    // The best server to be available for next segment assignment has only 18 Segments and current size is 1K
    LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
    ImmutableDruidServer druidServer = EasyMock.createMock(ImmutableDruidServer.class);
    EasyMock.expect(druidServer.getName()).andReturn("BEST_SERVER").anyTimes();
    EasyMock.expect(druidServer.getCurrSize()).andReturn(1000L).anyTimes();
    EasyMock.expect(druidServer.getMaxSize()).andReturn(MAX_SIZE_PER_HISTORICAL).anyTimes();

    EasyMock.expect(druidServer.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    Map<String, DataSegment> segments = new HashMap<>();
    for (int j = 0; j < (maxSegments - 2); j++) {
      DataSegment segment = getSegment(j);
      segments.put(segment.getIdentifier(), segment);
      EasyMock.expect(druidServer.getSegment(segment.getIdentifier())).andReturn(segment).anyTimes();
    }
    EasyMock.expect(druidServer.getSegments()).andReturn(segments).anyTimes();

    EasyMock.replay(druidServer);
    serverHolderList.add(new ServerHolder(druidServer, fromPeon));
    return serverHolderList;
  }

  /**
   * Returns segment with dummy id and size 100
   *
   * @param index
   *
   * @return segment
   */
  public static DataSegment getSegment(int index)
  {
    return getSegment(index, "DUMMY", day);
  }

  public static DataSegment getSegment(int index, String dataSource, Interval interval)
  {
    // Not using EasyMock as it hampers the performance of multithreads.
    DataSegment segment = new DataSegment(
        dataSource,
        interval,
        String.valueOf(index),
        new ConcurrentHashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        null,
        0,
        index * 100L
    );
    return segment;
  }

  @Test
  public void testFindNewSegmentHomeBalancerSuccess() throws InterruptedException
  {
    List<ServerHolder> serverHolderList = setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);
    // 28.18
    BalancerStrategy strategy = new RandomBalancerStrategy(18);
    ServerHolder holder = strategy.findNewSegmentHomeBalancer(segment, serverHolderList);
    Assert.assertNotNull("Should be able to find a place for new segment!!", holder);
    Assert.assertEquals("Best Server should be BEST_SERVER", "BEST_SERVER", holder.getServer().getName());
  }

  @Test
  public void testFindNewSegmentHomeBalancerFail() throws InterruptedException
  {
    List<ServerHolder> serverHolderList = setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);

    BalancerStrategy strategy = new RandomBalancerStrategy(19);
    ServerHolder holder = strategy.findNewSegmentHomeBalancer(segment, serverHolderList);
    Assert.assertNull("Should not be able to find a place for new segment!!", holder);
  }

}
