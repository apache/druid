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

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class CostBalancerStrategyTest
{
  private static final Interval day = new Interval("2015-01-01T00/2015-01-01T01");

  /**
   * Create Druid cluster with serverCount servers having maxSegments segments each, and 1 server with 98 segment
   * Cost Balancer Strategy should assign the next segment to the server with less segments.
   */
  public static List<ServerHolder> setupDummyCluster(int serverCount, int maxSegments)
  {
    List<ServerHolder> serverHolderList = Lists.newArrayList();
    // Create 10 servers with current size being 3K & max size being 10K
    // Each having having 100 segments
    for (int i = 0; i < serverCount; i++) {
      LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
      Map<String, DataSegment> segments = Maps.newHashMap();
      for (int j = 0; j < maxSegments; j++) {
        DataSegment segment = getSegment(j);
        segments.put(segment.getIdentifier(), segment);
      }

      serverHolderList.add(
          new ServerHolder(
              new ImmutableDruidServer(
                  new DruidServerMetadata("DruidServer_Name_" + i, "localhost", null, 10000000L, ServerType.HISTORICAL, "hot", 1),
                  3000L,
                  ImmutableMap.of("DUMMY", EasyMock.createMock(ImmutableDruidDataSource.class)),
                  ImmutableMap.copyOf(segments)
              ),
              fromPeon
          ));
    }

    // The best server to be available for next segment assignment has only 98 Segments
    LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
    ImmutableDruidServer druidServer = EasyMock.createMock(ImmutableDruidServer.class);
    EasyMock.expect(druidServer.getName()).andReturn("BEST_SERVER").anyTimes();
    EasyMock.expect(druidServer.getCurrSize()).andReturn(3000L).anyTimes();
    EasyMock.expect(druidServer.getMaxSize()).andReturn(10000000L).anyTimes();

    EasyMock.expect(druidServer.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    Map<String, DataSegment> segments = Maps.newHashMap();
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
        dataSource, interval, String.valueOf(index), Maps.<String, Object>newConcurrentMap(),
        Lists.<String>newArrayList(), Lists.<String>newArrayList(), null, 0, index * 100L
    );
    return segment;
  }

  @Test
  public void testCostBalancerMultiThreadedStrategy() throws InterruptedException
  {
    List<ServerHolder> serverHolderList = setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);

    BalancerStrategy strategy = new CostBalancerStrategy(
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4))
    );
    ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);
    Assert.assertNotNull("Should be able to find a place for new segment!!", holder);
    Assert.assertEquals("Best Server should be BEST_SERVER", "BEST_SERVER", holder.getServer().getName());
  }

  @Test
  public void testCostBalancerSingleThreadStrategy() throws InterruptedException
  {
    List<ServerHolder> serverHolderList = setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);

    final DateTime referenceTimestamp = new DateTime("2014-01-01");
    BalancerStrategy strategy = new CostBalancerStrategy(
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1))
    );
    ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);
    Assert.assertNotNull("Should be able to find a place for new segment!!", holder);
    Assert.assertEquals("Best Server should be BEST_SERVER", "BEST_SERVER", holder.getServer().getName());
  }

  @Test
  public void testComputeJointSegmentCost()
  {
    DateTime referenceTime = new DateTime("2014-01-01T00:00:00");
    CostBalancerStrategy strategy = new CostBalancerStrategy(
        MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(4))
    );
    double segmentCost = strategy.computeJointSegmentsCost(
        getSegment(
            100,
            "DUMMY",
            new Interval(
                referenceTime,
                referenceTime.plusHours(1)
            )
        ),
        getSegment(
            101,
            "DUMMY",
            new Interval(
                referenceTime.minusHours(2),
                referenceTime.minusHours(2).plusHours(1)
            )
        )
    );

    Assert.assertEquals(
        CostBalancerStrategy.INV_LAMBDA_SQUARE * CostBalancerStrategy.intervalCost(
            1 * CostBalancerStrategy.LAMBDA,
            -2 * CostBalancerStrategy.LAMBDA,
            -1 * CostBalancerStrategy.LAMBDA
        ) * 2,
        segmentCost, 1e-6);
  }

  @Test
  public void testIntervalCost() throws Exception
  {
    // additivity
    Assert.assertEquals(CostBalancerStrategy.intervalCost(1, 1, 3),
                        CostBalancerStrategy.intervalCost(1, 1, 2) +
                        CostBalancerStrategy.intervalCost(1, 2, 3), 1e-6);

    Assert.assertEquals(CostBalancerStrategy.intervalCost(2, 1, 3),
                        CostBalancerStrategy.intervalCost(2, 1, 2) +
                        CostBalancerStrategy.intervalCost(2, 2, 3), 1e-6);

    Assert.assertEquals(CostBalancerStrategy.intervalCost(3, 1, 2),
                        CostBalancerStrategy.intervalCost(1, 1, 2) +
                        CostBalancerStrategy.intervalCost(1, 0, 1) +
                        CostBalancerStrategy.intervalCost(1, 1, 2), 1e-6);

    // no overlap [0, 1) [1, 2)
    Assert.assertEquals(0.3995764, CostBalancerStrategy.intervalCost(1, 1, 2), 1e-6);
    // no overlap [0, 1) [-1, 0)
    Assert.assertEquals(0.3995764, CostBalancerStrategy.intervalCost(1, -1, 0), 1e-6);

    // exact overlap [0, 1), [0, 1)
    Assert.assertEquals(0.7357589, CostBalancerStrategy.intervalCost(1, 0, 1), 1e-6);
    // exact overlap [0, 2), [0, 2)
    Assert.assertEquals(2.270671, CostBalancerStrategy.intervalCost(2, 0, 2), 1e-6);
    // partial overlap [0, 2), [1, 3)
    Assert.assertEquals(1.681908, CostBalancerStrategy.intervalCost(2, 1, 3), 1e-6);
    // partial overlap [0, 2), [1, 2)
    Assert.assertEquals(1.135335, CostBalancerStrategy.intervalCost(2, 1, 2), 1e-6);
    // partial overlap [0, 2), [0, 1)
    Assert.assertEquals(1.135335, CostBalancerStrategy.intervalCost(2, 0, 1), 1e-6);
    // partial overlap [0, 3), [1, 2)
    Assert.assertEquals(1.534912, CostBalancerStrategy.intervalCost(3, 1, 2), 1e-6);
  }
}
