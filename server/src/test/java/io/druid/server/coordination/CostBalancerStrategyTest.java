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

package io.druid.server.coordination;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.client.ImmutableDruidServer;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CostBalancerStrategy;
import io.druid.server.coordinator.LoadQueuePeonTester;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CostBalancerStrategyTest
{
  private final List<ServerHolder> serverHolderList = Lists.newArrayList();
  private final Interval day = DateTime.now().toDateMidnight().toInterval();

  /**
   * Create Druid cluster with 10 servers having 100 segments each, and 1 server with 98 segment
   * Cost Balancer Strategy should assign the next segment to the server with less segments.
   */
  public void setupDummyCluster(int serverCount, int maxSegments)
  {
    // Create 10 servers with current size being 3K & max size being 10K
    // Each having having 100 segments
    for (int i = 0; i < serverCount; i++) {
      LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
      ImmutableDruidServer druidServer = EasyMock.createMock(ImmutableDruidServer.class);
      EasyMock.expect(druidServer.getName()).andReturn("DruidServer_Name_" + i).anyTimes();
      EasyMock.expect(druidServer.getCurrSize()).andReturn(3000L).anyTimes();
      EasyMock.expect(druidServer.getMaxSize()).andReturn(10000000L).anyTimes();

      EasyMock.expect(druidServer.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
      Map<String, DataSegment> segments = Maps.newHashMap();
      for (int j = 0; j < maxSegments; j++) {
        DataSegment segment = getSegment(j);
        segments.put(segment.getIdentifier(), segment);
        EasyMock.expect(druidServer.getSegment(segment.getIdentifier())).andReturn(segment).anyTimes();
      }

      EasyMock.expect(druidServer.getSegments()).andReturn(segments).anyTimes();

      EasyMock.replay(druidServer);
      serverHolderList.add(new ServerHolder(druidServer, fromPeon));
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
  }

  /**
   * Returns segment with dummy id and size 100
   *
   * @param index
   *
   * @return segment
   */
  private DataSegment getSegment(int index)
  {
    return getSegment(index, "DUMMY", day);
  }

  private DataSegment getSegment(int index, String dataSource, Interval interval)
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
    setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);

    final DateTime referenceTimestamp = new DateTime("2014-01-01");
    BalancerStrategy strategy = new CostBalancerStrategy(referenceTimestamp, 4);
    ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);
    Assert.assertNotNull("Should be able to find a place for new segment!!", holder);
    Assert.assertEquals("Best Server should be BEST_SERVER", "BEST_SERVER", holder.getServer().getName());
  }

  @Test
  public void testCostBalancerSingleThreadStrategy() throws InterruptedException
  {
    setupDummyCluster(10, 20);
    DataSegment segment = getSegment(1000);

    final DateTime referenceTimestamp = new DateTime("2014-01-01");
    BalancerStrategy strategy = new CostBalancerStrategy(referenceTimestamp, 1);
    ServerHolder holder = strategy.findNewSegmentHomeReplicator(segment, serverHolderList);
    Assert.assertNotNull("Should be able to find a place for new segment!!", holder);
    Assert.assertEquals("Best Server should be BEST_SERVER", "BEST_SERVER", holder.getServer().getName());
  }

  @Test
  @Ignore
  public void testBenchmark() throws InterruptedException
  {
    setupDummyCluster(100, 500);
    DataSegment segment = getSegment(1000);

    BalancerStrategy singleThreadStrategy = new CostBalancerStrategy(DateTime.now(DateTimeZone.UTC), 1);
    long start = System.currentTimeMillis();
    singleThreadStrategy.findNewSegmentHomeReplicator(segment, serverHolderList);
    long end = System.currentTimeMillis();
    long latencySingleThread = end - start;

    BalancerStrategy strategy = new CostBalancerStrategy(DateTime.now(DateTimeZone.UTC), 4);
    start = System.currentTimeMillis();
    strategy.findNewSegmentHomeReplicator(segment, serverHolderList);
    end = System.currentTimeMillis();
    long latencyMultiThread = end - start;

    System.err.println("Latency - Single Threaded (ms): " + latencySingleThread);
    System.err.println("Latency - Multi Threaded (ms): " + latencyMultiThread);
  }

  @Test
  public void testComputeJointSegmentCost()
  {
    DateTime referenceTime = new DateTime("2014-01-01T00:00:00");
    CostBalancerStrategy strategy = new CostBalancerStrategy(referenceTime, 4);
    double segmentCost = strategy.computeJointSegmentCosts(
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
                referenceTime.minusDays(2),
                referenceTime.minusDays(2).plusHours(1)
            )
        )
    );
    Assert.assertEquals(138028.62811791385d, segmentCost, 0);

  }

}
