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

package io.druid.server.coordination;

import com.google.api.client.util.Lists;
import com.google.common.collect.Maps;
import io.druid.client.ImmutableDruidServer;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CostBalancerStrategy;
import io.druid.server.coordinator.LoadQueuePeonTester;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.junit.Assert;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
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
    // Not using EasyMock as it hampers the performance of multithreads.
    DataSegment segment = new DataSegment(
        "DUMMY", day, String.valueOf(index), Maps.<String, Object>newConcurrentMap(),
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

  @Test @Ignore
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
}
