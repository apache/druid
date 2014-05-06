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

package io.druid.server.coordinator.rules;

import com.google.api.client.util.Lists;
import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.druid.client.DruidServer;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadPeonCallback;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ReplicationThrottler;
import io.druid.server.coordinator.SegmentReplicantLookup;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

/**
 */
public class LoadRuleTest
{
  private LoadQueuePeon mockPeon;
  private ReplicationThrottler throttler;
  private DataSegment segment;

  @Before
  public void setUp() throws Exception
  {
    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    throttler = new ReplicationThrottler(2, 1);
    for (String tier : Arrays.asList("hot", DruidServer.DEFAULT_TIER)) {
      throttler.updateReplicationState(tier);
      throttler.updateTerminationState(tier);
    }
    segment = new DataSegment(
        "foo",
        new Interval("0/3000"),
        new DateTime().toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        new NoneShardSpec(),
        0,
        0
    );
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testLoad() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    LoadRule rule = new LoadRule()
    {
      private final Map<String, Integer> tiers = ImmutableMap.of(
          "hot", 1,
          DruidServer.DEFAULT_TIER, 2
      );

      @Override
      public Map<String, Integer> getTieredReplicants()
      {
        return tiers;
      }

      @Override
      public int getNumReplicants(String tier)
      {
        return tiers.get(tier);
      }

      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
      {
        return true;
      }

      @Override
      public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
      {
        return true;
      }
    };

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverHot",
                            "hostHot",
                            1000,
                            "historical",
                            "hot",
                            0
                        ),
                        mockPeon
                    )
                )
            ),
            DruidServer.DEFAULT_TIER,
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            DruidServer.DEFAULT_TIER,
                            0
                        ),
                        mockPeon
                    )
                )
            )
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
                                     .withReplicationManager(throttler)
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 1);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get(DruidServer.DEFAULT_TIER).get() == 2);
  }

  @Test
  public void testDrop() throws Exception
  {
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.replay(mockPeon);

    LoadRule rule = new LoadRule()
    {
      private final Map<String, Integer> tiers = ImmutableMap.of(
          "hot", 0,
          DruidServer.DEFAULT_TIER, 0
      );

      @Override
      public Map<String, Integer> getTieredReplicants()
      {
        return tiers;
      }

      @Override
      public int getNumReplicants(String tier)
      {
        return tiers.get(tier);
      }

      @Override
      public String getType()
      {
        return "test";
      }

      @Override
      public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
      {
        return true;
      }

      @Override
      public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
      {
        return true;
      }
    };

    DruidServer server1 = new DruidServer(
        "serverHot",
        "hostHot",
        1000,
        "historical",
        "hot",
        0
    );
    server1.addDataSegment(segment.getIdentifier(), segment);
    DruidServer server2 = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        DruidServer.DEFAULT_TIER,
        0
    );
    server2.addDataSegment(segment.getIdentifier(), segment);
    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1,
                        mockPeon
                    )
                )
            ),
            DruidServer.DEFAULT_TIER,
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server2,
                        mockPeon
                    )
                )
            )
        )
    );


    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("hot").get() == 1);
    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get(DruidServer.DEFAULT_TIER).get() == 1);
  }
}
