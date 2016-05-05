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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.CostBalancerStrategyFactory;
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
  private static final Logger log = new Logger(LoadRuleTest.class);
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private static final ServiceEmitter emitter = new ServiceEmitter(
      "service",
      "host",
      new LoggingEmitter(
          log,
          LoggingEmitter.Level.ERROR,
          jsonMapper
      )
  );

  private LoadQueuePeon mockPeon;
  private ReplicationThrottler throttler;
  private DataSegment segment;


  @Before
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(emitter);
    emitter.start();
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
                        ).toImmutableDruidServer(),
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
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    CostBalancerStrategyFactory costBalancerStrategyFactory = new CostBalancerStrategyFactory(1);
    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategyFactory(costBalancerStrategyFactory)
                                     .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 1);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get(DruidServer.DEFAULT_TIER).get() == 2);
    costBalancerStrategyFactory.close();
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
                        server1.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            ),
            DruidServer.DEFAULT_TIER,
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );

    CostBalancerStrategyFactory costBalancerStrategyFactory = new CostBalancerStrategyFactory(1);
    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategyFactory(costBalancerStrategyFactory)
                                     .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("hot").get() == 1);
    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get(DruidServer.DEFAULT_TIER).get() == 1);
    costBalancerStrategyFactory.close();
  }

  @Test
  public void testLoadWithNonExistentTier() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    LoadRule rule = new LoadRule()
    {
      private final Map<String, Integer> tiers = ImmutableMap.of(
          "nonExistentTier", 1,
          "hot", 1
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
                        ).toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );
CostBalancerStrategyFactory costBalancerStrategyFactory = new CostBalancerStrategyFactory(1);
    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategyFactory(costBalancerStrategyFactory)
                                     .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 1);
    costBalancerStrategyFactory.close();
  }

  @Test
  public void testDropWithNonExistentTier() throws Exception
  {
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.replay(mockPeon);

    LoadRule rule = new LoadRule()
    {
      private final Map<String, Integer> tiers = ImmutableMap.of(
          "nonExistentTier", 1,
          "hot", 1
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
    DruidServer server2 = new DruidServer(
        "serverHo2t",
        "hostHot2",
        1000,
        "historical",
        "hot",
        0
    );
    server1.addDataSegment(segment.getIdentifier(), segment);
    server2.addDataSegment(segment.getIdentifier(), segment);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "hot",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1.toImmutableDruidServer(),
                        mockPeon
                    ),
                    new ServerHolder(
                        server2.toImmutableDruidServer(),
                        mockPeon
                    )
                )
            )
        )
    );
    CostBalancerStrategyFactory costBalancerStrategyFactory = new CostBalancerStrategyFactory(1);

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategyFactory(costBalancerStrategyFactory)
                                     .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("hot").get() == 1);
    costBalancerStrategyFactory.close();
  }
}
