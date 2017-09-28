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
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
//CHECKSTYLE.OFF: Regexp
import com.metamx.common.logger.Logger;
//CHECKSTYLE.ON: Regexp
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.LoggingEmitter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.DruidServer;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.server.coordination.ServerType;
import io.druid.server.coordinator.BalancerStrategy;
import io.druid.server.coordinator.CoordinatorDynamicConfig;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.CostBalancerStrategyFactory;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.LoadQueuePeonTester;
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
import java.util.Collections;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  private ReplicationThrottler throttler;

  private ListeningExecutorService exec;
  private BalancerStrategy balancerStrategy;

  private BalancerStrategy mockBalancerStrategy;

  @Before
  public void setUp() throws Exception
  {
    EmittingLogger.registerEmitter(emitter);
    emitter.start();
    throttler = EasyMock.createMock(ReplicationThrottler.class);

    exec = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(1));
    balancerStrategy = new CostBalancerStrategyFactory().createBalancerStrategy(exec);

    mockBalancerStrategy = EasyMock.createMock(BalancerStrategy.class);
  }

  @After
  public void tearDown() throws Exception
  {
    exec.shutdown();
    emitter.close();
  }

  @Test
  public void testLoad() throws Exception
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(true).anyTimes();

    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1,
        DruidServer.DEFAULT_TIER, 2
    ));

    final DataSegment segment = createDataSegment("foo");

    throttler.registerReplicantCreation(DruidServer.DEFAULT_TIER, segment.getIdentifier(), "hostNorm");
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(3);

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverHot",
                        "hostHot",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder()))),
            DruidServer.DEFAULT_TIER,
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverNorm",
                        "hostNorm",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        DruidServer.DEFAULT_TIER,
                        0
                    ).toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, DruidServer.DEFAULT_TIER));

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testLoadPriority() throws Exception
  {
    EasyMock.expect(throttler.canCreateReplicant(EasyMock.anyString())).andReturn(false).anyTimes();

    final LoadQueuePeon mockPeon1 = createEmptyPeon();
    final LoadQueuePeon mockPeon2 = createEmptyPeon();

    mockPeon2.loadSegment(EasyMock.anyObject(), EasyMock.isNull());
    EasyMock.expectLastCall().once();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(2);

    EasyMock.replay(throttler, mockPeon1, mockPeon2, mockBalancerStrategy);

    final LoadRule rule = createLoadRule(ImmutableMap.of(
        "tier1", 10,
        "tier2", 10
    ));

    final DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "tier1",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "server1",
                        "host1",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "tier1",
                        0
                    ).toImmutableDruidServer(),
                    mockPeon1
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder()))),
            "tier2",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "server2",
                        "host2",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "tier2",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon2
                ),
                new ServerHolder(
                    new DruidServer(
                        "server3",
                        "host3",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "tier2",
                        1
                    ).toImmutableDruidServer(),
                    mockPeon2
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    final DataSegment segment = createDataSegment("foo");

    final CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(Collections.singletonList(segment)).build(),
        segment
    );

    Assert.assertEquals(0L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier1"));
    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "tier2"));

    EasyMock.verify(throttler, mockPeon1, mockPeon2, mockBalancerStrategy);
  }

  @Test
  public void testDrop() throws Exception
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 0,
        DruidServer.DEFAULT_TIER, 0
    ));

    final DataSegment segment = createDataSegment("foo");

    DruidServer server1 = new DruidServer(
        "serverHot",
        "hostHot",
        null,
        1000,
        ServerType.HISTORICAL,
        "hot",
        0
    );
    server1.addDataSegment(segment.getIdentifier(), segment);
    DruidServer server2 = new DruidServer(
        "serverNorm",
        "hostNorm",
        null,
        1000,
        ServerType.HISTORICAL,
        DruidServer.DEFAULT_TIER,
        0
    );
    server2.addDataSegment(segment.getIdentifier(), segment);
    DruidServer server3 = new DruidServer(
        "serverNormNotServing",
        "hostNorm",
        null,
        10,
        ServerType.HISTORICAL,
        DruidServer.DEFAULT_TIER,
        0
    );
    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    server1.toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder()))),
            DruidServer.DEFAULT_TIER,
            Stream.of(
                new ServerHolder(
                    server2.toImmutableDruidServer(),
                    mockPeon
                ),
                new ServerHolder(
                    server3.toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "hot"));
    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", DruidServer.DEFAULT_TIER));

    EasyMock.verify(throttler, mockPeon);
  }

  @Test
  public void testLoadWithNonExistentTier() throws Exception
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(1);

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "nonExistentTier", 1,
        "hot", 1
    ));

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverHot",
                        "hostHot",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        0
                    ).toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    final DataSegment segment = createDataSegment("foo");

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testDropWithNonExistentTier() throws Exception
  {
    final LoadQueuePeon mockPeon = createEmptyPeon();
    mockPeon.dropSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    EasyMock.replay(throttler, mockPeon, mockBalancerStrategy);

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "nonExistentTier", 1,
        "hot", 1
    ));

    final DataSegment segment = createDataSegment("foo");

    DruidServer server1 = new DruidServer(
        "serverHot",
        "hostHot",
        null,
        1000,
        ServerType.HISTORICAL,
        "hot",
        0
    );
    DruidServer server2 = new DruidServer(
        "serverHo2t",
        "hostHot2",
        null,
        1000,
        ServerType.HISTORICAL,
        "hot",
        0
    );
    server1.addDataSegment(segment.getIdentifier(), segment);
    server2.addDataSegment(segment.getIdentifier(), segment);

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    server1.toImmutableDruidServer(),
                    mockPeon
                ),
                new ServerHolder(
                    server2.toImmutableDruidServer(),
                    mockPeon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    CoordinatorStats stats = rule.run(
        null,
        DruidCoordinatorRuntimeParams.newBuilder()
                                     .withDruidCluster(druidCluster)
                                     .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
                                     .withReplicationManager(throttler)
                                     .withBalancerStrategy(mockBalancerStrategy)
                                     .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
                                     .withAvailableSegments(Arrays.asList(segment)).build(),
        segment
    );

    Assert.assertEquals(1L, stats.getTieredStat("droppedCount", "hot"));

    EasyMock.verify(throttler, mockPeon, mockBalancerStrategy);
  }

  @Test
  public void testMaxLoadingQueueSize() throws Exception
  {
    EasyMock.expect(mockBalancerStrategy.findNewSegmentHomeReplicator(EasyMock.anyObject(), EasyMock.anyObject()))
            .andDelegateTo(balancerStrategy)
            .times(2);

    EasyMock.replay(throttler, mockBalancerStrategy);

    final LoadQueuePeonTester peon = new LoadQueuePeonTester();

    LoadRule rule = createLoadRule(ImmutableMap.of(
        "hot", 1
    ));

    DruidCluster druidCluster = new DruidCluster(
        null,
        ImmutableMap.of(
            "hot",
            Stream.of(
                new ServerHolder(
                    new DruidServer(
                        "serverHot",
                        "hostHot",
                        null,
                        1000,
                        ServerType.HISTORICAL,
                        "hot",
                        0
                    ).toImmutableDruidServer(),
                    peon
                )
            ).collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())))
        )
    );

    DataSegment dataSegment1 = createDataSegment("ds1");
    DataSegment dataSegment2 = createDataSegment("ds2");
    DataSegment dataSegment3 = createDataSegment("ds3");

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams
            .newBuilder()
            .withDruidCluster(druidCluster)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(druidCluster))
            .withReplicationManager(throttler)
            .withBalancerStrategy(mockBalancerStrategy)
            .withBalancerReferenceTimestamp(DateTimes.of("2013-01-01"))
            .withAvailableSegments(Arrays.asList(dataSegment1, dataSegment2, dataSegment3))
            .withDynamicConfigs(new CoordinatorDynamicConfig.Builder().withMaxSegmentsInNodeLoadingQueue(2).build())
            .build();

    CoordinatorStats stats1 = rule.run(null, params, dataSegment1);
    CoordinatorStats stats2 = rule.run(null, params, dataSegment2);
    CoordinatorStats stats3 = rule.run(null, params, dataSegment3);

    Assert.assertEquals(1L, stats1.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertEquals(1L, stats2.getTieredStat(LoadRule.ASSIGNED_COUNT, "hot"));
    Assert.assertFalse(stats3.getTiers(LoadRule.ASSIGNED_COUNT).contains("hot"));

    EasyMock.verify(throttler, mockBalancerStrategy);
  }

  private DataSegment createDataSegment(String dataSource)
  {
    return new DataSegment(
        dataSource,
        Intervals.of("0/3000"),
        DateTimes.nowUtc().toString(),
        Maps.newHashMap(),
        Lists.newArrayList(),
        Lists.newArrayList(),
        NoneShardSpec.instance(),
        0,
        0
    );
  }

  private static LoadRule createLoadRule(final Map<String, Integer> tieredReplicants)
  {
    return new LoadRule()
    {
      @Override
      public Map<String, Integer> getTieredReplicants()
      {
        return tieredReplicants;
      }

      @Override
      public int getNumReplicants(String tier)
      {
        return tieredReplicants.get(tier);
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
  }

  private static LoadQueuePeon createEmptyPeon()
  {
    final LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.newHashSet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop()).andReturn(Sets.newHashSet()).anyTimes();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.expect(mockPeon.getNumberOfSegmentsInQueue()).andReturn(0).anyTimes();

    return mockPeon;
  }
}
