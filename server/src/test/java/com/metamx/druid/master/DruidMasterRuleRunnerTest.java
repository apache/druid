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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.db.DatabaseRuleManager;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.master.rules.IntervalDropRule;
import com.metamx.druid.master.rules.IntervalLoadRule;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class DruidMasterRuleRunnerTest
{
  private DruidMaster master;
  private LoadQueuePeon mockPeon;
  private List<DataSegment> availableSegments;
  private DruidMasterRuleRunner ruleRunner;
  private ServiceEmitter emitter;
  private DatabaseRuleManager databaseRuleManager;

  @Before
  public void setUp()
  {
    master = EasyMock.createMock(DruidMaster.class);
    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    databaseRuleManager = EasyMock.createMock(DatabaseRuleManager.class);

    DateTime start = new DateTime("2012-01-01");
    availableSegments = Lists.newArrayList();
    for (int i = 0; i < 24; i++) {
      availableSegments.add(
          new DataSegment(
              "test",
              new Interval(start, start.plusHours(1)),
              new DateTime().toString(),
              Maps.<String, Object>newHashMap(),
              Lists.<String>newArrayList(),
              Lists.<String>newArrayList(),
              new NoneShardSpec(),
              IndexIO.CURRENT_VERSION_ID,
              1
          )
      );
      start = start.plusHours(1);
    }

    ruleRunner = new DruidMasterRuleRunner(master, 1, 24);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(databaseRuleManager);
  }

  /**
   * Nodes:
   * hot - 1 replicant
   * normal - 1 replicant
   * cold - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunThreeTiersOneReplicant() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), 1, "hot"),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "normal"),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "cold")
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

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
                            "hot"
                        ),
                        mockPeon
                    )
                )
            ),
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal"
                        ),
                        mockPeon
                    )
                )
            ),
            "cold",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverCold",
                            "hostCold",
                            1000,
                            "historical",
                            "cold"
                        ),
                        mockPeon
                    )
                )
            )
        )
    );

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 6);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("normal").get() == 6);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("cold").get() == 12);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * hot - 2 replicants
   * cold - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunTwoTiersTwoReplicants() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), 2, "hot"),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "cold")
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

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
                            "hot"
                        ),
                        mockPeon
                    ),
                    new ServerHolder(
                        new DruidServer(
                            "serverHot2",
                            "hostHot2",
                            1000,
                            "historical",
                            "hot"
                        ),
                        mockPeon
                    )
                )
            ),
            "cold",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverCold",
                            "hostCold",
                            1000,
                            "historical",
                            "cold"
                        ),
                        mockPeon
                    )
                )
            )
        )
    );

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 12);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("cold").get() == 18);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    EasyMock.verify(mockPeon);
  }

  /**
   * Nodes:
   * hot - 1 replicant
   * normal - 1 replicant
   *
   * @throws Exception
   */
  @Test
  public void testRunTwoTiersWithExistingSegments() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "hot"),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "normal")
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer normServer = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal"
    );
    for (DataSegment availableSegment : availableSegments) {
      normServer.addDataSegment(availableSegment.getIdentifier(), availableSegment);
    }

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
                            "hot"
                        ),
                        mockPeon
                    )
                )
            ),
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        normServer,
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(segmentReplicantLookup)
            .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 12);
    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("normal").get() == 0);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunTwoTiersTierDoesNotExist() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().times(12);
    EasyMock.replay(emitter);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "hot"),
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "normal")
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal"
                        ),
                        mockPeon
                    )
                )
            )
        )
    );

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withEmitter(emitter)
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build();


    ruleRunner.run(params);

    EasyMock.verify(emitter);
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunRuleDoesNotExist() throws Exception
  {
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().times(availableSegments.size());
    EasyMock.replay(emitter);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-02T00:00:00.000Z/2012-01-03T00:00:00.000Z"), 1, "normal")
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        new DruidServer(
                            "serverNorm",
                            "hostNorm",
                            1000,
                            "historical",
                            "normal"
                        ),
                        mockPeon
                    )
                )
            )
        )
    );

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withEmitter(emitter)
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build();

    ruleRunner.run(params);

    EasyMock.verify(emitter);
  }

  @Test
  public void testDropRemove() throws Exception
  {
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    master.removeSegment(EasyMock.<DataSegment>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(master);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "normal"),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal"
    );
    for (DataSegment segment : availableSegments) {
      server.addDataSegment(segment.getIdentifier(), segment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server,
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getGlobalStats().get("deletedCount").get() == 12);

    EasyMock.verify(master);
  }

  @Test
  public void testDropTooManyInSameTier() throws Exception
  {
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "normal"),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "serverNorm",
        "hostNorm",
        1000,
        "historical",
        "normal"
    );
    server1.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));

    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal"
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment.getIdentifier(), segment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1,
                        mockPeon
                    ),
                    new ServerHolder(
                        server2,
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("normal").get() == 1);
    Assert.assertTrue(stats.getGlobalStats().get("deletedCount").get() == 12);

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropTooManyInDifferentTiers() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "hot"),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "hot"
    );
    server1.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal"
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment.getIdentifier(), segment);
    }

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
            "normal",
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("normal").get() == 1);
    Assert.assertTrue(stats.getGlobalStats().get("deletedCount").get() == 12);

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDontDropInDifferentTiers() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "hot"),
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "hot"
    );
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal"
    );
    for (DataSegment segment : availableSegments) {
      server2.addDataSegment(segment.getIdentifier(), segment);
    }
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
            "normal",
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

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount") == null);
    Assert.assertTrue(stats.getGlobalStats().get("deletedCount").get() == 12);

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropServerActuallyServesSegment() throws Exception
  {
    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T01:00:00.000Z"), 0, "normal")
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DruidServer server1 = new DruidServer(
        "server1",
        "host1",
        1000,
        "historical",
        "normal"
    );
    server1.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal"
    );
    server2.addDataSegment(availableSegments.get(1).getIdentifier(), availableSegments.get(1));
    DruidServer server3 = new DruidServer(
        "serverNorm3",
        "hostNorm3",
        1000,
        "historical",
        "normal"
    );
    server3.addDataSegment(availableSegments.get(1).getIdentifier(), availableSegments.get(1));
    server3.addDataSegment(availableSegments.get(2).getIdentifier(), availableSegments.get(2));

    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    LoadQueuePeon anotherMockPeon = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(anotherMockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(anotherMockPeon.getLoadQueueSize()).andReturn(10L).atLeastOnce();
    EasyMock.replay(anotherMockPeon);

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1,
                        mockPeon
                    ),
                    new ServerHolder(
                        server2,
                        anotherMockPeon
                    ),
                    new ServerHolder(
                        server3,
                        anotherMockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("normal").get() == 1);

    EasyMock.verify(mockPeon);
    EasyMock.verify(anotherMockPeon);
  }

  /**
   * Nodes:
   * hot - 2 replicants
   *
   * @throws Exception
   */
  @Test
  public void testReplicantThrottle() throws Exception
  {
    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2013-01-01T00:00:00.000Z"), 2, "hot")
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

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
                            "hot"
                        ),
                        mockPeon
                    ),
                    new ServerHolder(
                        new DruidServer(
                            "serverHot2",
                            "hostHot2",
                            1000,
                            "historical",
                            "hot"
                        ),
                        mockPeon
                    )
                )
            )
        )
    );

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 48);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    DataSegment overFlowSegment = new DataSegment(
        "test",
        new Interval("2012-02-01/2012-02-02"),
        new DateTime().toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        new NoneShardSpec(),
        1,
        0
    );

    afterParams = ruleRunner.run(
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withEmitter(emitter)
            .withAvailableSegments(Arrays.asList(overFlowSegment))
            .withDatabaseRuleManager(databaseRuleManager)
            .withSegmentReplicantLookup(SegmentReplicantLookup.make(new DruidCluster()))
            .build()
    );
    stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("assignedCount").get("hot").get() == 1);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedCount") == null);
    Assert.assertTrue(stats.getPerTierStats().get("unassignedSize") == null);

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDropReplicantThrottle() throws Exception
  {
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    EasyMock.expect(databaseRuleManager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2013-01-02T00:00:00.000Z"), 1, "normal")
        )
    ).atLeastOnce();
    EasyMock.replay(databaseRuleManager);

    DataSegment overFlowSegment = new DataSegment(
        "test",
        new Interval("2012-02-01/2012-02-02"),
        new DateTime().toString(),
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        new NoneShardSpec(),
        1,
        0
    );
    List<DataSegment> longerAvailableSegments = Lists.newArrayList(availableSegments);
    longerAvailableSegments.add(overFlowSegment);

    DruidServer server1 = new DruidServer(
        "serverNorm1",
        "hostNorm1",
        1000,
        "historical",
        "normal"
    );
    for (DataSegment availableSegment : longerAvailableSegments) {
      server1.addDataSegment(availableSegment.getIdentifier(), availableSegment);
    }
    DruidServer server2 = new DruidServer(
        "serverNorm2",
        "hostNorm2",
        1000,
        "historical",
        "normal"
    );
    for (DataSegment availableSegment : longerAvailableSegments) {
      server2.addDataSegment(availableSegment.getIdentifier(), availableSegment);
    }

    DruidCluster druidCluster = new DruidCluster(
        ImmutableMap.of(
            "normal",
            MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(
                Arrays.asList(
                    new ServerHolder(
                        server1,
                        mockPeon
                    ),
                    new ServerHolder(
                        server2,
                        mockPeon
                    )
                )
            )
        )
    );

    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(longerAvailableSegments)
        .withDatabaseRuleManager(databaseRuleManager)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = ruleRunner.run(params);
    MasterStats stats = afterParams.getMasterStats();

    Assert.assertTrue(stats.getPerTierStats().get("droppedCount").get("normal").get() == 24);
    EasyMock.verify(mockPeon);
  }
}
