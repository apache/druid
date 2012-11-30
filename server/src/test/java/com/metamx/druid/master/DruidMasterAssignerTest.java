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
import com.metamx.druid.master.rules.IntervalLoadRule;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.master.rules.RuleMap;
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
public class DruidMasterAssignerTest
{
  private DruidMaster master;
  private LoadQueuePeon mockPeon;
  private List<DataSegment> availableSegments;
  private DruidMasterAssigner assigner;
  private ServiceEmitter emitter;

  @Before
  public void setUp()
  {
    master = EasyMock.createMock(DruidMaster.class);
    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);

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
              1
          )
      );
      start = start.plusHours(1);
    }

    assigner = new DruidMasterAssigner(master);

    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).anyTimes();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

    master.decrementRemovedSegmentsLifetime();
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(master);
  }

  @After
  public void tearDown() throws Exception
  {
    EasyMock.verify(mockPeon);
  }

  @Test
  public void testRunThreeTiersOneReplicant() throws Exception
  {
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

    RuleMap ruleMap = new RuleMap(
        ImmutableMap.<String, List<Rule>>of(
            "test",
            Lists.<Rule>newArrayList(
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), 1, "hot"),
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "normal"),
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "cold")
            )
        ),
        Lists.<Rule>newArrayList()
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withSegmentRuleLookup(segmentRuleLookup)
            .withSegmentReplicantLookup(new SegmentReplicantLookup())
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot").get() == 6);
    Assert.assertTrue(afterParams.getAssignedCount().get("normal").get() == 6);
    Assert.assertTrue(afterParams.getAssignedCount().get("cold").get() == 12);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(master);
  }

  @Test
  public void testRunTwoTiersTwoReplicants() throws Exception
  {
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

    RuleMap ruleMap = new RuleMap(
        ImmutableMap.<String, List<Rule>>of(
            "test",
            Lists.<Rule>newArrayList(
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), 2, "hot"),
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "cold")
            )
        ),
        Lists.<Rule>newArrayList()
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withSegmentRuleLookup(segmentRuleLookup)
            .withSegmentReplicantLookup(new SegmentReplicantLookup())
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot").get() == 12);
    Assert.assertTrue(afterParams.getAssignedCount().get("cold").get() == 18);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(master);
  }

  @Test
  public void testRunThreeTiersWithDefaultRules() throws Exception
  {
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

    RuleMap ruleMap = new RuleMap(
        ImmutableMap.<String, List<Rule>>of(
            "test",
            Lists.<Rule>newArrayList(
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T06:00:00.000Z"), 1, "hot"),
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "normal")
            )
        ),
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "cold")
        )
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withSegmentRuleLookup(segmentRuleLookup)
            .withSegmentReplicantLookup(new SegmentReplicantLookup())
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot").get() == 6);
    Assert.assertTrue(afterParams.getAssignedCount().get("normal").get() == 6);
    Assert.assertTrue(afterParams.getAssignedCount().get("cold").get() == 12);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(master);
  }

  @Test
  public void testRunTwoTiersWithDefaultRulesExistingSegments() throws Exception
  {
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
            )
        )
    );

    RuleMap ruleMap = new RuleMap(
        ImmutableMap.<String, List<Rule>>of(
            "test",
            Lists.<Rule>newArrayList(
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "hot")
            )
        ),
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "normal")
        )
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);
    SegmentReplicantLookup segmentReplicantLookup = SegmentReplicantLookup.make(druidCluster);

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withSegmentRuleLookup(segmentRuleLookup)
            .withSegmentReplicantLookup(segmentReplicantLookup)
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot").get() == 12);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(master);
  }

  @Test
  public void testRunTwoTiersTierDoesNotExist() throws Exception
  {
    emitter.emit(EasyMock.<ServiceEventBuilder>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(emitter);

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

    RuleMap ruleMap = new RuleMap(
        ImmutableMap.<String, List<Rule>>of(
            "test",
            Lists.<Rule>newArrayList(
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "hot")
            )
        ),
        Lists.<Rule>newArrayList(
            new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"), 1, "normal")
        )
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withEmitter(emitter)
            .withDruidCluster(druidCluster)
            .withAvailableSegments(availableSegments)
            .withSegmentRuleLookup(segmentRuleLookup)
            .withSegmentReplicantLookup(new SegmentReplicantLookup())
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot").get() == 0);
    Assert.assertTrue(afterParams.getAssignedCount().get("normal").get() == 12);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(emitter);
  }
}
