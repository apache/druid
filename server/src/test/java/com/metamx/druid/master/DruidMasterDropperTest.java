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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Table;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.master.rules.IntervalDropRule;
import com.metamx.druid.master.rules.IntervalLoadRule;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.master.rules.RuleMap;
import com.metamx.druid.shard.NoneShardSpec;
import junit.framework.Assert;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class DruidMasterDropperTest
{
  private DruidMaster master;
  private LoadQueuePeon mockPeon;
  private List<DataSegment> availableSegments;
  private DruidMasterDropper dropper;

  @Before
  public void setUp() throws Exception
  {
    master = EasyMock.createMock(DruidMaster.class);
    mockPeon = EasyMock.createMock(LoadQueuePeon.class);


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

    dropper = new DruidMasterDropper(master);
  }

  @After
  public void tearDown() throws Exception
  {
  }

  @Test
  public void testDropRemove() throws Exception
  {
    master.removeSegment(EasyMock.<DataSegment>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(master);

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
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "normal"),
                new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
            )
        ),
        Lists.<Rule>newArrayList()
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);
    Table<String, String, Integer> segmentsInCluster = HashBasedTable.create();
    for (DataSegment segment : availableSegments) {
      segmentsInCluster.put(segment.getIdentifier(), "normal", 1);
    }
    SegmentReplicantLookup segmentReplicantLookup = new SegmentReplicantLookup(segmentsInCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withSegmentRuleLookup(segmentRuleLookup)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = dropper.run(params);

    Assert.assertTrue(afterParams.getDeletedCount() == 12);

    EasyMock.verify(master);
  }

  @Test
  public void testDropTooManyInSameTier() throws Exception
  {
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).atLeastOnce();
    EasyMock.replay(mockPeon);

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
    server2.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));
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

    RuleMap ruleMap = new RuleMap(
        ImmutableMap.<String, List<Rule>>of(
            "test",
            Lists.<Rule>newArrayList(
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "normal")
            )
        ),
        Lists.<Rule>newArrayList(
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);

    Table<String, String, Integer> segmentsInCluster = HashBasedTable.create();
    for (DataSegment segment : availableSegments) {
      segmentsInCluster.put(segment.getIdentifier(), "normal", 1);
    }
    segmentsInCluster.put(availableSegments.get(0).getIdentifier(), "normal", 2);
    SegmentReplicantLookup segmentReplicantLookup = new SegmentReplicantLookup(segmentsInCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withSegmentRuleLookup(segmentRuleLookup)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = dropper.run(params);

    Assert.assertTrue(afterParams.getDroppedCount().get("normal").get() == 1);
    Assert.assertTrue(afterParams.getDeletedCount() == 12);

    EasyMock.verify(mockPeon);
  }


  @Test
  public void testDropTooManyInDifferentTiers() throws Exception
  {
    mockPeon.dropSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.replay(mockPeon);

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
    server2.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));
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

    RuleMap ruleMap = new RuleMap(
        ImmutableMap.<String, List<Rule>>of(
            "test",
            Lists.<Rule>newArrayList(
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "hot")
            )
        ),
        Lists.<Rule>newArrayList(
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);
    Table<String, String, Integer> segmentsInCluster = HashBasedTable.create();
    for (DataSegment segment : availableSegments) {
      segmentsInCluster.put(segment.getIdentifier(), "normal", 1);
    }
    segmentsInCluster.put(availableSegments.get(0).getIdentifier(), "hot", 1);
    SegmentReplicantLookup segmentReplicantLookup = new SegmentReplicantLookup(segmentsInCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withSegmentRuleLookup(segmentRuleLookup)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = dropper.run(params);

    Assert.assertTrue(afterParams.getDroppedCount().get("normal").get() == 1);
    Assert.assertTrue(afterParams.getDeletedCount() == 12);

    EasyMock.verify(mockPeon);
  }

  @Test
  public void testDontDropInDifferentTiers() throws Exception
  {
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
    server2.addDataSegment(availableSegments.get(0).getIdentifier(), availableSegments.get(0));
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

    RuleMap ruleMap = new RuleMap(
        ImmutableMap.<String, List<Rule>>of(
            "test",
            Lists.<Rule>newArrayList(
                new IntervalLoadRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-01T12:00:00.000Z"), 1, "hot")
            )
        ),
        Lists.<Rule>newArrayList(
            new IntervalDropRule(new Interval("2012-01-01T00:00:00.000Z/2012-01-02T00:00:00.000Z"))
        )
    );

    SegmentRuleLookup segmentRuleLookup = SegmentRuleLookup.make(availableSegments, ruleMap);
    Table<String, String, Integer> segmentsInCluster = HashBasedTable.create();
    for (DataSegment segment : availableSegments) {
      segmentsInCluster.put(segment.getIdentifier(), "normal", 1);
    }
    SegmentReplicantLookup segmentReplicantLookup = new SegmentReplicantLookup(segmentsInCluster);

    DruidMasterRuntimeParams params = new DruidMasterRuntimeParams.Builder()
        .withDruidCluster(druidCluster)
        .withMillisToWaitBeforeDeleting(0L)
        .withAvailableSegments(availableSegments)
        .withSegmentRuleLookup(segmentRuleLookup)
        .withSegmentReplicantLookup(segmentReplicantLookup)
        .build();

    DruidMasterRuntimeParams afterParams = dropper.run(params);

    Assert.assertTrue(afterParams.getDroppedCount().get("normal").get() == 0);
    Assert.assertTrue(afterParams.getDeletedCount() == 12);
  }
}
