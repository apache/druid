package com.metamx.druid.master;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidServer;
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
import java.util.Map;

/**
 */
public class DruidMasterAssignerTest
{
  private DruidMaster master;
  private LoadQueuePeon mockPeon;
  private List<DataSegment> availableSegments;
  private DruidMasterAssigner assigner;

  @Before
  public void setUp()
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

    assigner = new DruidMasterAssigner(master);

    mockPeon.loadSegment(EasyMock.<DataSegment>anyObject(), EasyMock.<LoadPeonCallback>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(mockPeon.getSegmentsToLoad()).andReturn(Sets.<DataSegment>newHashSet()).atLeastOnce();
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
    Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers = ImmutableMap.of(
        "hot",
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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

    Map<String, Rule> segmentRules = Maps.newHashMap();
    for (DataSegment segment : availableSegments) {
      for (Rule rule : ruleMap.getRules(segment.getDataSource())) {
        if (rule.appliesTo(segment.getInterval())) {
          segmentRules.put(segment.getIdentifier(), rule);
          break;
        }
      }
    }

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withHistoricalServers(historicalServers)
            .withAvailableSegments(availableSegments)
            .withSegmentRules(segmentRules)
            .withSegmentsInCluster(Maps.<String, Map<String, Integer>>newHashMap())
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot") == 6);
    Assert.assertTrue(afterParams.getAssignedCount().get("normal") == 6);
    Assert.assertTrue(afterParams.getAssignedCount().get("cold") == 12);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(master);
  }

  @Test
  public void testRunTwoTiersTwoReplicants() throws Exception
  {
    Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers = ImmutableMap.of(
        "hot",
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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

    Map<String, Rule> segmentRules = Maps.newHashMap();
    for (DataSegment segment : availableSegments) {
      for (Rule rule : ruleMap.getRules(segment.getDataSource())) {
        if (rule.appliesTo(segment.getInterval())) {
          segmentRules.put(segment.getIdentifier(), rule);
          break;
        }
      }
    }

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withHistoricalServers(historicalServers)
            .withAvailableSegments(availableSegments)
            .withSegmentRules(segmentRules)
            .withSegmentsInCluster(Maps.<String, Map<String, Integer>>newHashMap())
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot") == 12);
    Assert.assertTrue(afterParams.getAssignedCount().get("cold") == 18);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(master);
  }

  @Test
  public void testRunThreeTiersWithDefaultRules() throws Exception
  {
    Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers = ImmutableMap.of(
        "hot",
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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

    Map<String, Rule> segmentRules = Maps.newHashMap();
    for (DataSegment segment : availableSegments) {
      for (Rule rule : ruleMap.getRules(segment.getDataSource())) {
        if (rule.appliesTo(segment.getInterval())) {
          segmentRules.put(segment.getIdentifier(), rule);
          break;
        }
      }
    }

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withHistoricalServers(historicalServers)
            .withAvailableSegments(availableSegments)
            .withSegmentRules(segmentRules)
            .withSegmentsInCluster(Maps.<String, Map<String, Integer>>newHashMap())
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot") == 6);
    Assert.assertTrue(afterParams.getAssignedCount().get("normal") == 6);
    Assert.assertTrue(afterParams.getAssignedCount().get("cold") == 12);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(master);
  }

  @Test
  public void testRunTwoTiersWithDefaultRulesExistingSegments() throws Exception
  {
    Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers = ImmutableMap.of(
        "hot",
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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

    Map<String, Map<String, Integer>> segmentsInCluster = Maps.newHashMap();
    Map<String, Rule> segmentRules = Maps.newHashMap();
    for (DataSegment segment : availableSegments) {
      for (Rule rule : ruleMap.getRules(segment.getDataSource())) {
        if (rule.appliesTo(segment.getInterval())) {
          segmentRules.put(segment.getIdentifier(), rule);
          break;
        }
      }

      segmentsInCluster.put(segment.getIdentifier(), ImmutableMap.of("normal", 1));
    }

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withHistoricalServers(historicalServers)
            .withAvailableSegments(availableSegments)
            .withSegmentRules(segmentRules)
            .withSegmentsInCluster(segmentsInCluster)
            .build();

    DruidMasterRuntimeParams afterParams = assigner.run(params);

    Assert.assertTrue(afterParams.getAssignedCount().get("hot") == 12);
    Assert.assertTrue(afterParams.getUnassignedCount() == 0);
    Assert.assertTrue(afterParams.getUnassignedSize() == 0);

    EasyMock.verify(master);
  }

  @Test
  public void testRunTwoTiersTierDoesNotExist() throws Exception
  {
    Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers = ImmutableMap.of(
        "normal",
        MinMaxPriorityQueue.orderedBy(Comparators.inverse(Ordering.natural())).create(
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

    Map<String, Rule> segmentRules = Maps.newHashMap();
    for (DataSegment segment : availableSegments) {
      for (Rule rule : ruleMap.getRules(segment.getDataSource())) {
        if (rule.appliesTo(segment.getInterval())) {
          segmentRules.put(segment.getIdentifier(), rule);
          break;
        }
      }
    }

    DruidMasterRuntimeParams params =
        new DruidMasterRuntimeParams.Builder()
            .withHistoricalServers(historicalServers)
            .withAvailableSegments(availableSegments)
            .withSegmentRules(segmentRules)
            .withSegmentsInCluster(Maps.<String, Map<String, Integer>>newHashMap())
            .build();

    boolean exceptionOccurred = false;
    try {
    DruidMasterRuntimeParams afterParams = assigner.run(params);
    } catch (ISE e) {
      exceptionOccurred = true;
    }

    Assert.assertTrue(exceptionOccurred);
  }
}
