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

package io.druid.server.coordinator;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.DruidServer;
import io.druid.db.DatabaseRuleManager;
import io.druid.server.coordinator.helper.DruidCoordinatorRuleRunner;
import io.druid.server.coordinator.rules.PeriodLoadRule;
import io.druid.server.coordinator.rules.Rule;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Before;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidCoordinatorBalancerProfiler
{
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private DruidCoordinator coordinator;
  private DruidServer druidServer1;
  private DruidServer druidServer2;
  Map<String, DataSegment> segments = Maps.newHashMap();
  ServiceEmitter emitter;
  DatabaseRuleManager manager;
  PeriodLoadRule loadRule = new PeriodLoadRule(new Period("P5000Y"), null, null, 3, "normal");
  List<Rule> rules = ImmutableList.<Rule>of(loadRule);

  @Before
  public void setUp() throws Exception
  {
    coordinator = EasyMock.createMock(DruidCoordinator.class);
    druidServer1 = EasyMock.createMock(DruidServer.class);
    druidServer2 = EasyMock.createMock(DruidServer.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    manager = EasyMock.createMock(DatabaseRuleManager.class);
  }

  public void bigProfiler()
  {
    Stopwatch watch = new Stopwatch();
    int numSegments = 55000;
    int numServers = 50;
    EasyMock.expect(manager.getAllRules()).andReturn(ImmutableMap.<String, List<Rule>>of("test", rules)).anyTimes();
    EasyMock.expect(manager.getRules(EasyMock.<String>anyObject())).andReturn(rules).anyTimes();
    EasyMock.expect(manager.getRulesWithDefault(EasyMock.<String>anyObject())).andReturn(rules).anyTimes();
    EasyMock.replay(manager);

    coordinator.moveSegment(
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);

    List<DruidServer> serverList = Lists.newArrayList();
    Map<String, LoadQueuePeon> peonMap = Maps.newHashMap();
    List<ServerHolder> serverHolderList = Lists.newArrayList();
    Map<String, DataSegment> segmentMap = Maps.newHashMap();
    for (int i = 0; i < numSegments; i++) {
      segmentMap.put(
          "segment" + i,
          new DataSegment(
              "datasource" + i,
              new Interval(new DateTime("2012-01-01"), (new DateTime("2012-01-01")).plusHours(1)),
              (new DateTime("2012-03-01")).toString(),
              Maps.<String, Object>newHashMap(),
              Lists.<String>newArrayList(),
              Lists.<String>newArrayList(),
              new NoneShardSpec(),
              0,
              4L
          )
      );
    }

    for (int i = 0; i < numServers; i++) {
      DruidServer server = EasyMock.createMock(DruidServer.class);
      EasyMock.expect(server.getMetadata()).andReturn(null).anyTimes();
      EasyMock.expect(server.getCurrSize()).andReturn(30L).atLeastOnce();
      EasyMock.expect(server.getMaxSize()).andReturn(100L).atLeastOnce();
      EasyMock.expect(server.getTier()).andReturn("normal").anyTimes();
      EasyMock.expect(server.getName()).andReturn(Integer.toString(i)).atLeastOnce();
      EasyMock.expect(server.getHost()).andReturn(Integer.toString(i)).anyTimes();
      if (i == 0) {
        EasyMock.expect(server.getSegments()).andReturn(segmentMap).anyTimes();
      } else {
        EasyMock.expect(server.getSegments()).andReturn(new HashMap<String, DataSegment>()).anyTimes();
      }
      EasyMock.expect(server.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
      EasyMock.replay(server);

      LoadQueuePeon peon = new LoadQueuePeonTester();
      peonMap.put(Integer.toString(i), peon);
      serverHolderList.add(new ServerHolder(server, peon));
    }

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                .withDruidCluster(
                                    new DruidCluster(
                                        ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                            "normal",
                                            MinMaxPriorityQueue.orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                                                               .create(
                                                                   serverHolderList
                                                               )
                                        )
                                    )
                                )
                                .withLoadManagementPeons(
                                    peonMap
                                )
                                .withAvailableSegments(segmentMap.values())
                                .withDynamicConfigs(
                                    new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                                        MAX_SEGMENTS_TO_MOVE
                                    ).withReplicantLifetime(500)
                                                                     .withReplicationThrottleLimit(5)
                                                                     .build()
                                )
                                .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                .withEmitter(emitter)
                                .withDatabaseRuleManager(manager)
                                .withReplicationManager(new ReplicationThrottler(2, 500))
                                .withSegmentReplicantLookup(
                                    SegmentReplicantLookup.make(
                                        new DruidCluster(
                                            ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                                "normal",
                                                MinMaxPriorityQueue.orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                                                                   .create(
                                                                       serverHolderList
                                                                   )
                                            )
                                        )
                                    )
                                )
                                .build();

    DruidCoordinatorBalancerTester tester = new DruidCoordinatorBalancerTester(coordinator);
    DruidCoordinatorRuleRunner runner = new DruidCoordinatorRuleRunner(coordinator);
    watch.start();
    DruidCoordinatorRuntimeParams balanceParams = tester.run(params);
    DruidCoordinatorRuntimeParams assignParams = runner.run(params);
    System.out.println(watch.stop());
  }


  public void profileRun()
  {
    Stopwatch watch = new Stopwatch();
    LoadQueuePeonTester fromPeon = new LoadQueuePeonTester();
    LoadQueuePeonTester toPeon = new LoadQueuePeonTester();

    EasyMock.expect(druidServer1.getName()).andReturn("from").atLeastOnce();
    EasyMock.expect(druidServer1.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer1.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer1.getSegments()).andReturn(segments).anyTimes();
    EasyMock.expect(druidServer1.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer1);

    EasyMock.expect(druidServer2.getName()).andReturn("to").atLeastOnce();
    EasyMock.expect(druidServer2.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer2.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(druidServer2.getMaxSize()).andReturn(100L).atLeastOnce();
    EasyMock.expect(druidServer2.getSegments()).andReturn(new HashMap<String, DataSegment>()).anyTimes();
    EasyMock.expect(druidServer2.getSegment(EasyMock.<String>anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer2);

    coordinator.moveSegment(
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<String>anyObject(),
        EasyMock.<LoadPeonCallback>anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(coordinator);

    DruidCoordinatorRuntimeParams params =
        DruidCoordinatorRuntimeParams.newBuilder()
                                .withDruidCluster(
                                    new DruidCluster(
                                        ImmutableMap.<String, MinMaxPriorityQueue<ServerHolder>>of(
                                            "normal",
                                            MinMaxPriorityQueue.orderedBy(DruidCoordinatorBalancerTester.percentUsedComparator)
                                                               .create(
                                                                   Arrays.asList(
                                                                       new ServerHolder(druidServer1, fromPeon),
                                                                       new ServerHolder(druidServer2, toPeon)
                                                                   )
                                                               )
                                        )
                                    )
                                )
                                .withLoadManagementPeons(
                                    ImmutableMap.<String, LoadQueuePeon>of(
                                        "from",
                                        fromPeon,
                                        "to",
                                        toPeon
                                    )
                                )
                                .withAvailableSegments(segments.values())
                                .withDynamicConfigs(
                                    new CoordinatorDynamicConfig.Builder().withMaxSegmentsToMove(
                                        MAX_SEGMENTS_TO_MOVE
                                    ).build()
                                )
                                .withBalancerReferenceTimestamp(new DateTime("2013-01-01"))
                                .build();
    DruidCoordinatorBalancerTester tester = new DruidCoordinatorBalancerTester(coordinator);
    watch.start();
    DruidCoordinatorRuntimeParams balanceParams = tester.run(params);
    System.out.println(watch.stop());
  }

}
