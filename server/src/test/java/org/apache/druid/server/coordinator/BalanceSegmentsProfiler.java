/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.coordinator;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.ImmutableDruidServerTests;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.metadata.MetadataRuleManager;
import org.apache.druid.server.coordinator.duty.BalanceSegments;
import org.apache.druid.server.coordinator.duty.RunRules;
import org.apache.druid.server.coordinator.loading.LoadQueuePeon;
import org.apache.druid.server.coordinator.loading.SegmentLoadQueueManager;
import org.apache.druid.server.coordinator.loading.TestLoadQueuePeon;
import org.apache.druid.server.coordinator.rules.PeriodLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

/**
 * TODO convert benchmarks to JMH
 */
public class BalanceSegmentsProfiler
{
  private static final int MAX_SEGMENTS_TO_MOVE = 5;
  private SegmentLoadQueueManager loadQueueManager;
  private ImmutableDruidServer druidServer1;
  private ImmutableDruidServer druidServer2;
  List<DataSegment> segments = new ArrayList<>();
  ServiceEmitter emitter;
  MetadataRuleManager manager;
  PeriodLoadRule loadRule = new PeriodLoadRule(new Period("P5000Y"), null, ImmutableMap.of("normal", 3), null);
  List<Rule> rules = ImmutableList.of(loadRule);

  @Before
  public void setUp()
  {
    loadQueueManager = new SegmentLoadQueueManager(null, null);
    druidServer1 = EasyMock.createMock(ImmutableDruidServer.class);
    druidServer2 = EasyMock.createMock(ImmutableDruidServer.class);
    emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    manager = EasyMock.createMock(MetadataRuleManager.class);
  }

  public void bigProfiler()
  {
    Stopwatch watch = Stopwatch.createUnstarted();
    int numSegments = 55000;
    int numServers = 50;
    EasyMock.expect(manager.getAllRules()).andReturn(ImmutableMap.of("test", rules)).anyTimes();
    EasyMock.expect(manager.getRules(EasyMock.anyObject())).andReturn(rules).anyTimes();
    EasyMock.expect(manager.getRulesWithDefault(EasyMock.anyObject())).andReturn(rules).anyTimes();
    EasyMock.replay(manager);

    List<ServerHolder> serverHolderList = new ArrayList<>();
    List<DataSegment> segments = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      segments.add(
          new DataSegment(
              "datasource" + i,
              new Interval(DateTimes.of("2012-01-01"), (DateTimes.of("2012-01-01")).plusHours(1)),
              (DateTimes.of("2012-03-01")).toString(),
              new HashMap<>(),
              new ArrayList<>(),
              new ArrayList<>(),
              NoneShardSpec.instance(),
              0,
              4L
          )
      );
    }

    for (int i = 0; i < numServers; i++) {
      ImmutableDruidServer server = EasyMock.createMock(ImmutableDruidServer.class);
      EasyMock.expect(server.getMetadata()).andReturn(null).anyTimes();
      EasyMock.expect(server.getCurrSize()).andReturn(30L).atLeastOnce();
      EasyMock.expect(server.getMaxSize()).andReturn(100L).atLeastOnce();
      EasyMock.expect(server.getTier()).andReturn("normal").anyTimes();
      EasyMock.expect(server.getName()).andReturn(Integer.toString(i)).atLeastOnce();
      EasyMock.expect(server.getHost()).andReturn(Integer.toString(i)).anyTimes();
      if (i == 0) {
        ImmutableDruidServerTests.expectSegments(server, segments);
      } else {
        ImmutableDruidServerTests.expectSegments(server, Collections.emptyList());
      }
      EasyMock.expect(server.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
      EasyMock.replay(server);

      LoadQueuePeon peon = new TestLoadQueuePeon();
      serverHolderList.add(new ServerHolder(server, peon));
    }

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier("normal", serverHolderList.toArray(new ServerHolder[0]))
        .build();
    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(druidCluster)
        .withUsedSegments(segments)
        .withDynamicConfigs(
            CoordinatorDynamicConfig
                .builder()
                .withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE)
                .withReplicantLifetime(500)
                .withReplicationThrottleLimit(5)
                .build()
        )
        .withSegmentAssignerUsing(loadQueueManager)
        .withDatabaseRuleManager(manager)
        .build();

    BalanceSegments tester = new BalanceSegments(Duration.standardMinutes(1));
    RunRules runner = new RunRules(Set::size);
    watch.start();
    DruidCoordinatorRuntimeParams balanceParams = tester.run(params);
    DruidCoordinatorRuntimeParams assignParams = runner.run(params);
    System.out.println(watch.stop());
  }


  public void profileRun()
  {
    Stopwatch watch = Stopwatch.createUnstarted();
    TestLoadQueuePeon fromPeon = new TestLoadQueuePeon();
    TestLoadQueuePeon toPeon = new TestLoadQueuePeon();

    EasyMock.expect(druidServer1.getName()).andReturn("from").atLeastOnce();
    EasyMock.expect(druidServer1.getCurrSize()).andReturn(30L).atLeastOnce();
    EasyMock.expect(druidServer1.getMaxSize()).andReturn(100L).atLeastOnce();
    ImmutableDruidServerTests.expectSegments(druidServer1, segments);
    EasyMock.expect(druidServer1.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer1);

    EasyMock.expect(druidServer2.getName()).andReturn("to").atLeastOnce();
    EasyMock.expect(druidServer2.getTier()).andReturn("normal").anyTimes();
    EasyMock.expect(druidServer2.getCurrSize()).andReturn(0L).atLeastOnce();
    EasyMock.expect(druidServer2.getMaxSize()).andReturn(100L).atLeastOnce();
    ImmutableDruidServerTests.expectSegments(druidServer2, Collections.emptyList());
    EasyMock.expect(druidServer2.getSegment(EasyMock.anyObject())).andReturn(null).anyTimes();
    EasyMock.replay(druidServer2);

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(DateTimes.nowUtc())
        .withDruidCluster(
            DruidCluster
                .builder()
                .addTier(
                    "normal",
                    new ServerHolder(druidServer1, fromPeon),
                    new ServerHolder(druidServer2, toPeon)
                )
                .build()
        )
        .withUsedSegments(segments)
        .withDynamicConfigs(CoordinatorDynamicConfig.builder().withMaxSegmentsToMove(MAX_SEGMENTS_TO_MOVE).build())
        .withSegmentAssignerUsing(loadQueueManager)
        .build();
    BalanceSegments tester = new BalanceSegments(Duration.standardMinutes(1));
    watch.start();
    DruidCoordinatorRuntimeParams balanceParams = tester.run(params);
    System.out.println(watch.stop());
  }

}
