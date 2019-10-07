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

package org.apache.druid.server.coordinator.helper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorRuntimeParamsTestHelpers;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidClusterBuilder;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuleRunnerTest;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.LoadQueuePeon;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.List;

public class DruidCoordinatorCleanupOvershadowedTest
{
  DruidCoordinatorCleanupOvershadowed druidCoordinatorMarkAsUnusedOvershadowedSegments;
  DruidCoordinator coordinator = EasyMock.createStrictMock(DruidCoordinator.class);
  private List<DataSegment> usedSegments;
  DateTime start = DateTimes.of("2012-01-01");
  DruidCluster druidCluster;
  private LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
  private ImmutableDruidServer druidServer = EasyMock.createMock(ImmutableDruidServer.class);
  private ImmutableDruidDataSource druidDataSource = EasyMock.createMock(ImmutableDruidDataSource.class);
  private DataSegment segmentV0 = new DataSegment.Builder().dataSource("test")
                                                           .interval(new Interval(start, start.plusHours(1)))
                                                           .version("0")
                                                           .build();
  private DataSegment segmentV1 = new DataSegment.Builder().dataSource("test")
                                                           .interval(new Interval(start, start.plusHours(1)))
                                                           .version("1")
                                                           .build();
  private DataSegment segmentV2 = new DataSegment.Builder().dataSource("test")
                                                           .interval(new Interval(start, start.plusHours(1)))
                                                           .version("2")
                                                           .build();

  @Test
  public void testRun()
  {
    druidCoordinatorMarkAsUnusedOvershadowedSegments = new DruidCoordinatorCleanupOvershadowed(coordinator);
    usedSegments = ImmutableList.of(segmentV1, segmentV0, segmentV2);

    // Dummy values for comparisons in TreeSet
    EasyMock.expect(mockPeon.getLoadQueueSize())
            .andReturn(0L)
            .anyTimes();
    EasyMock.expect(druidServer.getMaxSize())
            .andReturn(0L)
            .anyTimes();
    EasyMock.expect(druidServer.getCurrSize())
            .andReturn(0L)
            .anyTimes();
    EasyMock.expect(druidServer.getName())
            .andReturn("")
            .anyTimes();
    EasyMock.expect(druidServer.getHost())
            .andReturn("")
            .anyTimes();
    EasyMock.expect(druidServer.getTier())
            .andReturn("")
            .anyTimes();
    EasyMock.expect(druidServer.getType())
            .andReturn(ServerType.HISTORICAL)
            .anyTimes();

    EasyMock.expect(druidServer.getDataSources())
            .andReturn(ImmutableList.of(druidDataSource))
            .anyTimes();
    EasyMock.expect(druidDataSource.getSegments())
            .andReturn(ImmutableSet.of(segmentV1, segmentV2))
            .anyTimes();
    EasyMock.expect(druidDataSource.getName()).andReturn("test").anyTimes();
    coordinator.markSegmentAsUnused(segmentV1);
    coordinator.markSegmentAsUnused(segmentV0);
    EasyMock.expectLastCall();
    EasyMock.replay(mockPeon, coordinator, druidServer, druidDataSource);

    druidCluster = DruidClusterBuilder
        .newBuilder()
        .addTier("normal", new ServerHolder(druidServer, mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = CoordinatorRuntimeParamsTestHelpers
        .newBuilder()
        .withUsedSegmentsInTest(usedSegments)
        .withCoordinatorStats(new CoordinatorStats())
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(
            DruidCoordinatorRuleRunnerTest.COORDINATOR_CONFIG_WITH_ZERO_LEADING_TIME_BEFORE_CAN_MARK_AS_UNUSED_OVERSHADOWED_SEGMENTS
        )
        .build();
    druidCoordinatorMarkAsUnusedOvershadowedSegments.run(params);
    EasyMock.verify(coordinator, druidDataSource, druidServer);
  }
}
