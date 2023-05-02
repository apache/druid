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

package org.apache.druid.server.coordinator.duty;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.server.coordinator.loadqueue.LoadQueuePeon;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.List;

@RunWith(JUnitParamsRunner.class)
public class MarkAsUnusedOvershadowedSegmentsTest
{
  private final DruidCoordinator coordinator = EasyMock.createStrictMock(DruidCoordinator.class);

  private final DateTime start = DateTimes.of("2012-01-01");

  private final LoadQueuePeon mockPeon = EasyMock.createMock(LoadQueuePeon.class);
  private final ImmutableDruidDataSource druidDataSource = EasyMock.createMock(ImmutableDruidDataSource.class);
  private final DataSegment segmentV0 = DataSegment.builder().dataSource("test")
                                                   .interval(new Interval(start, start.plusHours(1)))
                                                   .version("0")
                                                   .size(0)
                                                   .build();
  private final DataSegment segmentV1 = segmentV0.withVersion("1");
  private final DataSegment segmentV2 = segmentV0.withVersion("2");

  @Test
  @Parameters({"historical", "broker"})
  public void testRun(String serverTypeString)
  {
    ServerType serverType = ServerType.fromString(serverTypeString);

    MarkAsUnusedOvershadowedSegments markAsUnusedOvershadowedSegments =
        new MarkAsUnusedOvershadowedSegments(coordinator);
    final List<DataSegment> usedSegments = ImmutableList.of(segmentV1, segmentV0, segmentV2);

    // Dummy values for comparisons in TreeSet
    EasyMock.expect(mockPeon.getSegmentsInQueue())
            .andReturn(Collections.emptySet()).anyTimes();
    EasyMock.expect(mockPeon.getSegmentsMarkedToDrop())
            .andReturn(Collections.emptySet()).anyTimes();
    final ImmutableDruidServer druidServer = new DruidServer("", "", "", 0L, serverType, "", 0)
        .addDataSegment(segmentV1)
        .addDataSegment(segmentV2)
        .toImmutableDruidServer();

    coordinator.markSegmentsAsUnused("test", ImmutableSet.of(segmentV1.getId(), segmentV0.getId()));
    EasyMock.expectLastCall();
    EasyMock.replay(mockPeon, coordinator, druidDataSource);

    DruidCluster druidCluster = DruidCluster
        .builder()
        .addTier("normal", new ServerHolder(druidServer, mockPeon))
        .build();

    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams
        .newBuilder(System.nanoTime())
        .withUsedSegmentsInTest(usedSegments)
        .withDruidCluster(druidCluster)
        .withDynamicConfigs(
            CoordinatorDynamicConfig.builder()
                                    .withLeadingTimeMillisBeforeCanMarkAsUnusedOvershadowedSegments(0)
                                    .build()
        )
        .build();
    markAsUnusedOvershadowedSegments.run(params);
    EasyMock.verify(coordinator, druidDataSource);
  }
}
