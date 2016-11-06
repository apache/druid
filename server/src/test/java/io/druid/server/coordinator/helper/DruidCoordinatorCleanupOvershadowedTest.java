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

package io.druid.server.coordinator.helper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import io.druid.client.ImmutableDruidDataSource;
import io.druid.client.ImmutableDruidServer;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCluster;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.coordinator.ServerHolder;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DruidCoordinatorCleanupOvershadowedTest
{
  DruidCoordinatorCleanupOvershadowed druidCoordinatorCleanupOvershadowed;
  DruidCoordinator coordinator = EasyMock.createStrictMock(DruidCoordinator.class);
  private List<DataSegment> availableSegments;
  DateTime start = new DateTime("2012-01-01");
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
    druidCoordinatorCleanupOvershadowed = new DruidCoordinatorCleanupOvershadowed(coordinator);
    availableSegments = ImmutableList.of(segmentV1, segmentV0, segmentV2);

    druidCluster = new DruidCluster(
        ImmutableMap.of("normal", MinMaxPriorityQueue.orderedBy(Ordering.natural().reverse()).create(Arrays.asList(
            new ServerHolder(druidServer, mockPeon
            )))));

    EasyMock.expect(druidServer.getDataSources())
            .andReturn(ImmutableList.of(druidDataSource))
            .anyTimes();
    EasyMock.expect(druidDataSource.getSegments()).andReturn(ImmutableSet.<DataSegment>of(segmentV1, segmentV2)).anyTimes();
    EasyMock.expect(druidDataSource.getName()).andReturn("test").anyTimes();
    coordinator.removeSegment(segmentV1);
    coordinator.removeSegment(segmentV0);
    EasyMock.expectLastCall();
    EasyMock.replay(mockPeon, coordinator, druidServer, druidDataSource);
    DruidCoordinatorRuntimeParams params = DruidCoordinatorRuntimeParams.newBuilder()
                                                                        .withAvailableSegments(availableSegments)
                                                                        .withCoordinatorStats(new CoordinatorStats())
                                                                        .withDruidCluster(druidCluster)
                                                                        .build();
    druidCoordinatorCleanupOvershadowed.run(params);
    EasyMock.verify(coordinator, druidDataSource, druidServer);
  }
}
