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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.server.coordination.DruidServerMetadata;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.helper.DruidCoordinatorMirroringTierSegmentCopier;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Test class for DruidCoordinatorMirroringTierSegmentCopier
 */
public class DruidCoordinatorMirroringTierSegmentCopierTest
{

  private static final String TIER1 = "tier1";
  private static final String MIRROR1 = "tier1_mirror1";
  private static final String DATASOURCE_NAME = "src1";
  private static final DataSegment SEGMENT1 = createDataSegment(1);
  private static final DataSegment SEGMENT2 = createDataSegment(2);

  private static final ImmutableDruidDataSource DATA_SOURCE1 =
      new ImmutableDruidDataSource(DATASOURCE_NAME, Collections.emptyMap(), Collections.singletonList(SEGMENT1));

  private static final ImmutableDruidDataSource DATA_SOURCE2 =
      new ImmutableDruidDataSource(DATASOURCE_NAME, Collections.emptyMap(), Collections.singletonList(SEGMENT2));

  private LoadQueuePeon mockPeon;
  private LoadQueuePeon mockPeonMirror1;
  private LoadQueuePeon mockPeonMirror2;
  private DruidCoordinatorMirroringTierSegmentCopier copier;
  private ServerHolder server1;
  private ServerHolder server2;
  private ServerHolder mirror1;
  private ServerHolder mirror2;
  private Iterable<ServerHolder> hotTier;
  private DruidCoordinatorRuntimeParams params;

  @Before
  public void setUp()
  {
    mockPeon = EasyMock.createMock(LoadQueuePeon.class);
    mockPeonMirror1 = EasyMock.createMock(LoadQueuePeon.class);
    mockPeonMirror2 = EasyMock.createMock(LoadQueuePeon.class);
    EasyMock.expect(mockPeon.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.expect(mockPeonMirror1.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.expect(mockPeonMirror2.getLoadQueueSize()).andReturn(0L).anyTimes();
    EasyMock.replay(mockPeon);
    server1 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name1", "host1", null, 100L, ServerType.HISTORICAL, TIER1, 0), 0L,
        ImmutableMap.of(DATASOURCE_NAME, DATA_SOURCE1), 1
    ), mockPeon);

    server2 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name2", "host2", null, 200L, ServerType.HISTORICAL, TIER1, 0),
        100L, ImmutableMap.of(DATASOURCE_NAME, DATA_SOURCE2), 1
    ), mockPeon);
    copier = new DruidCoordinatorMirroringTierSegmentCopier(
        new TestDruidCoordinatorConfig(null, null, null,
                new Duration("PT5S"), null, null, null,
                null, null, null, null,
            null, null, null, new Duration(Long.MAX_VALUE),
                new Duration("PT1S"), 10, null
        ));

    TestDruidCoordinatorConfig druidCoordinatorConfig = new TestDruidCoordinatorConfig(
            null,
            null,
            null,
            new Duration("PT5S"),
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            new Duration(Long.MAX_VALUE),
            new Duration("PT1S"),
            10,
            null
    );
    hotTier = Stream.of(server1, server2)
        .collect(Collectors.toCollection(() -> new TreeSet<>(Collections.reverseOrder())));
  }

  @After
  public void tearDown()
  {
    EasyMock.verify(mockPeon, mockPeonMirror1, mockPeonMirror2);
  }

  @Test
  public void testSimpleCase()
  {
    mockPeonMirror1.loadSegment(EasyMock.eq(SEGMENT1), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    mockPeonMirror2.loadSegment(EasyMock.eq(SEGMENT2), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(mockPeonMirror1, mockPeonMirror2);

    mirror1 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name3", "host3", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(), 0
    ), mockPeonMirror1);
    mirror2 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name4", "host4", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(), 0
    ), mockPeonMirror2);

    DruidCluster druidCluster = new DruidCluster(null, ImmutableMap.of(TIER1, hotTier, MIRROR1,
        Stream.of(mirror1, mirror2)
            .collect(Collectors.toCollection(() -> new TreeSet<>(
                Collections.reverseOrder())))
    ));
    params = new DruidCoordinatorRuntimeParams.Builder().withDruidCluster(druidCluster).build();
    assertStats(2, 0);
  }

  @Test
  public void testIncompleteMirroringTier()
  {
    mockPeonMirror1.loadSegment(EasyMock.anyObject(), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(mockPeonMirror1, mockPeonMirror2);

    mirror1 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name3", "host3", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(), 0
    ), mockPeonMirror1);

    DruidCluster druidCluster = new DruidCluster(null, ImmutableMap.of(TIER1, hotTier, MIRROR1,
        Stream.of(mirror1)
            .collect(Collectors.toCollection(() -> new TreeSet<>(
                Collections.reverseOrder())))
    ));
    params = new DruidCoordinatorRuntimeParams.Builder().withDruidCluster(druidCluster).build();
    assertStats(1, 0);
  }

  @Test
  public void testAlreadyHalfMatched()
  {
    mockPeonMirror2.loadSegment(EasyMock.eq(SEGMENT2), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(mockPeonMirror1, mockPeonMirror2);

    mirror1 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name3", "host3", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(DATASOURCE_NAME, DATA_SOURCE1), 1
    ), mockPeonMirror1);
    mirror2 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name4", "host4", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(), 0
    ), mockPeonMirror2);

    DruidCluster druidCluster = new DruidCluster(null, ImmutableMap.of(TIER1, hotTier, MIRROR1,
        Stream.of(mirror1, mirror2)
            .collect(Collectors.toCollection(() -> new TreeSet<>(
                Collections.reverseOrder())))
    ));
    params = new DruidCoordinatorRuntimeParams.Builder().withDruidCluster(druidCluster).build();
    assertStats(1, 0);
  }

  @Test
  public void testAlreadyFullMatched()
  {
    EasyMock.replay(mockPeonMirror1, mockPeonMirror2);

    mirror1 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name3", "host3", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(DATASOURCE_NAME, DATA_SOURCE1), 1
    ), mockPeonMirror1);
    mirror2 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name4", "host4", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(DATASOURCE_NAME, DATA_SOURCE2), 1
    ), mockPeonMirror2);

    DruidCluster druidCluster = new DruidCluster(null, ImmutableMap.of(TIER1, hotTier, MIRROR1,
        Stream.of(mirror1, mirror2)
            .collect(Collectors.toCollection(() -> new TreeSet<>(
                Collections.reverseOrder())))
    ));
    params = new DruidCoordinatorRuntimeParams.Builder().withDruidCluster(druidCluster).build();
    assertStats(0, 0);
  }

  @Test
  public void testDroppingSegment()
  {
    mockPeonMirror1.loadSegment(EasyMock.eq(SEGMENT1), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    mockPeonMirror2.dropSegment(EasyMock.eq(SEGMENT1), EasyMock.anyObject());
    EasyMock.expectLastCall().once();
    EasyMock.replay(mockPeonMirror1, mockPeonMirror2);

    mirror1 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name3", "host3", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(), 0
    ), mockPeonMirror1);
    mirror2 = new ServerHolder(new ImmutableDruidServer(
        new DruidServerMetadata("name4", "host4", null, 100L, ServerType.HISTORICAL, MIRROR1, 0),
        0L, ImmutableMap.of(
        DATASOURCE_NAME,
        new ImmutableDruidDataSource(DATASOURCE_NAME, Collections.emptyMap(),
            ImmutableList.of(SEGMENT1, SEGMENT2)
        )
    ), 2
    ), mockPeonMirror2);

    DruidCluster druidCluster = new DruidCluster(null, ImmutableMap.of(TIER1, hotTier, MIRROR1,
        Stream.of(mirror1, mirror2)
            .collect(Collectors.toCollection(() -> new TreeSet<>(
                Collections.reverseOrder())))
    ));
    params = new DruidCoordinatorRuntimeParams.Builder().withDruidCluster(druidCluster).build();
    assertStats(1, 1);
  }

  private void assertStats(int addCount, int removeCount)
  {
    DruidCoordinatorRuntimeParams afterParams = copier.run(params);
    CoordinatorStats stats = afterParams.getCoordinatorStats();
    Assert.assertEquals(
        addCount,
        stats.getTieredStat(DruidCoordinatorMirroringTierSegmentCopier.ASSIGNED_COUNT, MIRROR1)
    );
    Assert.assertEquals(
        removeCount,
        stats.getTieredStat(DruidCoordinatorMirroringTierSegmentCopier.DROPPED_COUNT, MIRROR1)
    );
  }

  private static DataSegment createDataSegment(int size)
  {
    return new DataSegment("test", Intervals.of("2015-04-1%d/2015-04-1%d", size, size + 1), "1",
        ImmutableMap.of("containerName", "container" + size, "blobPath", "blobPath" + size), null,
        null, NoneShardSpec.instance(), 0, size
    );
  }
}
