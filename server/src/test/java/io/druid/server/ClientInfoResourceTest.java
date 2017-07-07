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

/*
 * Druid - a distributed column store.
 * Copyright 2015 - Yahoo! Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import io.druid.client.DruidServer;
import io.druid.client.FilteredServerInventoryView;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.HighestPriorityTierSelectorStrategy;
import io.druid.client.selector.RandomServerSelectorStrategy;
import io.druid.client.selector.ServerSelector;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.server.coordination.ServerType;
import io.druid.server.security.AuthConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.SingleElementPartitionChunk;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ClientInfoResourceTest
{
  private static final String KEY_DIMENSIONS = "dimensions";
  private static final String KEY_METRICS = "metrics";
  private static final DateTime FIXED_TEST_TIME = new DateTime(2015, 9, 14, 0, 0); /* always use the same current time for unit tests */


  private final String dataSource = "test-data-source";

  private FilteredServerInventoryView serverInventoryView;
  private TimelineServerView timelineServerView;
  private ClientInfoResource resource;

  @Before
  public void setup()
  {
    VersionedIntervalTimeline<String, ServerSelector> timeline = new VersionedIntervalTimeline<>(Ordering.<String>natural());
    DruidServer server = new DruidServer("name", "host",  null, 1234, ServerType.HISTORICAL, "tier", 0);

    addSegment(timeline, server, "1960-02-13/1961-02-14", ImmutableList.of("d5"), ImmutableList.of("m5"), "v0");

    // segments within [2014-02-13, 2014-02-18]
    addSegment(timeline, server, "2014-02-13/2014-02-14", ImmutableList.of("d1"), ImmutableList.of("m1"), "v0");
    addSegment(timeline, server, "2014-02-14/2014-02-15", ImmutableList.of("d1"), ImmutableList.of("m1"), "v0");
    addSegment(timeline, server, "2014-02-16/2014-02-17", ImmutableList.of("d1"), ImmutableList.of("m1"), "v0");
    addSegment(timeline, server, "2014-02-17/2014-02-18", ImmutableList.of("d2"), ImmutableList.of("m2"), "v0");

    // segments within [2015-02-01, 2015-02-13]
    addSegment(timeline, server, "2015-02-01/2015-02-07", ImmutableList.of("d1"), ImmutableList.of("m1"), "v1");
    addSegment(timeline, server, "2015-02-07/2015-02-13", ImmutableList.of("d1"), ImmutableList.of("m1"), "v1");
    addSegmentWithShardSpec(
        timeline, server, "2015-02-03/2015-02-05",
        ImmutableList.of("d1", "d2"),
        ImmutableList.of("m1", "m2"),
        "v2",
        new NumberedShardSpec(0, 2)
    );
    addSegmentWithShardSpec(
        timeline, server, "2015-02-03/2015-02-05",
        ImmutableList.of("d1", "d2", "d3"),
        ImmutableList.of("m1", "m2", "m3"),
        "v2",
        new NumberedShardSpec(1, 2)
    );
    addSegment(
        timeline,
        server,
        "2015-02-09/2015-02-10",
        ImmutableList.of("d1", "d3"),
        ImmutableList.of("m1", "m3"),
        "v2"
    );
    addSegment(timeline, server, "2015-02-11/2015-02-12", ImmutableList.of("d3"), ImmutableList.of("m3"), "v2");

    // segments within [2015-03-13, 2015-03-19]
    addSegment(timeline, server, "2015-03-13/2015-03-19", ImmutableList.of("d1"), ImmutableList.of("m1"), "v3");
    addSegment(timeline, server, "2015-03-13/2015-03-14", ImmutableList.of("d1"), ImmutableList.of("m1"), "v4");
    addSegment(timeline, server, "2015-03-14/2015-03-15", ImmutableList.of("d1"), ImmutableList.of("m1"), "v5");
    addSegment(timeline, server, "2015-03-15/2015-03-16", ImmutableList.of("d1"), ImmutableList.of("m1"), "v6");

    // imcomplete segment
    addSegmentWithShardSpec(
        timeline, server, "2015-04-03/2015-04-05",
        ImmutableList.of("d4"),
        ImmutableList.of("m4"),
        "v7",
        new NumberedShardSpec(0, 2)
    );

    serverInventoryView = EasyMock.createMock(FilteredServerInventoryView.class);
    EasyMock.expect(serverInventoryView.getInventory()).andReturn(ImmutableList.of(server)).anyTimes();

    timelineServerView = EasyMock.createMock(TimelineServerView.class);
    EasyMock.expect(timelineServerView.getTimeline(EasyMock.anyObject(TableDataSource.class))).andReturn(timeline);

    EasyMock.replay(serverInventoryView, timelineServerView);

    resource = getResourceTestHelper(
        serverInventoryView, timelineServerView,
        new SegmentMetadataQueryConfig()
    );
  }

  @Test
  public void testGetDatasourceNonFullWithInterval()
  {
    Map<String, Object> actual = resource.getDatasource(dataSource, "1975/2015", null);
    Map<String, Object> expected = ImmutableMap.<String, Object>of(
        KEY_DIMENSIONS, ImmutableSet.of("d1", "d2"),
        KEY_METRICS, ImmutableSet.of("m1", "m2")
    );
    EasyMock.verify(serverInventoryView);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetDatasourceFullWithInterval()
  {
    Map<String, Object> actual = resource.getDatasource(dataSource, "1975/2015", "true");
    Map<String, Object> expected = ImmutableMap.<String, Object>of(
        "2014-02-13T00:00:00.000Z/2014-02-15T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2014-02-16T00:00:00.000Z/2014-02-17T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2014-02-17T00:00:00.000Z/2014-02-18T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d2"), KEY_METRICS, ImmutableSet.of("m2"))
    );

    EasyMock.verify(serverInventoryView, timelineServerView);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetDatasourceFullWithSmallInterval()
  {
    Map<String, Object> actual = resource.getDatasource(
        dataSource,
        "2014-02-13T09:00:00.000Z/2014-02-17T23:00:00.000Z",
        "true"
    );
    Map<String, Object> expected = ImmutableMap.<String, Object>of(
        "2014-02-13T09:00:00.000Z/2014-02-15T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2014-02-16T00:00:00.000Z/2014-02-17T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2014-02-17T00:00:00.000Z/2014-02-17T23:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d2"), KEY_METRICS, ImmutableSet.of("m2"))
    );

    EasyMock.verify(serverInventoryView, timelineServerView);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetDatasourceWithDefaultInterval()
  {
    Map<String, Object> actual = resource.getDatasource(dataSource, null, null);
    Map<String, Object> expected = ImmutableMap.<String, Object>of(KEY_DIMENSIONS, ImmutableSet.of(), KEY_METRICS, ImmutableSet.of());

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetDatasourceWithConfiguredDefaultInterval()
  {
    ClientInfoResource defaultResource = getResourceTestHelper(
        serverInventoryView, timelineServerView,
        new SegmentMetadataQueryConfig("P100Y")
    );

    Map<String, Object> expected = ImmutableMap.<String, Object>of(
        KEY_DIMENSIONS,
        ImmutableSet.of("d1", "d2", "d3", "d4", "d5"),
        KEY_METRICS,
        ImmutableSet.of("m1", "m2", "m3", "m4", "m5")
    );

    Map<String, Object> actual = defaultResource.getDatasource(dataSource, null, null);

    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetDatasourceFullWithOvershadowedSegments1()
  {
    Map<String, Object> actual = resource.getDatasource(
        dataSource,
        "2015-02-02T09:00:00.000Z/2015-02-06T23:00:00.000Z",
        "true"
    );
    
    Map<String, Object> expected = ImmutableMap.<String, Object>of(
        "2015-02-02T09:00:00.000Z/2015-02-03T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2015-02-03T00:00:00.000Z/2015-02-05T00:00:00.000Z",
        ImmutableMap.of(
            KEY_DIMENSIONS,
            ImmutableSet.of("d1", "d2", "d3"),
            KEY_METRICS,
            ImmutableSet.of("m1", "m2", "m3")
        ),
        "2015-02-05T00:00:00.000Z/2015-02-06T23:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    );

    EasyMock.verify(serverInventoryView, timelineServerView);
    Assert.assertEquals(expected, actual);

  }

  @Test
  public void testGetDatasourceFullWithOvershadowedSegments2()
  {
    Map<String, Object> actual= resource.getDatasource(
        dataSource,
        "2015-02-09T09:00:00.000Z/2015-02-13T23:00:00.000Z",
        "true"
    );

    Map<String, Object> expected = ImmutableMap.<String, Object>of(
        "2015-02-09T09:00:00.000Z/2015-02-10T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1", "d3"), KEY_METRICS, ImmutableSet.of("m1", "m3")),
        "2015-02-10T00:00:00.000Z/2015-02-11T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2015-02-11T00:00:00.000Z/2015-02-12T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d3"), KEY_METRICS, ImmutableSet.of("m3")),
        "2015-02-12T00:00:00.000Z/2015-02-13T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    );

    EasyMock.verify(serverInventoryView, timelineServerView);
    Assert.assertEquals(expected, actual);
  }

  /**
   * Though segments within [2015-03-13, 2015-03-19] have different versions, they all abut with each other and have
   * same dimensions/metrics, so they all should be merged together.
   */
  @Test
  public void testGetDatasourceFullWithOvershadowedSegmentsMerged()
  {
    Map<String, Object> actual = resource.getDatasource(
        dataSource,
        "2015-03-13T02:00:00.000Z/2015-03-19T15:00:00.000Z",
        "true"
    );

    Map<String, Object> expected = ImmutableMap.<String, Object>of(
        "2015-03-13T02:00:00.000Z/2015-03-19T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    );

    EasyMock.verify(serverInventoryView, timelineServerView);
    Assert.assertEquals(expected, actual);
  }

  /**
   * If "full" is specified, then dimensions/metrics that exist in an incompelte segment should be ingored
   */
  @Test
  public void testGetDatasourceFullWithIncompleteSegment()
  {
    Map<String, Object> actual = resource.getDatasource(dataSource, "2015-04-03/2015-04-05", "true");
    Map<String, Object> expected = ImmutableMap.of();

    EasyMock.verify(serverInventoryView, timelineServerView);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetDatasourceFullWithLargeInterval()
  {
    Map<String, Object> actual = resource.getDatasource(dataSource, "1975/2050", "true");
    Map<String, Object> expected = ImmutableMap.<String, Object>builder().put(
        "2014-02-13T00:00:00.000Z/2014-02-15T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    ).put(
        "2014-02-16T00:00:00.000Z/2014-02-17T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    ).put(
        "2014-02-17T00:00:00.000Z/2014-02-18T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d2"), KEY_METRICS, ImmutableSet.of("m2"))
    ).put(
        "2015-02-01T00:00:00.000Z/2015-02-03T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    ).put(
        "2015-02-03T00:00:00.000Z/2015-02-05T00:00:00.000Z",
        ImmutableMap.of(
            KEY_DIMENSIONS,
            ImmutableSet.of("d1", "d2", "d3"),
            KEY_METRICS,
            ImmutableSet.of("m1", "m2", "m3")
        )
    ).put(
        "2015-02-05T00:00:00.000Z/2015-02-09T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    ).put(
        "2015-02-09T00:00:00.000Z/2015-02-10T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1", "d3"), KEY_METRICS, ImmutableSet.of("m1", "m3"))
    ).put(
        "2015-02-10T00:00:00.000Z/2015-02-11T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    ).put(
        "2015-02-11T00:00:00.000Z/2015-02-12T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d3"), KEY_METRICS, ImmutableSet.of("m3"))
    ).put(
        "2015-02-12T00:00:00.000Z/2015-02-13T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    ).put(
        "2015-03-13T00:00:00.000Z/2015-03-19T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1"))
    ).build();

    EasyMock.verify(serverInventoryView, timelineServerView);
    Assert.assertEquals(expected, actual);
  }

  private void addSegment(
      VersionedIntervalTimeline<String, ServerSelector> timeline,
      DruidServer server,
      String interval,
      List<String> dims,
      List<String> metrics,
      String version
  )
  {
    DataSegment segment = DataSegment.builder()
                                     .dataSource(dataSource)
                                     .interval(new Interval(interval))
                                     .version(version)
                                     .dimensions(dims)
                                     .metrics(metrics)
                                     .size(1)
                                     .build();
    server.addDataSegment(segment.getIdentifier(), segment);
    ServerSelector ss = new ServerSelector(segment, new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()));
    timeline.add(new Interval(interval), version, new SingleElementPartitionChunk<ServerSelector>(ss));
  }

  private void addSegmentWithShardSpec(
      VersionedIntervalTimeline<String, ServerSelector> timeline,
      DruidServer server,
      String interval,
      List<String> dims,
      List<String> metrics,
      String version,
      ShardSpec shardSpec
  )
  {
    DataSegment segment = DataSegment.builder()
                                     .dataSource(dataSource)
                                     .interval(new Interval(interval))
                                     .version(version)
                                     .dimensions(dims)
                                     .metrics(metrics)
                                     .shardSpec(shardSpec)
                                     .size(1)
                                     .build();
    server.addDataSegment(segment.getIdentifier(), segment);
    ServerSelector ss = new ServerSelector(segment, new HighestPriorityTierSelectorStrategy(new RandomServerSelectorStrategy()));
    timeline.add(new Interval(interval), version, shardSpec.createChunk(ss));
  }

  private ClientInfoResource getResourceTestHelper(
      FilteredServerInventoryView serverInventoryView,
      TimelineServerView timelineServerView,
      SegmentMetadataQueryConfig segmentMetadataQueryConfig
  )
  {
    return new ClientInfoResource(serverInventoryView, timelineServerView, segmentMetadataQueryConfig, new AuthConfig())
    {
      @Override
      protected DateTime getCurrentTime()
      {
        return FIXED_TEST_TIME;
      }
    };
  }
}
