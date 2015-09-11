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

import java.util.List;
import java.util.Map;

import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.client.TimelineServerView;
import io.druid.client.selector.ServerSelector;
import io.druid.query.TableDataSource;
import io.druid.query.metadata.SegmentMetadataQueryConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import io.druid.timeline.partition.SingleElementPartitionChunk;

import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

public class ClientInfoResourceTest
{
  private static final String KEY_DIMENSIONS = "dimensions";
  private static final String KEY_METRICS = "metrics";
  private static final DateTime FIXED_TEST_TIME = new DateTime(2015, 9, 14, 0, 0); /* always use the same current time for unit tests */


  private final String dataSource = "test-data-source";
  private final String version = "v0";

  private InventoryView serverInventoryView;
  private TimelineServerView timelineServerView;
  private ClientInfoResource resource;

  @Before
  public void setup()
  {
    VersionedIntervalTimeline<String, ServerSelector> timeline = new VersionedIntervalTimeline<>(Ordering.<String>natural());
    DruidServer server = new DruidServer("name", "host", 1234, "type", "tier", 0);

    addSegment(timeline, server, "1960-02-13/1961-02-14", ImmutableList.of("d1"), ImmutableList.of("m1"));
    addSegment(timeline, server, "2014-02-13/2014-02-14", ImmutableList.of("d1"), ImmutableList.of("m1"));
    addSegment(timeline, server, "2014-02-14/2014-02-15", ImmutableList.of("d1"), ImmutableList.of("m1"));
    addSegment(timeline, server, "2014-02-16/2014-02-17", ImmutableList.of("d1"), ImmutableList.of("m1"));
    addSegment(timeline, server, "2014-02-17/2014-02-18", ImmutableList.of("d2"), ImmutableList.of("m2"));

    serverInventoryView = EasyMock.createMock(InventoryView.class);
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
  public void testGetDatasourceNonFullWithLargeInterval()
  {
    Map<String, Object> actual = resource.getDatasource(dataSource, "1975/2050", null);
    Map<String, ?> expected = ImmutableMap.of(
        KEY_DIMENSIONS, ImmutableSet.of("d1", "d2"),
        KEY_METRICS, ImmutableSet.of("m1", "m2")
    );
    EasyMock.verify(serverInventoryView);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testGetDatasourceFullWithLargeInterval()
  {

    Map<String, Object> actual = resource.getDatasource(dataSource, "1975/2050", "true");
    Map<String, ?> expected = ImmutableMap.of(
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
    Map<String, ?> expected = ImmutableMap.of(
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
    Map<String, Object> actual = resource.getDatasource(dataSource, null, "false");
    Assert.assertEquals(actual.size(), 0);
  }

  @Test
  public void testGetDatasourceWithConfiguredDefaultInterval()
  {
    ClientInfoResource defaultResource = getResourceTestHelper(
        serverInventoryView, timelineServerView,
        new SegmentMetadataQueryConfig("P100Y")
    );

    Map<String, ?> expected = ImmutableMap.of(
        "1960-02-13T00:00:00.000Z/1961-02-14T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2014-02-13T00:00:00.000Z/2014-02-15T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2014-02-16T00:00:00.000Z/2014-02-17T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d1"), KEY_METRICS, ImmutableSet.of("m1")),
        "2014-02-17T00:00:00.000Z/2014-02-18T00:00:00.000Z",
        ImmutableMap.of(KEY_DIMENSIONS, ImmutableSet.of("d2"), KEY_METRICS, ImmutableSet.of("m2"))
    );

    Map<String, Object> actual = defaultResource.getDatasource(dataSource, null, "false");
    Assert.assertEquals(expected, actual);
  }


  private void addSegment(
      VersionedIntervalTimeline<String, ServerSelector> timeline,
      DruidServer server,
      String interval,
      List<String> dims,
      List<String> metrics
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
    ServerSelector ss = new ServerSelector(segment, null);
    timeline.add(new Interval(interval), version, new SingleElementPartitionChunk<ServerSelector>(ss));
  }

  private ClientInfoResource getResourceTestHelper(
      InventoryView serverInventoryView,
      TimelineServerView timelineServerView,
      SegmentMetadataQueryConfig segmentMetadataQueryConfig
  )
  {
    return new ClientInfoResource(serverInventoryView, timelineServerView, segmentMetadataQueryConfig)
    {
      @Override
      protected DateTime getCurrentTime()
      {
        return FIXED_TEST_TIME;
      }
    };
  }
}
