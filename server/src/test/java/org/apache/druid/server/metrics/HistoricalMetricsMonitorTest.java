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

package org.apache.druid.server.metrics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.SegmentManager;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class HistoricalMetricsMonitorTest extends EasyMockSupport
{
  private DruidServerConfig druidServerConfig;
  private SegmentManager segmentManager;
  private SegmentLoadDropHandler segmentLoadDropMgr;
  private StubServiceEmitter serviceEmitter;

  @Before
  public void setUp()
  {
    druidServerConfig = EasyMock.createStrictMock(DruidServerConfig.class);
    segmentManager = EasyMock.createStrictMock(SegmentManager.class);
    segmentLoadDropMgr = EasyMock.createStrictMock(SegmentLoadDropHandler.class);
    serviceEmitter = new StubServiceEmitter("test", "localhost");
  }

  @Test
  public void testSimple()
  {
    final long size = 5;
    final String dataSource = "dataSource";
    final DataSegment dataSegment = new DataSegment(
        dataSource,
        Intervals.of("2014/2015"),
        "version",
        ImmutableMap.of(),
        ImmutableList.of(),
        ImmutableList.of(),
        null,
        1,
        size
    );
    final long maxSize = 10;
    final int priority = 111;
    final String tier = "tier";

    EasyMock.expect(druidServerConfig.getMaxSize()).andReturn(maxSize).once();
    EasyMock.expect(segmentLoadDropMgr.getSegmentsToDelete()).andReturn(ImmutableList.of(dataSegment)).once();
    EasyMock.expect(druidServerConfig.getTier()).andReturn(tier).once();
    EasyMock.expect(druidServerConfig.getPriority()).andReturn(priority).once();
    EasyMock.expect(segmentManager.getDataSourceSizes()).andReturn(ImmutableMap.of(dataSource, size));
    EasyMock.expect(druidServerConfig.getTier()).andReturn(tier).once();
    EasyMock.expect(druidServerConfig.getPriority()).andReturn(priority).once();
    EasyMock.expect(druidServerConfig.getMaxSize()).andReturn(maxSize).times(2);
    EasyMock.expect(segmentManager.getDataSourceCounts()).andReturn(ImmutableMap.of(dataSource, 1L));
    EasyMock.expect(druidServerConfig.getTier()).andReturn(tier).once();
    EasyMock.expect(druidServerConfig.getPriority()).andReturn(priority).once();

    final HistoricalMetricsMonitor monitor = new HistoricalMetricsMonitor(
        druidServerConfig,
        segmentManager,
        segmentLoadDropMgr
    );

    EasyMock.replay(druidServerConfig, segmentManager, segmentLoadDropMgr);
    monitor.doMonitor(serviceEmitter);
    EasyMock.verify(druidServerConfig, segmentManager, segmentLoadDropMgr);

    final List<Event> events = serviceEmitter.getEvents();

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "metric", "segment/max",
        "value", maxSize
    ), asMap(events.get(0)));

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "dataSource", dataSource,
        "metric", "segment/pendingDelete",
        "priority", String.valueOf(priority),
        "tier", tier,
        "value", dataSegment.getSize()
    ), asMap(events.get(1)));

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "metric", "segment/used",
        "value", dataSegment.getSize(),
        "tier", tier,
        "priority", String.valueOf(priority),
        "dataSource", dataSource
    ), asMap(events.get(2)));

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "metric", "segment/usedPercent",
        "value", dataSegment.getSize() * 1.0D / maxSize,
        "tier", tier,
        "priority", String.valueOf(priority),
        "dataSource", dataSource
    ), asMap(events.get(3)));

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "metric", "segment/count",
        "value", 1L,
        "tier", tier,
        "priority", String.valueOf(priority),
        "dataSource", dataSource
    ), asMap(events.get(4)));
  }

  private Map<String, Object> asMap(Event event)
  {
    final Map<String, Object> map = event.toMap();
    Assert.assertNotNull(map.remove("feed"));
    Assert.assertNotNull(map.remove("timestamp"));
    Assert.assertNotNull(map.remove("service"));
    Assert.assertNotNull(map.remove("host"));

    return map;
  }
}
