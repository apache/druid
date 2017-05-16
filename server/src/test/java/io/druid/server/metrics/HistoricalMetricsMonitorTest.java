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

package io.druid.server.metrics;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.DruidServerConfig;
import io.druid.server.SegmentManager;
import io.druid.server.coordination.ZkCoordinator;
import io.druid.timeline.DataSegment;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HistoricalMetricsMonitorTest extends EasyMockSupport
{
  private DruidServerConfig druidServerConfig;
  private SegmentManager segmentManager;
  private ZkCoordinator zkCoordinator;
  private ServiceEmitter serviceEmitter;

  @Before
  public void setUp()
  {
    druidServerConfig = EasyMock.createStrictMock(DruidServerConfig.class);
    segmentManager = EasyMock.createStrictMock(SegmentManager.class);
    zkCoordinator = EasyMock.createStrictMock(ZkCoordinator.class);
    serviceEmitter = EasyMock.createStrictMock(ServiceEmitter.class);
  }

  @Test
  public void testSimple()
  {
    final long size = 5;
    final String dataSource = "dataSource";
    final DataSegment dataSegment = new DataSegment(
        dataSource,
        Interval.parse("2014/2015"),
        "version",
        ImmutableMap.<String, Object>of(),
        ImmutableList.<String>of(),
        ImmutableList.<String>of(),
        null,
        1,
        size
    );
    final long maxSize = 10;
    final int priority = 111;
    final String tier = "tier";

    EasyMock.expect(druidServerConfig.getMaxSize()).andReturn(maxSize).once();
    EasyMock.expect(zkCoordinator.getPendingDeleteSnapshot()).andReturn(ImmutableList.of(dataSegment)).once();
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
        zkCoordinator
    );

    final Capture<ServiceEventBuilder<ServiceMetricEvent>> eventCapture = EasyMock.newCapture(CaptureType.ALL);
    serviceEmitter.emit(EasyMock.capture(eventCapture));
    EasyMock.expectLastCall().times(5);

    EasyMock.replay(druidServerConfig, segmentManager, zkCoordinator, serviceEmitter);
    monitor.doMonitor(serviceEmitter);
    EasyMock.verify(druidServerConfig, segmentManager, zkCoordinator, serviceEmitter);

    final String host = "host";
    final String service = "service";
    Assert.assertTrue(eventCapture.hasCaptured());
    final List<Map<String, Object>> events = Lists.transform(
        eventCapture.getValues(),
        new Function<ServiceEventBuilder<ServiceMetricEvent>, Map<String, Object>>()
        {
          @Nullable
          @Override
          public Map<String, Object> apply(
              @Nullable ServiceEventBuilder<ServiceMetricEvent> input
          )
          {
            final HashMap<String, Object> map = new HashMap<>(input.build(service, host).toMap());
            Assert.assertNotNull(map.remove("feed"));
            Assert.assertNotNull(map.remove("timestamp"));
            Assert.assertNotNull(map.remove("service"));
            Assert.assertNotNull(map.remove("host"));
            return map;
          }
        }
    );

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "metric", "segment/max",
        "value", maxSize
    ), events.get(0));

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "dataSource", dataSource,
        "metric", "segment/pendingDelete",
        "priority", String.valueOf(priority),
        "tier", tier,
        "value", dataSegment.getSize()
    ), events.get(1));

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "metric", "segment/used",
        "value", dataSegment.getSize(),
        "tier", tier,
        "priority", String.valueOf(priority),
        "dataSource", dataSource
    ), events.get(2));

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "metric", "segment/usedPercent",
        "value", dataSegment.getSize() * 1.0D / maxSize,
        "tier", tier,
        "priority", String.valueOf(priority),
        "dataSource", dataSource
    ), events.get(3));

    Assert.assertEquals(ImmutableMap.<String, Object>of(
        "metric", "segment/count",
        "value", 1L,
        "tier", tier,
        "priority", String.valueOf(priority),
        "dataSource", dataSource
    ), events.get(4));
  }
}
