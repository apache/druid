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

import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class BrokerSegmentStatsMonitorTest
{
  private static final RowKey SEGMENT_METRIC_KEY1 = RowKey.with(Dimension.DATASOURCE, "dataSource1")
                                                          .with(Dimension.VERSION, "2024-01-01T00:00:00.000Z")
                                                          .with(Dimension.INTERVAL, "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z")
                                                          .build();
  private static final RowKey SEGMENT_METRIC_KEY2 = RowKey.with(Dimension.DATASOURCE, "dataSource2")
                                                          .with(Dimension.VERSION, "2024-01-02T00:00:00.000Z")
                                                          .with(Dimension.INTERVAL, "2024-01-02T00:00:00.000Z/2024-01-03T00:00:00.000Z")
                                                          .build();

  @Test
  public void test_monitor()
  {
    BrokerSegmentStatsProvider statsProvider = new BrokerSegmentStatsProvider()
    {
      @Override
      public Map<RowKey, Long> getSegmentAddedCount()
      {
        return Map.of(SEGMENT_METRIC_KEY1, 10L, SEGMENT_METRIC_KEY2, 5L);
      }

      @Override
      public Map<RowKey, Long> getSegmentRemovedCount()
      {
        return Map.of(SEGMENT_METRIC_KEY1, 1L);
      }
    };
    final BrokerSegmentStatsMonitor monitor = new BrokerSegmentStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assert.assertTrue(monitor.doMonitor(emitter));

    Assert.assertEquals(3, emitter.getNumEmittedEvents());

    emitter.verifyValue("serverview/segment/added", Map.of("dataSource", "dataSource1", "version", "2024-01-01T00:00:00.000Z", "interval", "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"), 10L);
    emitter.verifyValue("serverview/segment/added", Map.of("dataSource", "dataSource2", "version", "2024-01-02T00:00:00.000Z", "interval", "2024-01-02T00:00:00.000Z/2024-01-03T00:00:00.000Z"), 5L);
    emitter.verifyValue("serverview/segment/removed", Map.of("dataSource", "dataSource1", "version", "2024-01-01T00:00:00.000Z", "interval", "2024-01-01T00:00:00.000Z/2024-01-02T00:00:00.000Z"), 1L);
  }

  @Test
  public void test_monitor_withNullCounts()
  {
    BrokerSegmentStatsProvider statsProvider = new BrokerSegmentStatsProvider()
    {
      @Override
      public Map<RowKey, Long> getSegmentAddedCount()
      {
        return null;
      }

      @Override
      public Map<RowKey, Long> getSegmentRemovedCount()
      {
        return null;
      }
    };
    final BrokerSegmentStatsMonitor monitor = new BrokerSegmentStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter();
    Assert.assertTrue(monitor.doMonitor(emitter));

    Assert.assertEquals(0, emitter.getNumEmittedEvents());
  }

  @Test
  public void test_monitor_withEmptyCounts()
  {
    BrokerSegmentStatsProvider statsProvider = new BrokerSegmentStatsProvider()
    {
      @Override
      public Map<RowKey, Long> getSegmentAddedCount()
      {
        return Map.of();
      }

      @Override
      public Map<RowKey, Long> getSegmentRemovedCount()
      {
        return Map.of();
      }
    };
    final BrokerSegmentStatsMonitor monitor = new BrokerSegmentStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    Assert.assertTrue(monitor.doMonitor(emitter));

    Assert.assertEquals(0, emitter.getNumEmittedEvents());
  }
}
