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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class TaskCountStatsMonitorTest
{
  private TaskCountStatsProvider statsProvider;
  private static final RowKey TASK_METRIC_KEY1 = RowKey.with(Dimension.DATASOURCE, "d1")
                                                       .with(Dimension.TASK_TYPE, "index")
                                                       .with(Dimension.SUPERVISOR_ID, "null")
                                                       .build();
  private static final RowKey TASK_METRIC_KEY2 = RowKey.with(Dimension.DATASOURCE, "d1")
                                                       .with(Dimension.TASK_TYPE, "kill")
                                                       .with(Dimension.SUPERVISOR_ID, "null")
                                                       .build();

  @Before
  public void setUp()
  {
    statsProvider = new TaskCountStatsProvider()
    {
      @Override
      public Map<RowKey, Long> getSuccessfulTaskCount()
      {
        return ImmutableMap.of(TASK_METRIC_KEY1, 1L);
      }

      @Override
      public Map<RowKey, Long> getFailedTaskCount()
      {
        return ImmutableMap.of(TASK_METRIC_KEY1, 1L, TASK_METRIC_KEY2, 1L);
      }

      @Override
      public Map<RowKey, Long> getRunningTaskCount()
      {
        return ImmutableMap.of(TASK_METRIC_KEY1, 1L);
      }

      @Override
      public Map<RowKey, Long> getPendingTaskCount()
      {
        return ImmutableMap.of(TASK_METRIC_KEY1, 2L);
      }

      @Override
      public Map<RowKey, Long> getWaitingTaskCount()
      {
        return ImmutableMap.of(TASK_METRIC_KEY1, 2L, TASK_METRIC_KEY2, 1L);
      }

      @Override
      public CoordinatorRunStats getStats()
      {
        final CoordinatorRunStats stats = new CoordinatorRunStats();
        stats.add(Stat.INFO_1, 10);
        stats.addToSegmentStat(Stat.DEBUG_1, "hot", "wiki", 20);
        return stats;
      }
    };
  }

  @Test
  public void testMonitor()
  {
    final TaskCountStatsMonitor monitor = new TaskCountStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    Assert.assertEquals(9, emitter.getNumEmittedEvents());

    emitter.verifyValue("task/success/count", Map.of("dataSource", "d1", "taskType", "index", "supervisorId", "null"), 1L);
    emitter.verifyValue("task/failed/count", Map.of("dataSource", "d1", "taskType", "index", "supervisorId", "null"), 1L);
    emitter.verifyValue("task/failed/count", Map.of("dataSource", "d1", "taskType", "kill", "supervisorId", "null"), 1L);
    emitter.verifyValue("task/running/count", Map.of("dataSource", "d1", "taskType", "index", "supervisorId", "null"), 1L);
    emitter.verifyValue("task/pending/count", Map.of("dataSource", "d1", "taskType", "index", "supervisorId", "null"), 2L);
    emitter.verifyValue("task/waiting/count", Map.of("dataSource", "d1", "taskType", "index", "supervisorId", "null"), 2L);
    emitter.verifyValue("task/waiting/count", Map.of("dataSource", "d1", "taskType", "kill", "supervisorId", "null"), 1L);

    emitter.verifyValue(Stat.INFO_1.getMetricName(), 10L);
    emitter.verifyValue(Stat.DEBUG_1.getMetricName(), Map.of("tier", "hot", "dataSource", "wiki"), 20L);
  }

  private static class Stat
  {
    static final CoordinatorStat INFO_1 = CoordinatorStat.toLogAndEmit("i1", "info/1", CoordinatorStat.Level.INFO);
    static final CoordinatorStat DEBUG_1 = CoordinatorStat.toDebugAndEmit("d1", "debug/1");
  }
}
