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
import org.junit.Test;

public class TaskCountStatsMonitorTest
{
  @Test
  public void testMonitor()
  {
    TaskCountStatsProvider statsProvider = () -> {
      final CoordinatorRunStats stats = new CoordinatorRunStats();
      stats.add(Stat.INFO_1, 10);
      stats.add(
          Stat.RUNNING_TASKS,
          RowKey.with(Dimension.DATASOURCE, "wiki").and(Dimension.TASK_TYPE, "test"),
          20
      );
      return stats;
    };

    final TaskCountStatsMonitor monitor = new TaskCountStatsMonitor(statsProvider);
    final StubServiceEmitter emitter = new StubServiceEmitter("service", "host");
    monitor.doMonitor(emitter);

    Assert.assertEquals(2, emitter.getEvents().size());
    emitter.verifyValue(Stat.INFO_1.getMetricName(), 10L);
    emitter.verifyValue(
        Stat.RUNNING_TASKS.getMetricName(),
        ImmutableMap.of("taskType", "test", "dataSource", "wiki"),
        20L
    );
  }

  private static class Stat
  {
    static final CoordinatorStat INFO_1 = CoordinatorStat.toLogAndEmit("i1", "info/1", CoordinatorStat.Level.INFO);
    static final CoordinatorStat RUNNING_TASKS
        = CoordinatorStat.toDebugAndEmit("runningTasks", "task/running/count");
  }
}
