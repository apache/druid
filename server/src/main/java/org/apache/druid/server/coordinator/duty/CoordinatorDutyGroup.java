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

import com.google.common.collect.EvictingQueue;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Stopwatch;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A group of {@link CoordinatorDuty}.
 */
public class CoordinatorDutyGroup
{
  private static final Logger log = new Logger(CoordinatorDutyGroup.class);

  private final String name;
  private final Duration period;
  private final List<CoordinatorDuty> duties;
  private final List<String> dutyNames;
  private final DruidCoordinator.DutyGroupHelper coordinator;

  private final AtomicReference<DateTime> lastRunStartTime = new AtomicReference<>();
  private final AtomicReference<DateTime> lastRunEndTime = new AtomicReference<>();

  private final EvictingQueue<Long> runTimes = EvictingQueue.create(20);
  private final EvictingQueue<Long> gapTimes = EvictingQueue.create(20);

  public CoordinatorDutyGroup(
      String name,
      List<CoordinatorDuty> duties,
      Duration period,
      DruidCoordinator.DutyGroupHelper coordinator
  )
  {
    this.name = name;
    this.duties = duties;
    this.period = period;
    this.dutyNames = duties.stream().map(duty -> duty.getClass().getName()).collect(Collectors.toList());
    this.coordinator = coordinator;

    log.info("Created dutyGroup[%s] with period[%s] and duties[%s].", name, period, dutyNames);
  }

  public String getName()
  {
    return name;
  }

  public Duration getPeriod()
  {
    return period;
  }

  public synchronized DutyGroupStatus getStatus()
  {
    return new DutyGroupStatus(
        name,
        period,
        dutyNames,
        lastRunStartTime.get(),
        lastRunEndTime.get(),
        computeWindowAverage(runTimes),
        computeWindowAverage(gapTimes)
    );
  }

  /**
   * Runs this duty group if the coordinator is leader and emits stats collected
   * during the run.
   */
  public void run(DruidCoordinatorRuntimeParams params)
  {
    markRunStarted();

    final boolean coordinationPaused = params.getCoordinatorDynamicConfig().getPauseCoordination();
    if (coordinationPaused && coordinator.isLeader()) {
      log.info("Coordination has been paused. Duties will not run until coordination is resumed.");
      return;
    }

    final CoordinatorRunStats stats = params.getCoordinatorStats();
    for (CoordinatorDuty duty : duties) {
      if (coordinator.isLeader()) {
        final Stopwatch dutyRunTime = Stopwatch.createStarted();
        params = duty.run(params);
        dutyRunTime.stop();

        final String dutyName = duty.getClass().getName();
        if (params == null) {
          log.warn("Stopping run for group[%s] on request of duty[%s].", name, dutyName);
          return;
        } else {
          stats.add(
              Stats.CoordinatorRun.DUTY_RUN_TIME,
              RowKey.of(Dimension.DUTY, dutyName),
              dutyRunTime.millisElapsed()
          );
        }
      }
    }

    // Emit stats collected from all duties
    if (stats.rowCount() > 0) {
      stats.forEachStat(this::emitStat);

      final String statsTable = stats.buildStatsTable();
      if (!statsTable.isEmpty()) {
        log.info("Collected stats for duty group[%s]: %s", name, statsTable);
      }
    }

    // Emit the runtime of the entire duty group
    final long runMillis = markRunCompleted();
    emitStat(Stats.CoordinatorRun.GROUP_RUN_TIME, RowKey.empty(), runMillis);
  }

  private void emitStat(CoordinatorStat stat, RowKey rowKey, long value)
  {
    if (stat.shouldEmit()) {
      coordinator.emitStat(stat, rowKey, value);
    }
  }

  private synchronized long computeWindowAverage(EvictingQueue<Long> window)
  {
    final int numEntries = window.size();
    if (numEntries > 0) {
      long totalTimeMillis = window.stream().mapToLong(Long::longValue).sum();
      return totalTimeMillis / numEntries;
    } else {
      return 0;
    }
  }

  private synchronized void addToWindow(EvictingQueue<Long> window, long value)
  {
    window.add(value);
  }

  private void markRunStarted()
  {
    final DateTime now = DateTimes.nowUtc();

    final DateTime lastStart = lastRunStartTime.getAndSet(now);
    if (lastStart != null) {
      addToWindow(gapTimes, now.getMillis() - lastStart.getMillis());
    }
  }

  private long markRunCompleted()
  {
    final DateTime now = DateTimes.nowUtc();
    lastRunEndTime.set(now);

    final long runtimeMillis = now.getMillis() - lastRunStartTime.get().getMillis();
    addToWindow(runTimes, runtimeMillis);

    return runtimeMillis;
  }
}
