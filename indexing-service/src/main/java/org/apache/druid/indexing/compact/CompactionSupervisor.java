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

package org.apache.druid.indexing.compact;

import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;

/**
 * Supervisor for compaction of a single datasource.
 */
public class CompactionSupervisor implements Supervisor
{
  private static final Logger log = new Logger(CompactionSupervisor.class);

  private final CompactionScheduler scheduler;
  private final CompactionSupervisorSpec supervisorSpec;

  public CompactionSupervisor(
      CompactionSupervisorSpec supervisorSpec,
      CompactionScheduler scheduler
  )
  {
    this.supervisorSpec = supervisorSpec;
    this.scheduler = scheduler;
  }

  @Override
  public void start()
  {
    final String dataSource = getDataSource();
    if (supervisorSpec.isSuspended()) {
      log.info("Suspending compaction for dataSource[%s].", dataSource);
      scheduler.stopCompaction(dataSource);
    } else {
      log.info("Starting compaction for dataSource[%s].", dataSource);
      scheduler.startCompaction(dataSource, supervisorSpec.getSpec());
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    final String dataSource = getDataSource();
    log.info("Stopping compaction for dataSource[%s].", dataSource);
    scheduler.stopCompaction(dataSource);
  }

  @Override
  public SupervisorReport<AutoCompactionSnapshot> getStatus()
  {
    final AutoCompactionSnapshot snapshot;
    if (supervisorSpec.isSuspended()) {
      snapshot = AutoCompactionSnapshot.builder(getDataSource())
                                       .withStatus(AutoCompactionSnapshot.AutoCompactionScheduleStatus.NOT_ENABLED)
                                       .build();
    } else {
      snapshot = scheduler.getCompactionSnapshot(getDataSource());
    }

    return new SupervisorReport<>(supervisorSpec.getId(), DateTimes.nowUtc(), snapshot);
  }

  @Override
  public SupervisorStateManager.State getState()
  {
    if (!scheduler.isRunning()) {
      return State.SCHEDULER_DISABLED;
    } else if (supervisorSpec.isSuspended()) {
      return State.SUSPENDED;
    } else {
      return State.RUNNING;
    }
  }

  private String getDataSource()
  {
    return supervisorSpec.getSpec().getDataSource();
  }

  // Un-implemented methods used only by streaming supervisors

  @Override
  public void reset(DataSourceMetadata dataSourceMetadata)
  {
    // Do nothing
  }

  @Override
  public void resetOffsets(DataSourceMetadata resetDataSourceMetadata)
  {
    // Do nothing
  }

  @Override
  public void checkpoint(int taskGroupId, DataSourceMetadata checkpointMetadata)
  {
    // Do nothing
  }

  @Override
  public LagStats computeLagStats()
  {
    return new LagStats(0L, 0L, 0L);
  }

  @Override
  public int getActiveTaskGroupsCount()
  {
    return 0;
  }

  public enum State implements SupervisorStateManager.State
  {
    SCHEDULER_DISABLED(true),
    RUNNING(true),
    SUSPENDED(true),
    UNHEALTHY(false);

    private final boolean healthy;

    State(boolean healthy)
    {
      this.healthy = healthy;
    }

    @Override
    public boolean isFirstRunOnly()
    {
      return false;
    }

    @Override
    public boolean isHealthy()
    {
      return healthy;
    }
  }
}
