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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;

import javax.annotation.Nullable;

/**
 * Supervisor for compaction of a single datasource.
 */
public class CompactionSupervisor implements Supervisor
{
  private static final Logger log = new Logger(CompactionSupervisor.class);

  private final String dataSource;
  private final CompactionScheduler scheduler;
  private final CompactionSupervisorSpec supervisorSpec;

  public CompactionSupervisor(
      CompactionSupervisorSpec supervisorSpec,
      CompactionScheduler scheduler
  )
  {
    this.supervisorSpec = supervisorSpec;
    this.scheduler = scheduler;
    this.dataSource = supervisorSpec.getSpec().getDataSource();
  }

  @Override
  public void start()
  {
    if (supervisorSpec.isSuspended()) {
      log.info("Suspending compaction for dataSource[%s].", dataSource);
      scheduler.stopCompaction(dataSource);
    } else if (!supervisorSpec.getValidationResult().isValid()) {
      log.warn(
          "Cannot start compaction supervisor for datasource[%s] since the compaction supervisor spec is invalid. "
          + "Reason[%s].",
          dataSource,
          supervisorSpec.getValidationResult().getReason()
      );
    } else {
      log.info("Starting compaction for dataSource[%s].", dataSource);
      scheduler.startCompaction(dataSource, supervisorSpec.getSpec());
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    log.info("Stopping compaction for dataSource[%s].", dataSource);
    scheduler.stopCompaction(dataSource);
  }

  @Override
  public SupervisorReport<AutoCompactionSnapshot> getStatus()
  {
    final AutoCompactionSnapshot snapshot;
    if (supervisorSpec.isSuspended()) {
      snapshot = AutoCompactionSnapshot.builder(dataSource)
                                       .withStatus(AutoCompactionSnapshot.AutoCompactionScheduleStatus.NOT_ENABLED)
                                       .build();
    } else if (!supervisorSpec.getValidationResult().isValid()) {
      snapshot = AutoCompactionSnapshot.builder(dataSource)
                                       .withMessage(StringUtils.format(
                                           "Compaction supervisor spec is invalid. Reason[%s].",
                                           supervisorSpec.getValidationResult().getReason()
                                       ))
                                       .build();
    } else {
      snapshot = scheduler.getCompactionSnapshot(dataSource);
    }

    return new SupervisorReport<>(supervisorSpec.getId(), DateTimes.nowUtc(), snapshot);
  }

  @Override
  public SupervisorStateManager.State getState()
  {
    if (!scheduler.isRunning()) {
      return State.SCHEDULER_STOPPED;
    } else if (supervisorSpec.isSuspended()) {
      return State.SUSPENDED;
    } else if (!supervisorSpec.getValidationResult().isValid()) {
      return State.INVALID_SPEC;
    } else {
      return State.RUNNING;
    }
  }

  @Override
  public void reset(@Nullable DataSourceMetadata dataSourceMetadata)
  {
    // do nothing
  }

  public enum State implements SupervisorStateManager.State
  {
    SCHEDULER_STOPPED(true),
    RUNNING(true),
    SUSPENDED(true),
    INVALID_SPEC(false),
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
