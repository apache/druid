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

package org.apache.druid.indexing.scheduledbatch;

import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.supervisor.Supervisor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;

public class ScheduledBatchSupervisor implements Supervisor
{
  private static final Logger log = new Logger(ScheduledBatchSupervisor.class);
  private final ScheduledBatchSupervisorSpec supervisorSpec;
  private final ScheduledBatchTaskManager batchTaskManager;

  public ScheduledBatchSupervisor(
      final ScheduledBatchSupervisorSpec supervisorSpec,
      final ScheduledBatchTaskManager batchTaskManager
  )
  {
    this.supervisorSpec = supervisorSpec;
    this.batchTaskManager = batchTaskManager;
  }

  @Override
  public void start()
  {
    if (supervisorSpec.isSuspended()) {
      batchTaskManager.stopScheduledIngestion(supervisorSpec.getId());
    } else {
      batchTaskManager.startScheduledIngestion(
          supervisorSpec.getId(),
          supervisorSpec.getDataSources().get(0),
          supervisorSpec.getSchedulerConfig(),
          supervisorSpec.getSpec()
      );
    }
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    batchTaskManager.stopScheduledIngestion(supervisorSpec.getId());
  }

  @Override
  public SupervisorReport<ScheduledBatchSupervisorStatus> getStatus()
  {
    return new SupervisorReport<>(
        supervisorSpec.getId(),
        DateTimes.nowUtc(),
        batchTaskManager.getSupervisorStatus(supervisorSpec.getId())
    );
  }

  @Override
  public SupervisorStateManager.State getState()
  {
    if (supervisorSpec.isSuspended()) {
      return State.SUSPENDED;
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
    RUNNING(true),
    SUSPENDED(true);

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
