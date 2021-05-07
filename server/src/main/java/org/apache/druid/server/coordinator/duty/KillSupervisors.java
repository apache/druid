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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;

/**
 * CoordinatorDuty for automatic deletion of terminated supervisors from the supervisor table in metadata storage.
 */
public class KillSupervisors implements CoordinatorDuty
{
  private static final Logger log = new Logger(KillSupervisors.class);

  private final long period;
  private final long retainDuration;
  private long lastKillTime = 0;

  private final MetadataSupervisorManager metadataSupervisorManager;

  @Inject
  public KillSupervisors(
      DruidCoordinatorConfig config,
      MetadataSupervisorManager metadataSupervisorManager
  )
  {
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.period = config.getCoordinatorSupervisorKillPeriod().getMillis();
    Preconditions.checkArgument(
        this.period >= config.getCoordinatorMetadataStoreManagementPeriod().getMillis(),
        "Coordinator supervisor kill period must be >= druid.coordinator.period.metadataStoreManagementPeriod"
    );
    this.retainDuration = config.getCoordinatorSupervisorKillDurationToRetain().getMillis();
    Preconditions.checkArgument(this.retainDuration >= 0, "Coordinator supervisor kill retainDuration must be >= 0");
    log.debug(
        "Supervisor Kill Task scheduling enabled with period [%s], retainDuration [%s]",
        this.period,
        this.retainDuration
    );
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if ((lastKillTime + period) < System.currentTimeMillis()) {
      lastKillTime = System.currentTimeMillis();

      long timestamp = System.currentTimeMillis() - retainDuration;
      int supervisorRemoved = metadataSupervisorManager.removeTerminatedSupervisorsOlderThan(timestamp);
      ServiceEmitter emitter = params.getEmitter();
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "metadata/kill/supervisor/count",
              supervisorRemoved
          )
      );
      log.info("Finished running KillSupervisors duty. Removed %,d supervisor", supervisorRemoved);
    }
    return params;
  }
}
