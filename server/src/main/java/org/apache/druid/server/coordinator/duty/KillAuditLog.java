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
import org.apache.druid.audit.AuditManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;

public class KillAuditLog implements CoordinatorDuty
{
  private static final Logger log = new Logger(KillAuditLog.class);

  private final long period;
  private final long retainDuration;
  private long lastKillTime = 0;

  private final AuditManager auditManager;

  @Inject
  public KillAuditLog(
      AuditManager auditManager,
      DruidCoordinatorConfig config
  )
  {
    this.period = config.getCoordinatorAuditKillPeriod().getMillis();
    Preconditions.checkArgument(
        this.period >= config.getCoordinatorMetadataStoreManagementPeriod().getMillis(),
        "coordinator audit kill period must be >= druid.coordinator.period.metadataStoreManagementPeriod"
    );
    this.retainDuration = config.getCoordinatorAuditKillDurationToRetain().getMillis();
    Preconditions.checkArgument(this.retainDuration >= 0, "coordinator audit kill retainDuration must be >= 0");
    Preconditions.checkArgument(this.retainDuration < System.currentTimeMillis(), "Coordinator audit kill retainDuration cannot be greater than current time in ms");
    log.debug(
        "Audit Kill Task scheduling enabled with period [%s], retainDuration [%s]",
        this.period,
        this.retainDuration
    );
    this.auditManager = auditManager;
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    long currentTimeMillis = System.currentTimeMillis();
    if ((lastKillTime + period) < currentTimeMillis) {
      lastKillTime = currentTimeMillis;
      long timestamp = currentTimeMillis - retainDuration;
      try {
        int auditRemoved = auditManager.removeAuditLogsOlderThan(timestamp);
        ServiceEmitter emitter = params.getEmitter();
        emitter.emit(
            new ServiceMetricEvent.Builder().build(
                "metadata/kill/audit/count",
                auditRemoved
            )
        );
        log.info("Finished running KillAuditLog duty. Removed %,d audit logs", auditRemoved);
      }
      catch (Exception e) {
        log.error(e, "Failed to kill audit log");
      }
    }
    return params;
  }
}
