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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.joda.time.Duration;

/**
 * CoordinatorDuty for automatic deletion of terminated supervisors from the supervisor table in metadata storage.
 * This class has the same purpose as {@link KillSupervisors} but uses a different configuration style as
 * detailed in {@link CoordinatorCustomDuty}. This class primary purpose is as an example to demostrate the usuage
 * of the {@link CoordinatorCustomDuty} {@link org.apache.druid.guice.annotations.ExtensionPoint}
 *
 * Production use case should still use {@link KillSupervisors}. In the future, we might migrate all metadata
 * management coordinator duties to {@link CoordinatorCustomDuty} but until then this class will remains undocumented
 * and should not be use in production.
 */
public class KillSupervisorsCustomDuty implements CoordinatorCustomDuty
{
  private static final Logger log = new Logger(KillSupervisorsCustomDuty.class);

  private final Duration retainDuration;
  private final MetadataSupervisorManager metadataSupervisorManager;

  @JsonCreator
  public KillSupervisorsCustomDuty(
      @JsonProperty("retainDuration") Duration retainDuration,
      @JacksonInject MetadataSupervisorManager metadataSupervisorManager
  )
  {
    this.metadataSupervisorManager = metadataSupervisorManager;
    this.retainDuration = retainDuration;
    Preconditions.checkArgument(this.retainDuration != null && this.retainDuration.getMillis() >= 0, "(Custom Duty) Coordinator supervisor kill retainDuration must be >= 0");
    log.info(
        "Supervisor Kill Task scheduling enabled with retainDuration [%s]",
        this.retainDuration
    );
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    long timestamp = System.currentTimeMillis() - retainDuration.getMillis();
    try {
      int supervisorRemoved = metadataSupervisorManager.removeTerminatedSupervisorsOlderThan(timestamp);
      ServiceEmitter emitter = params.getEmitter();
      emitter.emit(
          new ServiceMetricEvent.Builder().build(
              "metadata/kill/supervisor/count",
              supervisorRemoved
          )
      );
      log.info("Finished running KillSupervisors duty. Removed %,d supervisor specs", supervisorRemoved);
    }
    catch (Exception e) {
      log.error(e, "Failed to kill terminated supervisor metadata");
    }
    return params;
  }
}
