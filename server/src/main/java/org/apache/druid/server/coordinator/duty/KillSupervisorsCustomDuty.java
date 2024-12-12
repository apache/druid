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
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.config.MetadataCleanupConfig;
import org.joda.time.Duration;

/**
 * Example {@link CoordinatorCustomDuty} for automatic deletion of terminated
 * supervisors from the metadata storage. This duty has the same implementation
 * as {@link KillSupervisors} but uses a different configuration style as
 * detailed in {@link CoordinatorCustomDuty}.
 * <p>
 * This duty is only an example to demonstrate the usage of coordinator custom
 * duties. All production clusters should continue using {@link KillSupervisors}.
 */
@UnstableApi
public class KillSupervisorsCustomDuty implements CoordinatorCustomDuty
{
  private static final Logger log = new Logger(KillSupervisorsCustomDuty.class);

  private final KillSupervisors delegate;

  @JsonCreator
  public KillSupervisorsCustomDuty(
      @JsonProperty("durationToRetain") Duration retainDuration,
      @JacksonInject MetadataSupervisorManager metadataSupervisorManager
  )
  {
    this.delegate = new KillSupervisors(
        // Pass period as zero here, actual period of custom duties is configured at the duty group level
        new MetadataCleanupConfig(true, Duration.ZERO, retainDuration),
        metadataSupervisorManager
    );
    log.warn("This is only an example implementation of a custom duty and"
             + " must not be used in production. Use KillSupervisors duty instead.");
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    return delegate.run(params);
  }
}
