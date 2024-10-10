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

import org.apache.druid.metadata.MetadataSupervisorManager;
import org.apache.druid.server.coordinator.config.MetadataCleanupConfig;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.DateTime;

/**
 * Cleans up terminated supervisors from the supervisors table in metadata storage.
 */
public class KillSupervisors extends MetadataCleanupDuty
{
  private final MetadataSupervisorManager metadataSupervisorManager;

  public KillSupervisors(
      MetadataCleanupConfig config,
      MetadataSupervisorManager metadataSupervisorManager
  )
  {
    super("supervisors", config, Stats.Kill.SUPERVISOR_SPECS);
    this.metadataSupervisorManager = metadataSupervisorManager;
  }

  @Override
  protected int cleanupEntriesCreatedBefore(DateTime minCreatedTime)
  {
    return metadataSupervisorManager.removeTerminatedSupervisorsOlderThan(minCreatedTime.getMillis());
  }
}
