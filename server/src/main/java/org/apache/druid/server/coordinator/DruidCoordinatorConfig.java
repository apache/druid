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

package org.apache.druid.server.coordinator;

import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class DruidCoordinatorConfig
{
  @Config("druid.coordinator.startDelay")
  @Default("PT300s")
  public abstract Duration getCoordinatorStartDelay();

  @Config("druid.coordinator.period")
  @Default("PT60s")
  public abstract Duration getCoordinatorPeriod();

  @Config("druid.coordinator.period.indexingPeriod")
  @Default("PT1800s")
  public abstract Duration getCoordinatorIndexingPeriod();

  @Config("druid.coordinator.period.metadataStoreManagementPeriod")
  @Default("PT1H")
  public abstract Duration getCoordinatorMetadataStoreManagementPeriod();

  @Config("druid.coordinator.kill.period")
  @Default("P1D")
  public abstract Duration getCoordinatorKillPeriod();

  @Config("druid.coordinator.kill.durationToRetain")
  @Default("P90D")
  public abstract Duration getCoordinatorKillDurationToRetain();

  @Config("druid.coordinator.kill.ignoreDurationToRetain")
  @Default("false")
  public abstract boolean getCoordinatorKillIgnoreDurationToRetain();

  @Config("druid.coordinator.kill.maxSegments")
  @Default("100")
  public abstract int getCoordinatorKillMaxSegments();

  @Config("druid.coordinator.kill.supervisor.period")
  @Default("P1D")
  public abstract Duration getCoordinatorSupervisorKillPeriod();

  @Config("druid.coordinator.kill.supervisor.durationToRetain")
  @Default("P90D")
  public abstract Duration getCoordinatorSupervisorKillDurationToRetain();

  @Config("druid.coordinator.kill.audit.period")
  @Default("P1D")
  public abstract Duration getCoordinatorAuditKillPeriod();

  @Config("druid.coordinator.kill.audit.durationToRetain")
  @Default("P90D")
  public abstract Duration getCoordinatorAuditKillDurationToRetain();

  @Config("druid.coordinator.kill.compaction.period")
  @Default("P1D")
  public abstract Duration getCoordinatorCompactionKillPeriod();

  @Config("druid.coordinator.kill.rule.period")
  @Default("P1D")
  public abstract Duration getCoordinatorRuleKillPeriod();

  @Config("druid.coordinator.kill.rule.durationToRetain")
  @Default("P90D")
  public abstract Duration getCoordinatorRuleKillDurationToRetain();

  @Config("druid.coordinator.kill.datasource.period")
  @Default("P1D")
  public abstract Duration getCoordinatorDatasourceKillPeriod();

  @Config("druid.coordinator.kill.datasource.durationToRetain")
  @Default("P90D")
  public abstract Duration getCoordinatorDatasourceKillDurationToRetain();

  @Config("druid.coordinator.load.timeout")
  public Duration getLoadTimeoutDelay()
  {
    return new Duration(15 * 60 * 1000);
  }

  @Config("druid.coordinator.loadqueuepeon.repeatDelay")
  public Duration getLoadQueuePeonRepeatDelay()
  {
    return Duration.millis(50);
  }

  @Config("druid.coordinator.loadqueuepeon.type")
  public String getLoadQueuePeonType()
  {
    return "http";
  }

  @Config("druid.coordinator.curator.loadqueuepeon.numCallbackThreads")
  public int getNumCuratorCallBackThreads()
  {
    return 2;
  }

  @Config("druid.coordinator.loadqueuepeon.http.repeatDelay")
  public Duration getHttpLoadQueuePeonRepeatDelay()
  {
    return Duration.millis(60000);
  }

  @Config("druid.coordinator.loadqueuepeon.http.hostTimeout")
  public Duration getHttpLoadQueuePeonHostTimeout()
  {
    return Duration.millis(300000);
  }

  @Config("druid.coordinator.loadqueuepeon.http.batchSize")
  public int getHttpLoadQueuePeonBatchSize()
  {
    return 1;
  }

  @Config("druid.coordinator.compaction.skipLockedIntervals")
  public boolean getCompactionSkipLockedIntervals()
  {
    return true;
  }

}
