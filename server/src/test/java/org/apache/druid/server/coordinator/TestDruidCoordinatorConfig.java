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

public class TestDruidCoordinatorConfig extends DruidCoordinatorConfig
{

  private final Duration coordinatorStartDelay;
  private final Duration coordinatorPeriod;
  private final Duration coordinatorIndexingPeriod;
  private final Duration metadataStoreManagementPeriod;
  private final Duration loadTimeoutDelay;
  private final Duration coordinatorKillPeriod;
  private final Duration coordinatorKillDurationToRetain;
  private final Duration coordinatorSupervisorKillPeriod;
  private final Duration coordinatorSupervisorKillDurationToRetain;
  private final Duration coordinatorAuditKillPeriod;
  private final Duration coordinatorAuditKillDurationToRetain;
  private final Duration coordinatorCompactionKillPeriod;
  private final Duration coordinatorRuleKillPeriod;
  private final Duration coordinatorRuleKillDurationToRetain;
  private final Duration coordinatorDatasourceKillPeriod;
  private final Duration coordinatorDatasourceKillDurationToRetain;
  private final Duration getLoadQueuePeonRepeatDelay;
  private final int coordinatorKillMaxSegments;

  private String tierMirroringMap = null;

  public TestDruidCoordinatorConfig(
      Duration coordinatorStartDelay,
      Duration coordinatorPeriod,
      Duration coordinatorIndexingPeriod,
      Duration metadataStoreManagementPeriod,
      Duration loadTimeoutDelay,
      Duration coordinatorKillPeriod,
      Duration coordinatorKillDurationToRetain,
      Duration coordinatorSupervisorKillPeriod,
      Duration coordinatorSupervisorKillDurationToRetain,
      Duration coordinatorAuditKillPeriod,
      Duration coordinatorAuditKillDurationToRetain,
      Duration coordinatorCompactionKillPeriod,
      Duration coordinatorRuleKillPeriod,
      Duration coordinatorRuleKillDurationToRetain,
      Duration coordinatorDatasourceKillPeriod,
      Duration coordinatorDatasourceKillDurationToRetain,
      int coordinatorKillMaxSegments,
      Duration getLoadQueuePeonRepeatDelay
  )
  {
    this.coordinatorStartDelay = coordinatorStartDelay;
    this.coordinatorPeriod = coordinatorPeriod;
    this.coordinatorIndexingPeriod = coordinatorIndexingPeriod;
    this.metadataStoreManagementPeriod = metadataStoreManagementPeriod;
    this.loadTimeoutDelay = loadTimeoutDelay;
    this.coordinatorKillPeriod = coordinatorKillPeriod;
    this.coordinatorKillDurationToRetain = coordinatorKillDurationToRetain;
    this.coordinatorSupervisorKillPeriod = coordinatorSupervisorKillPeriod;
    this.coordinatorSupervisorKillDurationToRetain = coordinatorSupervisorKillDurationToRetain;
    this.coordinatorAuditKillPeriod = coordinatorAuditKillPeriod;
    this.coordinatorAuditKillDurationToRetain = coordinatorAuditKillDurationToRetain;
    this.coordinatorCompactionKillPeriod = coordinatorCompactionKillPeriod;
    this.coordinatorRuleKillPeriod = coordinatorRuleKillPeriod;
    this.coordinatorRuleKillDurationToRetain = coordinatorRuleKillDurationToRetain;
    this.coordinatorDatasourceKillPeriod = coordinatorDatasourceKillPeriod;
    this.coordinatorDatasourceKillDurationToRetain = coordinatorDatasourceKillDurationToRetain;
    this.coordinatorKillMaxSegments = coordinatorKillMaxSegments;
    this.getLoadQueuePeonRepeatDelay = getLoadQueuePeonRepeatDelay;
  }

  @Override
  public Duration getCoordinatorStartDelay()
  {
    return coordinatorStartDelay;
  }

  @Override
  public Duration getCoordinatorPeriod()
  {
    return coordinatorPeriod;
  }

  @Override
  public Duration getCoordinatorIndexingPeriod()
  {
    return coordinatorIndexingPeriod;
  }

  @Override
  public Duration getCoordinatorMetadataStoreManagementPeriod()
  {
    return metadataStoreManagementPeriod;
  }

  @Override
  public Duration getCoordinatorKillPeriod()
  {
    return coordinatorKillPeriod;
  }

  @Override
  public Duration getCoordinatorKillDurationToRetain()
  {
    return coordinatorKillDurationToRetain;
  }

  @Override
  public Duration getCoordinatorSupervisorKillPeriod()
  {
    return coordinatorSupervisorKillPeriod;
  }

  @Override
  public Duration getCoordinatorSupervisorKillDurationToRetain()
  {
    return coordinatorSupervisorKillDurationToRetain;
  }

  @Override
  public Duration getCoordinatorAuditKillPeriod()
  {
    return coordinatorAuditKillPeriod;
  }

  @Override
  public Duration getCoordinatorAuditKillDurationToRetain()
  {
    return coordinatorAuditKillDurationToRetain;
  }

  @Override
  public Duration getCoordinatorCompactionKillPeriod()
  {
    return coordinatorCompactionKillPeriod;
  }

  @Override
  public Duration getCoordinatorRuleKillPeriod()
  {
    return coordinatorRuleKillPeriod;
  }

  @Override
  public Duration getCoordinatorRuleKillDurationToRetain()
  {
    return coordinatorRuleKillDurationToRetain;
  }

  @Override
  public Duration getCoordinatorDatasourceKillPeriod()
  {
    return coordinatorDatasourceKillPeriod;
  }

  @Override
  public Duration getCoordinatorDatasourceKillDurationToRetain()
  {
    return coordinatorDatasourceKillDurationToRetain;
  }

  @Override
  public int getCoordinatorKillMaxSegments()
  {
    return coordinatorKillMaxSegments;
  }

  @Override
  public Duration getLoadTimeoutDelay()
  {
    return loadTimeoutDelay == null ? super.getLoadTimeoutDelay() : loadTimeoutDelay;
  }

  @Override
  public Duration getLoadQueuePeonRepeatDelay()
  {
    return getLoadQueuePeonRepeatDelay;
  }

  @Override public String getTierMirroringMapConfigured()
  {
    return tierMirroringMap;
  }

}
