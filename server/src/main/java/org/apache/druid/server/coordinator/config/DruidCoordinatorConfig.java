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

package org.apache.druid.server.coordinator.config;

import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.server.coordinator.balancer.BalancerStrategyFactory;
import org.joda.time.Duration;

/**
 * Contains all static configs for the Coordinator.
 */
public class DruidCoordinatorConfig
{
  private final CoordinatorRunConfig runConfig;
  private final CoordinatorPeriodConfig periodConfig;
  private final CoordinatorKillConfigs killConfigs;
  private final BalancerStrategyFactory balancerStrategyFactory;
  private final HttpLoadQueuePeonConfig httpLoadQueuePeonConfig;

  @Inject
  public DruidCoordinatorConfig(
      CoordinatorRunConfig runConfig,
      CoordinatorPeriodConfig periodConfig,
      CoordinatorKillConfigs killConfigs,
      BalancerStrategyFactory balancerStrategyFactory,
      HttpLoadQueuePeonConfig httpLoadQueuePeonConfig
  )
  {
    this.killConfigs = killConfigs;
    this.runConfig = runConfig;
    this.periodConfig = periodConfig;
    this.balancerStrategyFactory = balancerStrategyFactory;
    this.httpLoadQueuePeonConfig = httpLoadQueuePeonConfig;

    validateKillConfigs();
  }

  public Duration getCoordinatorStartDelay()
  {
    return runConfig.getStartDelay();
  }

  public Duration getCoordinatorPeriod()
  {
    return runConfig.getPeriod();
  }

  public Duration getCoordinatorIndexingPeriod()
  {
    return periodConfig.getIndexingPeriod();
  }

  public Duration getCoordinatorMetadataStoreManagementPeriod()
  {
    return periodConfig.getMetadataStoreManagementPeriod();
  }

  public CoordinatorKillConfigs getKillConfigs()
  {
    return killConfigs;
  }

  public BalancerStrategyFactory getBalancerStrategyFactory()
  {
    return balancerStrategyFactory;
  }

  public HttpLoadQueuePeonConfig getHttpLoadQueuePeonConfig()
  {
    return httpLoadQueuePeonConfig;
  }

  private void validateKillConfigs()
  {
    validateKillConfig(killConfigs.auditLogs(), "audit");
    validateKillConfig(killConfigs.compactionConfigs(), "compaction");
    validateKillConfig(killConfigs.datasources(), "datasource");
    validateKillConfig(killConfigs.rules(), "rule");
    validateKillConfig(killConfigs.supervisors(), "supervisor");
    validateKillConfig(killConfigs.segmentSchemas(), "segmentSchema");

    // Validate config for killing unused segments
    final KillUnusedSegmentsConfig killUnusedConfig
        = killConfigs.unusedSegments(getCoordinatorIndexingPeriod());
    if (killUnusedConfig.getCleanupPeriod().getMillis() < getCoordinatorIndexingPeriod().getMillis()) {
      throw newInvalidInputExceptionForOperator(
          "'druid.coordinator.kill.period'[%s] must be greater than or equal to"
          + " 'druid.coordinator.period.indexingPeriod'[%s]",
          killUnusedConfig.getCleanupPeriod(), getCoordinatorIndexingPeriod()
      );
    }
    if (killUnusedConfig.getMaxSegments() < 0) {
      throw newInvalidInputExceptionForOperator(
          "'druid.coordinator.kill.maxSegments'[%s] must be a positive integer.",
          killUnusedConfig.getMaxSegments()
      );
    }
  }

  private void validateKillConfig(MetadataCleanupConfig config, String propertyPrefix)
  {
    if (!config.isCleanupEnabled()) {
      // Do not perform validation if cleanup is disabled
      return;
    }

    final Duration metadataManagementPeriod = getCoordinatorMetadataStoreManagementPeriod();
    final Duration period = config.getCleanupPeriod();
    if (period == null || period.getMillis() < metadataManagementPeriod.getMillis()) {
      throw newInvalidInputExceptionForOperator(
          "'druid.coordinator.kill.%s.period'[%s] must be greater than"
          + " 'druid.coordinator.period.metadataStoreManagementPeriod'[%s]",
          propertyPrefix, period, metadataManagementPeriod
      );
    }

    final Duration retainDuration = config.getDurationToRetain();
    if (retainDuration == null || retainDuration.getMillis() < 0) {
      throw newInvalidInputExceptionForOperator(
          "'druid.coordinator.kill.%s.durationToRetain'[%s] must be 0 milliseconds or higher",
          propertyPrefix, retainDuration
      );
    }
    if (retainDuration.getMillis() > System.currentTimeMillis()) {
      throw newInvalidInputExceptionForOperator(
          "'druid.coordinator.kill.%s.durationToRetain'[%s] cannot be greater than current time in milliseconds",
          propertyPrefix, retainDuration
      );
    }
  }

  private static DruidException newInvalidInputExceptionForOperator(String msgFormat, Object... args)
  {
    return DruidException.forPersona(DruidException.Persona.OPERATOR)
                         .ofCategory(DruidException.Category.INVALID_INPUT)
                         .build(msgFormat, args);
  }

}
