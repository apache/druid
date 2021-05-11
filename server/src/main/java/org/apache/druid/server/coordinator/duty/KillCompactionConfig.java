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
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.stream.Collectors;

public class KillCompactionConfig implements CoordinatorDuty
{
  private static final Logger log = new Logger(KillCompactionConfig.class);
  private static final long UPDATE_RETRY_DELAY = 1000;
  private static final int UPDATE_NUM_RETRY = 5;

  static final String COUNT_METRIC = "metadata/kill/compaction/count";

  private final long period;
  private long lastKillTime = 0;

  private final JacksonConfigManager jacksonConfigManager;
  private final SqlSegmentsMetadataManager sqlSegmentsMetadataManager;

  @Inject
  public KillCompactionConfig(
      DruidCoordinatorConfig config,
      SqlSegmentsMetadataManager sqlSegmentsMetadataManager,
      JacksonConfigManager jacksonConfigManager
  )
  {
    this.sqlSegmentsMetadataManager = sqlSegmentsMetadataManager;
    this.jacksonConfigManager = jacksonConfigManager;
    this.period = config.getCoordinatorCompactionKillPeriod().getMillis();
    Preconditions.checkArgument(
        this.period >= config.getCoordinatorMetadataStoreManagementPeriod().getMillis(),
        "Coordinator compaction configuration kill period must be >= druid.coordinator.period.metadataStoreManagementPeriod"
    );
    log.debug(
        "Compaction Configuration Kill Task scheduling enabled with period [%s]",
        this.period
    );
  }

  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    long currentTimeMillis = System.currentTimeMillis();
    if ((lastKillTime + period) < currentTimeMillis) {
      lastKillTime = currentTimeMillis;
      // If current compaction config is empty then there is nothing to do
      CoordinatorCompactionConfig current = CoordinatorCompactionConfig.current(jacksonConfigManager);
      if (CoordinatorCompactionConfig.empty().equals(current)) {
        log.info("Finished running KillCompactionConfig duty. Nothing to do as compaction config is already empty.");
        emitMetric(params.getEmitter(), 0);
        return params;
      }
      int attemps = 0;
      ConfigManager.SetResult setResult = null;
      int compactionConfigRemoved = 0;
      try {
        while (attemps < UPDATE_NUM_RETRY) {
          // Get all active datasources
          // Note that we get all active datasources after getting compaction config to prevent race condition if new
          // datasource and config are added.
          Set<String> activeDatasources = sqlSegmentsMetadataManager.retrieveAllDataSourceNames();

          final Map<String, DataSourceCompactionConfig> updated = current
              .getCompactionConfigs()
              .stream()
              .filter(dataSourceCompactionConfig -> activeDatasources.contains(dataSourceCompactionConfig.getDataSource()))
              .collect(Collectors.toMap(DataSourceCompactionConfig::getDataSource, Function.identity()));

          // Calculate number of compaction configs to remove for logging
          compactionConfigRemoved = current.getCompactionConfigs().size() - updated.size();

          setResult = jacksonConfigManager.set(
              CoordinatorCompactionConfig.CONFIG_KEY,
              // Do database insert without swap if the current config is empty as this means the config may be null in the database
              current,
              CoordinatorCompactionConfig.from(current, ImmutableList.copyOf(updated.values())),
              new AuditInfo("KillCompactionConfig", "CoordinatorDuty for automatic deletion of compaction config", "")
          );

          if (setResult.isOk() || !setResult.isRetryable()) {
            break;
          }
          attemps++;
          updateRetryDelay();
          // Refresh current view of Compaction Configuration before trying again
          current = CoordinatorCompactionConfig.current(jacksonConfigManager);
        }
      }
      catch (Exception e) {
        log.error(e, "Failed to kill compaction configurations");
        emitMetric(params.getEmitter(), 0);
        return params;
      }

      if (setResult.isOk()) {
        log.info("Finished running KillCompactionConfig duty. Removed %,d compaction configs", compactionConfigRemoved);
        emitMetric(params.getEmitter(), compactionConfigRemoved);
      } else {
        log.error(setResult.getException(), "Failed to kill compaction configurations");
        emitMetric(params.getEmitter(), 0);
      }
    }
    return params;
  }

  private void updateRetryDelay()
  {
    try {
      Thread.sleep(ThreadLocalRandom.current().nextLong(UPDATE_RETRY_DELAY));
    }
    catch (InterruptedException ie) {
      throw new RuntimeException(ie);
    }
  }

  private void emitMetric(ServiceEmitter emitter, int compactionConfigRemoved)
  {
    emitter.emit(
        new ServiceMetricEvent.Builder().build(
            COUNT_METRIC,
            compactionConfigRemoved
        )
    );
  }
}
