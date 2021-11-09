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
import org.apache.druid.java.util.RetryableException;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CoordinatorDuty for automatic deletion of compaction configurations from the config table in metadata storage.
 * Note that this will delete compaction configuration for inactive datasources
 * (datasource with no used and unused segments) immediately.
 */
public class KillCompactionConfig implements CoordinatorDuty
{
  private static final Logger log = new Logger(KillCompactionConfig.class);
  private static final int UPDATE_NUM_RETRY = 5;

  static final String COUNT_METRIC = "metadata/kill/compaction/count";

  private final long period;
  private long lastKillTime = 0;

  private final JacksonConfigManager jacksonConfigManager;
  private final SqlSegmentsMetadataManager sqlSegmentsMetadataManager;
  private final MetadataStorageConnector connector;
  private final MetadataStorageTablesConfig connectorConfig;

  @Inject
  public KillCompactionConfig(
      DruidCoordinatorConfig config,
      SqlSegmentsMetadataManager sqlSegmentsMetadataManager,
      JacksonConfigManager jacksonConfigManager,
      MetadataStorageConnector connector,
      MetadataStorageTablesConfig connectorConfig
  )
  {
    this.sqlSegmentsMetadataManager = sqlSegmentsMetadataManager;
    this.jacksonConfigManager = jacksonConfigManager;
    this.period = config.getCoordinatorCompactionKillPeriod().getMillis();
    this.connector = connector;
    this.connectorConfig = connectorConfig;
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
      try {
        RetryUtils.retry(
            () -> {
              final byte[] currentBytes = CoordinatorCompactionConfig.getConfigInByteFromDb(connector, connectorConfig);
              final CoordinatorCompactionConfig current = CoordinatorCompactionConfig.convertByteToConfig(jacksonConfigManager, currentBytes);
              // If current compaction config is empty then there is nothing to do
              if (CoordinatorCompactionConfig.empty().equals(current)) {
                log.info(
                    "Finished running KillCompactionConfig duty. Nothing to do as compaction config is already empty.");
                emitMetric(params.getEmitter(), 0);
                return ConfigManager.SetResult.ok();
              }

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
              int compactionConfigRemoved = current.getCompactionConfigs().size() - updated.size();

              ConfigManager.SetResult result = jacksonConfigManager.set(
                  CoordinatorCompactionConfig.CONFIG_KEY,
                  currentBytes,
                  CoordinatorCompactionConfig.from(current, ImmutableList.copyOf(updated.values())),
                  new AuditInfo(
                      "KillCompactionConfig",
                      "CoordinatorDuty for automatic deletion of compaction config",
                      ""
                  )
              );
              if (result.isOk()) {
                log.info(
                    "Finished running KillCompactionConfig duty. Removed %,d compaction configs",
                    compactionConfigRemoved
                );
                emitMetric(params.getEmitter(), compactionConfigRemoved);
              } else if (result.isRetryable()) {
                // Failed but is retryable
                log.debug("Retrying KillCompactionConfig duty");
                throw new RetryableException(result.getException());
              } else {
                // Failed and not retryable
                log.error(result.getException(), "Failed to kill compaction configurations");
                emitMetric(params.getEmitter(), 0);
              }
              return result;
            },
            e -> e instanceof RetryableException,
            UPDATE_NUM_RETRY
        );
      }
      catch (Exception e) {
        log.error(e, "Failed to kill compaction configurations");
        emitMetric(params.getEmitter(), 0);
      }
    }
    return params;
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
