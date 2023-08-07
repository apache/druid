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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.RetryableException;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataManager;
import org.apache.druid.server.coordinator.CoordinatorCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * CoordinatorDuty for automatic deletion of compaction configurations from the config table in metadata storage.
 * Note that this will delete compaction configuration for inactive datasources
 * (datasource with no used and unused segments) immediately.
 */
public class KillCompactionConfig extends MetadataCleanupDuty
{
  private static final Logger log = new Logger(KillCompactionConfig.class);
  private static final int UPDATE_NUM_RETRY = 5;

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
    super(
        "compaction configs",
        "druid.coordinator.kill.compaction",
        config.getCoordinatorCompactionKillPeriod(),
        Duration.millis(1), // Retain duration is ignored
        Stats.Kill.COMPACTION_CONFIGS,
        config
    );
    this.sqlSegmentsMetadataManager = sqlSegmentsMetadataManager;
    this.jacksonConfigManager = jacksonConfigManager;
    this.connector = connector;
    this.connectorConfig = connectorConfig;
  }

  @Override
  protected int cleanupEntriesCreatedBefore(DateTime minCreatedTime)
  {
    try {
      return RetryUtils.retry(
          this::tryDeleteCompactionConfigs,
          e -> e instanceof RetryableException,
          UPDATE_NUM_RETRY
      );
    }
    catch (Exception e) {
      log.error(e, "Failed to kill compaction configurations");
      return 0;
    }
  }

  /**
   * Tries to delete compaction configs for inactive datasources and returns
   * the number of compaction configs successfully removed.
   */
  private int tryDeleteCompactionConfigs() throws RetryableException
  {
    final byte[] currentBytes = CoordinatorCompactionConfig.getConfigInByteFromDb(connector, connectorConfig);
    final CoordinatorCompactionConfig current = CoordinatorCompactionConfig.convertByteToConfig(
        jacksonConfigManager,
        currentBytes
    );
    // If current compaction config is empty then there is nothing to do
    if (CoordinatorCompactionConfig.empty().equals(current)) {
      log.info("Nothing to do as compaction config is already empty.");
      return 0;
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

    // Calculate number of compaction configs removed
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
      return compactionConfigRemoved;
    } else if (result.isRetryable()) {
      log.debug("Retrying KillCompactionConfig duty");
      throw new RetryableException(result.getException());
    } else {
      log.error(result.getException(), "Failed to kill compaction configurations");
      return 0;
    }
  }
}
