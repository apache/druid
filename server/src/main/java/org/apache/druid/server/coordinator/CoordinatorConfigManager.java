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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.Configs;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.error.NotFound;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.UnaryOperator;

/**
 * Manager to fetch and update dynamic configs {@link CoordinatorDynamicConfig}
 * and {@link DruidCompactionConfig}.
 */
public class CoordinatorConfigManager
{
  private static final Logger log = new Logger(CoordinatorConfigManager.class);

  private static final long UPDATE_RETRY_DELAY = 1000;
  static final int MAX_UPDATE_RETRIES = 5;

  private final AuditManager auditManager;
  private final JacksonConfigManager jacksonConfigManager;
  private final MetadataStorageConnector metadataStorageConnector;
  private final MetadataStorageTablesConfig tablesConfig;

  @Inject
  public CoordinatorConfigManager(
      JacksonConfigManager jacksonConfigManager,
      MetadataStorageConnector metadataStorageConnector,
      MetadataStorageTablesConfig tablesConfig,
      AuditManager auditManager
  )
  {
    this.jacksonConfigManager = jacksonConfigManager;
    this.metadataStorageConnector = metadataStorageConnector;
    this.tablesConfig = tablesConfig;
    this.auditManager = auditManager;
  }

  public CoordinatorDynamicConfig getCurrentDynamicConfig()
  {
    CoordinatorDynamicConfig dynamicConfig = jacksonConfigManager.watch(
        CoordinatorDynamicConfig.CONFIG_KEY,
        CoordinatorDynamicConfig.class,
        CoordinatorDynamicConfig.builder().build()
    ).get();

    return Preconditions.checkNotNull(dynamicConfig, "Got null config from watcher?!");
  }

  public ConfigManager.SetResult setDynamicConfig(CoordinatorDynamicConfig config, AuditInfo auditInfo)
  {
    return jacksonConfigManager.set(
        CoordinatorDynamicConfig.CONFIG_KEY,
        config,
        auditInfo
    );
  }

  public DruidCompactionConfig getCurrentCompactionConfig()
  {
    DruidCompactionConfig config = jacksonConfigManager.watch(
        DruidCompactionConfig.CONFIG_KEY,
        DruidCompactionConfig.class,
        DruidCompactionConfig.empty()
    ).get();

    return Preconditions.checkNotNull(config, "Got null config from watcher?!");
  }

  /**
   * Gets the current compaction config and applies the given operator on it.
   * If the operator returns an updated config, it is persisted in the metadata
   * config store. This method is also compatible with pre-0.22.0 versions of Druid.
   *
   * @return A successful {@code SetResult} if the compaction is unchanged
   * or if the update was successful.
   */
  public ConfigManager.SetResult getAndUpdateCompactionConfig(
      UnaryOperator<DruidCompactionConfig> operator,
      AuditInfo auditInfo
  )
  {
    // Fetch the bytes and use to build the current config and perform compare-and-swap.
    // This avoids failures in ConfigManager while updating configs previously
    // persisted by older versions of Druid which didn't have fields such as 'granularitySpec'.
    final byte[] currentBytes = metadataStorageConnector.lookup(
        tablesConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        DruidCompactionConfig.CONFIG_KEY
    );
    DruidCompactionConfig current = convertBytesToCompactionConfig(currentBytes);
    DruidCompactionConfig updated = operator.apply(current);

    if (current.equals(updated)) {
      return ConfigManager.SetResult.ok();
    } else {
      return jacksonConfigManager.set(
          DruidCompactionConfig.CONFIG_KEY,
          currentBytes,
          updated,
          auditInfo
      );
    }
  }

  public DruidCompactionConfig convertBytesToCompactionConfig(byte[] bytes)
  {
    return jacksonConfigManager.convertByteToConfig(
        bytes,
        DruidCompactionConfig.class,
        DruidCompactionConfig.empty()
    );
  }

  public boolean updateCompactionTaskSlots(
      @Nullable Double compactionTaskSlotRatio,
      @Nullable Integer maxCompactionTaskSlots,
      AuditInfo auditInfo
  )
  {
    UnaryOperator<DruidCompactionConfig> operator = current -> {
      final ClusterCompactionConfig currentClusterConfig = current.clusterConfig();
      final ClusterCompactionConfig updatedClusterConfig = new ClusterCompactionConfig(
          Configs.valueOrDefault(compactionTaskSlotRatio, currentClusterConfig.getCompactionTaskSlotRatio()),
          Configs.valueOrDefault(maxCompactionTaskSlots, currentClusterConfig.getMaxCompactionTaskSlots()),
          currentClusterConfig.getCompactionPolicy(),
          currentClusterConfig.isUseSupervisors(),
          currentClusterConfig.getEngine()
      );

      return current.withClusterConfig(updatedClusterConfig);
    };

    return updateConfigHelper(operator, auditInfo);
  }

  public boolean updateClusterCompactionConfig(
      ClusterCompactionConfig config,
      AuditInfo auditInfo
  )
  {
    UnaryOperator<DruidCompactionConfig> operator = current -> current.withClusterConfig(config);
    return updateConfigHelper(operator, auditInfo);
  }

  public ClusterCompactionConfig getClusterCompactionConfig()
  {
    return getCurrentCompactionConfig().clusterConfig();
  }

  public boolean updateDatasourceCompactionConfig(
      DataSourceCompactionConfig config,
      AuditInfo auditInfo
  )
  {
    UnaryOperator<DruidCompactionConfig> callable = current -> current.withDatasourceConfig(config);
    return updateConfigHelper(callable, auditInfo);
  }

  public DataSourceCompactionConfig getDatasourceCompactionConfig(String dataSource)
  {
    final DruidCompactionConfig current = getCurrentCompactionConfig();
    final Optional<DataSourceCompactionConfig> config = current.findConfigForDatasource(dataSource);
    if (config.isPresent()) {
      return config.get();
    } else {
      throw NotFound.exception("Datasource compaction config does not exist");
    }
  }

  public boolean deleteDatasourceCompactionConfig(
      String dataSource,
      AuditInfo auditInfo
  )
  {
    UnaryOperator<DruidCompactionConfig> callable = current -> {
      final Map<String, DataSourceCompactionConfig> configs = current.dataSourceToCompactionConfigMap();
      final DataSourceCompactionConfig config = configs.remove(dataSource);
      if (config == null) {
        throw NotFound.exception("Datasource compaction config does not exist");
      }

      return current.withDatasourceConfigs(List.copyOf(configs.values()));
    };
    return updateConfigHelper(callable, auditInfo);
  }

  public List<DataSourceCompactionConfigAuditEntry> getCompactionConfigHistory(
      String dataSource,
      @Nullable String interval,
      @Nullable Integer count
  )
  {
    Interval theInterval = interval == null ? null : Intervals.of(interval);
    try {
      List<AuditEntry> auditEntries;
      if (theInterval == null && count != null) {
        auditEntries = auditManager.fetchAuditHistory(
            DruidCompactionConfig.CONFIG_KEY,
            DruidCompactionConfig.CONFIG_KEY,
            count
        );
      } else {
        auditEntries = auditManager.fetchAuditHistory(
            DruidCompactionConfig.CONFIG_KEY,
            DruidCompactionConfig.CONFIG_KEY,
            theInterval
        );
      }
      DataSourceCompactionConfigHistory history = new DataSourceCompactionConfigHistory(dataSource);
      for (AuditEntry audit : auditEntries) {
        DruidCompactionConfig compactionConfig = convertBytesToCompactionConfig(
            audit.getPayload().serialized().getBytes(StandardCharsets.UTF_8)
        );
        history.add(compactionConfig, audit.getAuditInfo(), audit.getAuditTime());
      }
      return history.getEntries();
    }
    catch (Exception e) {
      throw InternalServerError.exception(
          Throwables.getRootCause(e),
          "Could not fetch audit entries"
      );
    }
  }

  private boolean updateConfigHelper(
      UnaryOperator<DruidCompactionConfig> configUpdateOperator,
      AuditInfo auditInfo
  )
  {
    int attemps = 0;
    ConfigManager.SetResult setResult = null;
    try {
      while (attemps < MAX_UPDATE_RETRIES) {
        setResult = getAndUpdateCompactionConfig(configUpdateOperator, auditInfo);
        if (setResult.isOk() || !setResult.isRetryable()) {
          break;
        }
        attemps++;
        updateRetryDelay();
      }
    }
    catch (DruidException e) {
      throw e;
    }
    catch (Exception e) {
      log.warn(e, "Compaction config update failed");
      throw InternalServerError.exception(
          Throwables.getRootCause(e),
          "Failed to perform operation on compaction config"
      );
    }

    if (setResult.isOk()) {
      return true;
    } else if (setResult.getException() instanceof NoSuchElementException) {
      log.warn(setResult.getException(), "Update compaction config failed");
      throw NotFound.exception(
          Throwables.getRootCause(setResult.getException()),
          "Compaction config does not exist"
      );
    } else {
      log.warn(setResult.getException(), "Update compaction config failed");
      throw InternalServerError.exception(
          Throwables.getRootCause(setResult.getException()),
          "Failed to perform operation on compaction config"
      );
    }
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

}
