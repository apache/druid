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
import org.apache.druid.common.config.ConfigEtag;
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

  public CoordinatorDynamicConfig convertBytesToDynamicConfig(@Nullable byte[] bytes)
  {
    return jacksonConfigManager.convertByteToConfig(
        bytes,
        CoordinatorDynamicConfig.class,
        CoordinatorDynamicConfig.builder().build()
    );
  }

  public ConfigManager.SetResult updateDynamicConfig(
      UnaryOperator<CoordinatorDynamicConfig> operator,
      @Nullable String ifMatchEtag,
      AuditInfo auditInfo
  )
  {
    return jacksonConfigManager.setIfMatch(
        CoordinatorDynamicConfig.CONFIG_KEY,
        ifMatchEtag,
        CoordinatorDynamicConfig.class,
        CoordinatorDynamicConfig.builder().build(),
        operator,
        auditInfo
    );
  }

  @Nullable
  public byte[] getCurrentDynamicConfigBytes()
  {
    return jacksonConfigManager.getCurrentBytes(CoordinatorDynamicConfig.CONFIG_KEY);
  }

  @Nullable
  public byte[] getCurrentCompactionConfigBytes()
  {
    return jacksonConfigManager.getCurrentBytes(DruidCompactionConfig.CONFIG_KEY);
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
    return getAndUpdateCompactionConfig(operator, null, auditInfo);
  }

  public ConfigManager.SetResult getAndUpdateCompactionConfig(
      UnaryOperator<DruidCompactionConfig> operator,
      @Nullable String ifMatchEtag,
      AuditInfo auditInfo
  )
  {
    if (ifMatchEtag != null && !jacksonConfigManager.isCompareAndSwapEnabled()) {
      return ConfigManager.SetResult.preconditionFailed(
          new IllegalStateException(
              "If-Match requires druid.manager.config.enableCompareAndSwap to be enabled for key["
              + DruidCompactionConfig.CONFIG_KEY
              + "]"
          )
      );
    }
    // Fetch the bytes and use to build the current config and perform compare-and-swap.
    // This avoids failures in ConfigManager while updating configs previously
    // persisted by older versions of Druid which didn't have fields such as 'granularitySpec'.
    final byte[] currentBytes = metadataStorageConnector.lookup(
        tablesConfig.getConfigTable(),
        MetadataStorageConnector.CONFIG_TABLE_KEY_COLUMN,
        MetadataStorageConnector.CONFIG_TABLE_VALUE_COLUMN,
        DruidCompactionConfig.CONFIG_KEY
    );
    if (ifMatchEtag != null && !ConfigEtag.matches(ifMatchEtag, currentBytes)) {
      return ConfigManager.SetResult.preconditionFailed(
          new IllegalStateException("If-Match precondition failed for compaction config")
      );
    }
    DruidCompactionConfig current = convertBytesToCompactionConfig(currentBytes);
    DruidCompactionConfig updated = operator.apply(current);

    if (current.equals(updated)) {
      return ConfigManager.SetResult.ok();
    }
    final ConfigManager.SetResult result = jacksonConfigManager.set(
        DruidCompactionConfig.CONFIG_KEY,
        currentBytes,
        updated,
        auditInfo
    );
    // Under If-Match, a lost CAS means a concurrent writer committed between our
    // read and write: the precondition no longer holds, so report it as such
    // rather than as a retryable failure (conditional writes must not auto-retry).
    if (ifMatchEtag != null && result.isRetryable()) {
      return ConfigManager.SetResult.preconditionFailed(
          new IllegalStateException("If-Match precondition failed (concurrent update) for compaction config")
      );
    }
    return result;
  }

  public DruidCompactionConfig convertBytesToCompactionConfig(@Nullable byte[] bytes)
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
    return updateCompactionTaskSlots(compactionTaskSlotRatio, maxCompactionTaskSlots, null, auditInfo);
  }

  public boolean updateCompactionTaskSlots(
      @Nullable Double compactionTaskSlotRatio,
      @Nullable Integer maxCompactionTaskSlots,
      @Nullable String ifMatchEtag,
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
          currentClusterConfig.getEngine(),
          currentClusterConfig.isStoreCompactionStatePerSegment()
      );

      return current.withClusterConfig(updatedClusterConfig);
    };

    return updateConfigHelper(operator, ifMatchEtag, auditInfo);
  }

  public boolean updateClusterCompactionConfig(
      ClusterCompactionConfig config,
      @Nullable String ifMatchEtag,
      AuditInfo auditInfo
  )
  {
    UnaryOperator<DruidCompactionConfig> operator = current -> current.withClusterConfig(config);
    return updateConfigHelper(operator, ifMatchEtag, auditInfo);
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
    return updateDatasourceCompactionConfig(config, null, auditInfo);
  }

  public boolean updateDatasourceCompactionConfig(
      DataSourceCompactionConfig config,
      @Nullable String ifMatchEtag,
      AuditInfo auditInfo
  )
  {
    UnaryOperator<DruidCompactionConfig> callable = current -> current.withDatasourceConfig(config);
    return updateConfigHelper(callable, ifMatchEtag, auditInfo);
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
    return deleteDatasourceCompactionConfig(dataSource, null, auditInfo);
  }

  public boolean deleteDatasourceCompactionConfig(
      String dataSource,
      @Nullable String ifMatchEtag,
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
    return updateConfigHelper(callable, ifMatchEtag, auditInfo);
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
      @Nullable String ifMatchEtag,
      AuditInfo auditInfo
  )
  {
    int attempts = 0;
    ConfigManager.SetResult setResult = null;
    // Only an unconditional write can be retried: getAndUpdateCompactionConfig converts a lost
    // CAS under If-Match into a (non-retryable) precondition failure, so conditional writes exit
    // this loop on the first attempt rather than silently retrying past the caller's precondition.
    try {
      while (attempts < MAX_UPDATE_RETRIES) {
        setResult = getAndUpdateCompactionConfig(configUpdateOperator, ifMatchEtag, auditInfo);
        if (setResult.isOk() || !setResult.isRetryable()) {
          break;
        }
        attempts++;
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
    }

    // getAndUpdateCompactionConfig already converts a lost CAS under If-Match into
    // a precondition failure, so a retryable result here is only a genuine
    // (non-conditional) CAS conflict that exhausted its retries.
    if (setResult.isPreconditionFailed()) {
      log.info("If-Match precondition failed on compaction config update");
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.PRECONDITION_FAILED)
                          .build(setResult.getException().getMessage());
    }

    if (setResult.getException() instanceof NoSuchElementException) {
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
