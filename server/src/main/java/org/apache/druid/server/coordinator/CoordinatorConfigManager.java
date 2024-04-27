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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.metadata.MetadataStorageConnector;
import org.apache.druid.metadata.MetadataStorageTablesConfig;

import java.util.function.UnaryOperator;

/**
 * Manager to fetch and update dynamic configs {@link CoordinatorDynamicConfig}
 * and {@link CoordinatorCompactionConfig}.
 */
public class CoordinatorConfigManager
{
  private final JacksonConfigManager jacksonConfigManager;
  private final MetadataStorageConnector metadataStorageConnector;
  private final MetadataStorageTablesConfig tablesConfig;

  @Inject
  public CoordinatorConfigManager(
      JacksonConfigManager jacksonConfigManager,
      MetadataStorageConnector metadataStorageConnector,
      MetadataStorageTablesConfig tablesConfig
  )
  {
    this.jacksonConfigManager = jacksonConfigManager;
    this.metadataStorageConnector = metadataStorageConnector;
    this.tablesConfig = tablesConfig;
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

  public CoordinatorCompactionConfig getCurrentCompactionConfig()
  {
    CoordinatorCompactionConfig config = jacksonConfigManager.watch(
        CoordinatorCompactionConfig.CONFIG_KEY,
        CoordinatorCompactionConfig.class,
        CoordinatorCompactionConfig.empty()
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
      UnaryOperator<CoordinatorCompactionConfig> operator,
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
        CoordinatorCompactionConfig.CONFIG_KEY
    );
    CoordinatorCompactionConfig current = convertBytesToCompactionConfig(currentBytes);
    CoordinatorCompactionConfig updated = operator.apply(current);

    if (current.equals(updated)) {
      return ConfigManager.SetResult.ok();
    } else {
      return jacksonConfigManager.set(
          CoordinatorCompactionConfig.CONFIG_KEY,
          currentBytes,
          updated,
          auditInfo
      );
    }
  }

  public CoordinatorCompactionConfig convertBytesToCompactionConfig(byte[] bytes)
  {
    return jacksonConfigManager.convertByteToConfig(
        bytes,
        CoordinatorCompactionConfig.class,
        CoordinatorCompactionConfig.empty()
    );
  }
}
