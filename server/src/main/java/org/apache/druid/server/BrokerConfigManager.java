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

package org.apache.druid.server;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.common.config.ConfigManager;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.logger.Logger;

/**
 * Manager to fetch and update dynamic broker configuration {@link BrokerDynamicConfig}.
 */
public class BrokerConfigManager
{
  private static final Logger log = new Logger(BrokerConfigManager.class);

  private final JacksonConfigManager jacksonConfigManager;
  private final AuditManager auditManager;

  @Inject
  public BrokerConfigManager(
      JacksonConfigManager jacksonConfigManager,
      AuditManager auditManager
  )
  {
    this.jacksonConfigManager = jacksonConfigManager;
    this.auditManager = auditManager;
  }

  public BrokerDynamicConfig getCurrentDynamicConfig()
  {
    BrokerDynamicConfig dynamicConfig = jacksonConfigManager.watch(
        BrokerDynamicConfig.CONFIG_KEY,
        BrokerDynamicConfig.class,
        BrokerDynamicConfig.builder().build()
    ).get();

    return Preconditions.checkNotNull(dynamicConfig, "Got null config from watcher?!");
  }

  public ConfigManager.SetResult setDynamicConfig(BrokerDynamicConfig config, AuditInfo auditInfo)
  {
    return jacksonConfigManager.set(
        BrokerDynamicConfig.CONFIG_KEY,
        config,
        auditInfo
    );
  }
}
