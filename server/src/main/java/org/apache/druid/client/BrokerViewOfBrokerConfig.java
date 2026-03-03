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

package org.apache.druid.client;

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.broker.BrokerDynamicConfig;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Broker view of broker dynamic configuration.
 */
public class BrokerViewOfBrokerConfig
{
  private static final Logger log = new Logger(BrokerViewOfBrokerConfig.class);
  private final AtomicReference<BrokerDynamicConfig> config;

  @Inject
  public BrokerViewOfBrokerConfig(JacksonConfigManager configManager)
  {
    this.config = configManager.watch(
        BrokerDynamicConfig.CONFIG_KEY,
        BrokerDynamicConfig.class,
        new BrokerDynamicConfig(null)
    );
  }

  @VisibleForTesting
  public BrokerViewOfBrokerConfig(BrokerDynamicConfig initialConfig)
  {
    this.config = new AtomicReference<>(initialConfig);
  }

  public BrokerDynamicConfig getDynamicConfig()
  {
    return config.get();
  }

  /**
   * Update the config view with a new broker dynamic config snapshot.
   */
  public void setDynamicConfig(BrokerDynamicConfig updatedConfig)
  {
    config.set(updatedConfig);
    log.info("Broker dynamic config updated to [%s]", updatedConfig);
  }

  @LifecycleStart
  public void start()
  {
    log.info("Broker dynamic config initialized[%s].", config.get());
  }
}
