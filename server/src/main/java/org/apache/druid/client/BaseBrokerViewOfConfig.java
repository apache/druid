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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;

import javax.validation.constraints.NotNull;

/**
 * Base class for broker's view of dynamic configuration fetched from the Coordinator.
 * Subclasses must implement:
 * {@link #fetchConfigFromClient()} to fetch configuration from their specific client</li>
 * {@link #getConfigTypeName()} for logging purposes</li>
 *
 * @param <DynamicConfig> the type of dynamic configuration (e.g., CoordinatorDynamicConfig, BrokerDynamicConfig)
 */
public abstract class BaseBrokerViewOfConfig<DynamicConfig>
{
  private static final Logger log = new Logger(BaseBrokerViewOfConfig.class);

  @GuardedBy("this")
  private DynamicConfig config;

  /**
   * Fetch the configuration from the Coordinator via the HTTP client.
   * This is called once during broker startup.
   *
   * @return the configuration fetched from the Coordinator
   * @throws Exception if the fetch fails
   */
  protected abstract DynamicConfig fetchConfigFromClient() throws Exception;

  /**
   * E.g., "coordinator dynamic configuration", "broker dynamic configuration"
   */
  protected abstract String getConfigTypeName();

  /**
   * Return the current dynamic configuration.
   */
  public synchronized DynamicConfig getDynamicConfig()
  {
    return config;
  }

  /**
   * Update the config view with a new dynamic config snapshot.
   * This is called when the Coordinator pushes a configuration update to this broker.
   *
   * @param updatedConfig the new configuration snapshot
   */
  public synchronized void setDynamicConfig(@NotNull DynamicConfig updatedConfig)
  {
    config = updatedConfig;
    log.info("Updated %s to [%s]", getConfigTypeName(), updatedConfig);
  }

  /**
   * Fetch the initial configuration from the Coordinator on broker startup.
   * If the fetch fails, the broker startup will fail with a RuntimeException,
   * preventing the broker from serving queries with stale or missing configuration.
   */
  @LifecycleStart
  public void start()
  {
    try {
      log.info("Fetching %s from Coordinator.", getConfigTypeName());

      DynamicConfig fetchedConfig = fetchConfigFromClient();
      setDynamicConfig(fetchedConfig);

      log.info("Successfully fetched %s: [%s]", getConfigTypeName(), fetchedConfig);
    }
    catch (Exception e) {
      // If the fetch fails, the broker should not serve queries. Throw the exception and try again on restart.
      throw new RuntimeException("Failed to initialize " + getConfigTypeName(), e);
    }
  }
}
