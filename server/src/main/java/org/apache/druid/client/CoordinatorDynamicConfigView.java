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

import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.inject.Inject;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Set;

public class CoordinatorDynamicConfigView
{
  private static final Logger log = new Logger(CoordinatorDynamicConfigView.class);
  private final CoordinatorClient coordinatorClient;

  @GuardedBy("this")
  private CoordinatorDynamicConfig config;
  @GuardedBy("this")
  private Set<String> sourceCloneServers;
  @GuardedBy("this")
  private Set<String> targetCloneServers;

  @Inject
  public CoordinatorDynamicConfigView(CoordinatorClient coordinatorClient)
  {
    this.coordinatorClient = coordinatorClient;
  }

  public synchronized CoordinatorDynamicConfig getDynamicConfiguration()
  {
    return config;
  }

  public synchronized void setDynamicConfiguration(@NotNull CoordinatorDynamicConfig updatedConfig)
  {
    config = updatedConfig.snapshot();
    Map<String, String> cloneServers = config.getCloneServers();
    sourceCloneServers = ImmutableSet.copyOf(cloneServers.keySet());
    targetCloneServers = ImmutableSet.copyOf(cloneServers.values());
  }

  public synchronized Set<String> getSourceCloneServers()
  {
    return sourceCloneServers;
  }

  public synchronized Set<String> getTargetCloneServers()
  {
    return targetCloneServers;
  }

  @LifecycleStart
  public void start()
  {
    try {
      log.info("Fetching coordinator dynamic configuration.");

      CoordinatorDynamicConfig coordinatorDynamicConfig = coordinatorClient.getCoordinatorDynamicConfig().get();
      setDynamicConfiguration(coordinatorDynamicConfig);

      log.info("Successfully initialized dynamic config: [%s]", coordinatorDynamicConfig);
    }
    catch (Exception e) {
      // If the fetch fails, the broker should not serve queries. Throw the exception and try again on restart.
      log.error(e, "Failed to initialize coordinator dynamic config");
      throw new RuntimeException(e);
    }
  }
}
