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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.broker.BrokerClient;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;

/**
 * Updates all brokers with the latest coordinator dynamic config.
 */
public class CoordinatorDynamicConfigSyncer extends BaseDynamicConfigSyncer<CoordinatorDynamicConfig>
{
  private final CoordinatorConfigManager configManager;

  @Inject
  public CoordinatorDynamicConfigSyncer(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      final CoordinatorConfigManager configManager,
      @Json final ObjectMapper jsonMapper,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final ServiceEmitter emitter
  )
  {
    super(
        clientFactory,
        jsonMapper,
        druidNodeDiscoveryProvider,
        emitter,
        Execs.scheduledSingleThreaded("CoordinatorDynamicConfigSyncer-%d")
    );
    this.configManager = configManager;
  }

  @Override
  protected CoordinatorDynamicConfig getCurrentConfig()
  {
    return configManager.getCurrentDynamicConfig();
  }

  @Override
  protected boolean pushConfigToBroker(BrokerClient brokerClient, CoordinatorDynamicConfig config) throws Exception
  {
    return brokerClient.updateCoordinatorDynamicConfig(config).get();
  }

  @Override
  protected String getConfigTypeName()
  {
    return "coordinator";
  }
}
