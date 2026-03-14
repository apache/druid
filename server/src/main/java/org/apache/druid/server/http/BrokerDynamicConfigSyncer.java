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
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.server.broker.BrokerDynamicConfig;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Syncs broker dynamic configuration to all brokers.
 */
public class BrokerDynamicConfigSyncer extends BaseDynamicConfigSyncer<BrokerDynamicConfig>
{
  private final AtomicReference<BrokerDynamicConfig> currentConfig;

  @Inject
  public BrokerDynamicConfigSyncer(
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      final JacksonConfigManager configManager,
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
        Execs.scheduledSingleThreaded("BrokerDynamicConfigSyncer-%d")
    );
    this.currentConfig = configManager.watch(
        BrokerDynamicConfig.CONFIG_KEY,
        BrokerDynamicConfig.class,
        BrokerDynamicConfig.builder().build()
    );
  }

  @Override
  protected BrokerDynamicConfig getCurrentConfig()
  {
    return currentConfig.get();
  }

  @Override
  protected boolean pushConfigToBroker(BrokerClient brokerClient, BrokerDynamicConfig config) throws Exception
  {
    return brokerClient.updateBrokerDynamicConfig(config).get();
  }

  @Override
  protected String getConfigTypeName()
  {
    return "broker";
  }
}
