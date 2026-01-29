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

package org.apache.druid.consul.discovery;

import com.ecwid.consul.v1.ConsulClient;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.Provider;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.PolyBind;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.DruidNode;

import java.util.Collections;
import java.util.List;

/**
 * Guice module for Consul-based service discovery and leader election.
 * <p>
 * To enable Consul discovery, set {@code druid.discovery.type=consul} and configure
 * the extension under {@code druid.discovery.consul.*}. See the extension documentation
 * for the full list of configuration options.
 */
public class ConsulDiscoveryModule implements DruidModule
{
  private static final String CONSUL_KEY = "consul";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.emptyList();
  }

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.discovery.consul", ConsulDiscoveryConfig.class);

    // Bind ConsulClient first since ConsulApiClient depends on it
    binder.bind(ConsulClient.class)
          .toProvider(ConsulClientProvider.class)
          .in(LazySingleton.class);

    binder.bind(ConsulApiClient.class)
          .toProvider(ConsulApiClientProvider.class)
          .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidNodeDiscoveryProvider.class))
            .addBinding(CONSUL_KEY)
            .to(ConsulDruidNodeDiscoveryProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidNodeAnnouncer.class))
            .addBinding(CONSUL_KEY)
            .to(ConsulDruidNodeAnnouncer.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, Coordinator.class))
            .addBinding(CONSUL_KEY)
            .toProvider(CoordinatorLeaderSelectorProvider.class)
            .in(LazySingleton.class);

    PolyBind.optionBinder(binder, Key.get(DruidLeaderSelector.class, IndexingService.class))
            .addBinding(CONSUL_KEY)
            .toProvider(OverlordLeaderSelectorProvider.class)
            .in(LazySingleton.class);
  }

  private static class ConsulApiClientProvider implements Provider<ConsulApiClient>
  {
    private final ConsulClient consulClient;
    private final ConsulDiscoveryConfig config;
    private final ObjectMapper jsonMapper;

    @Inject
    ConsulApiClientProvider(
        ConsulClient consulClient,
        ConsulDiscoveryConfig config,
        @Json ObjectMapper jsonMapper
    )
    {
      this.consulClient = consulClient;
      this.config = config;
      this.jsonMapper = jsonMapper;
    }

    @Override
    public ConsulApiClient get()
    {
      return new DefaultConsulApiClient(consulClient, config, jsonMapper);
    }
  }

  private static class ConsulClientProvider implements Provider<ConsulClient>
  {
    private final ConsulDiscoveryConfig config;

    @Inject
    ConsulClientProvider(ConsulDiscoveryConfig config)
    {
      this.config = config;
    }

    @Override
    public ConsulClient get()
    {
      return ConsulClients.create(config);
    }
  }

  private static class CoordinatorLeaderSelectorProvider implements Provider<DruidLeaderSelector>
  {
    private final DruidNode self;
    private final ConsulDiscoveryConfig config;
    private final ConsulClient consulClient;

    @Inject
    CoordinatorLeaderSelectorProvider(
        @Self DruidNode self,
        ConsulDiscoveryConfig config,
        ConsulClient consulClient
    )
    {
      this.self = self;
      this.config = config;
      this.consulClient = consulClient;
    }

    @Override
    public DruidLeaderSelector get()
    {
      return new ConsulLeaderSelector(
          self,
          config.getLeader().getCoordinatorLeaderLockPath(),
          config,
          consulClient
      );
    }
  }

  private static class OverlordLeaderSelectorProvider implements Provider<DruidLeaderSelector>
  {
    private final DruidNode self;
    private final ConsulDiscoveryConfig config;
    private final ConsulClient consulClient;

    @Inject
    OverlordLeaderSelectorProvider(
        @Self DruidNode self,
        ConsulDiscoveryConfig config,
        ConsulClient consulClient
    )
    {
      this.self = self;
      this.config = config;
      this.consulClient = consulClient;
    }

    @Override
    public DruidLeaderSelector get()
    {
      return new ConsulLeaderSelector(
          self,
          config.getLeader().getOverlordLeaderLockPath(),
          config,
          consulClient
      );
    }
  }
}
