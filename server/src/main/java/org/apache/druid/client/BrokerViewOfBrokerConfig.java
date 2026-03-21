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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import org.apache.druid.client.coordinator.Coordinator;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.CoordinatorClientImpl;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceLocator;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.server.broker.BrokerDynamicConfig;

import javax.validation.constraints.NotNull;
import java.util.Map;

/**
 * Broker view of broker dynamic configuration.
 *
 * <p>Also implements {@link QueryConfigProvider} to expose the effective default query context: the
 * merge of static defaults (from {@link DefaultQueryConfig}) and operator-supplied overrides
 * (from {@link BrokerDynamicConfig#getQueryContext()}). Dynamic values take precedence.
 */
public class BrokerViewOfBrokerConfig extends BaseBrokerViewOfConfig<BrokerDynamicConfig>
    implements QueryConfigProvider
{
  private final CoordinatorClient coordinatorClient;
  private final DefaultQueryConfig defaultQueryConfig;

  /**
   * Pre-computed merge of {@link DefaultQueryConfig#getContext()} and
   * {@link BrokerDynamicConfig#getQueryContext()}, recomputed on each config sync.
   * Dynamic config values override static defaults. {@link QueryContext} provides immutability.
   */
  private volatile QueryContext resolvedDefaultQueryContext;

  @Inject
  public BrokerViewOfBrokerConfig(
      @Json final ObjectMapper jsonMapper,
      @EscalatedGlobal final ServiceClientFactory clientFactory,
      @Coordinator final ServiceLocator serviceLocator,
      final DefaultQueryConfig defaultQueryConfig,
      final BrokerViewOfConfigsConfig startupConfig
  )
  {
    super(startupConfig);
    this.defaultQueryConfig = defaultQueryConfig;
    this.resolvedDefaultQueryContext = QueryContext.of(defaultQueryConfig.getContext());
    this.coordinatorClient =
        new CoordinatorClientImpl(
            clientFactory.makeClient(
                NodeRole.COORDINATOR.getJsonName(),
                serviceLocator,
                StandardRetryPolicy.builder().maxAttempts(15).build()
            ),
            jsonMapper
        );
  }

  @VisibleForTesting
  public BrokerViewOfBrokerConfig(
      final CoordinatorClient coordinatorClient,
      final DefaultQueryConfig defaultQueryConfig
  )
  {
    this(coordinatorClient, defaultQueryConfig, new BrokerViewOfConfigsConfig());
  }

  @VisibleForTesting
  public BrokerViewOfBrokerConfig(
      final CoordinatorClient coordinatorClient,
      final DefaultQueryConfig defaultQueryConfig,
      final BrokerViewOfConfigsConfig startupConfig
  )
  {
    super(startupConfig);
    this.coordinatorClient = coordinatorClient;
    this.defaultQueryConfig = defaultQueryConfig;
    this.resolvedDefaultQueryContext = QueryContext.of(defaultQueryConfig.getContext());
  }

  @Override
  protected BrokerDynamicConfig fetchConfigFromClient() throws Exception
  {
    return coordinatorClient.getBrokerDynamicConfig().get();
  }

  @Override
  protected String getConfigTypeName()
  {
    return "broker";
  }

  @Override
  protected BrokerDynamicConfig getDefaultConfig()
  {
    return BrokerDynamicConfig.builder().build();
  }

  /**
   * Update the config view with a new broker dynamic config snapshot, and recompute the
   * resolved default query context by merging static defaults with dynamic overrides.
   */
  @Override
  public synchronized void setDynamicConfig(@NotNull BrokerDynamicConfig updatedConfig)
  {
    super.setDynamicConfig(updatedConfig);
    resolvedDefaultQueryContext = QueryContext.of(QueryContexts.override(
        defaultQueryConfig.getContext(),
        updatedConfig.getQueryContext().asMap()
    ));
  }

  /**
   * Returns the pre-computed merge of static {@link DefaultQueryConfig} context and dynamic
   * {@link BrokerDynamicConfig#getQueryContext()}. Dynamic values take precedence over static defaults.
   */
  @Override
  public Map<String, Object> getContext()
  {
    return resolvedDefaultQueryContext.asMap();
  }
}
