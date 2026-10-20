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

import com.google.inject.Inject;
import jakarta.validation.constraints.NotNull;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.DefaultQueryConfig;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextConfig;
import org.apache.druid.query.context.QueryContextParameterValidator;
import org.apache.druid.server.broker.BrokerDynamicConfig;
import org.apache.druid.server.broker.QueryConfigSnapshot;

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
  private static final String DEFAULT_QUERY_CONTEXT_PROPERTY = "druid.query.default.context";

  private final CoordinatorClient coordinatorClient;
  private final DefaultQueryConfig defaultQueryConfig;

  /**
   * The dynamic config plus the merge of {@link DefaultQueryConfig#getContext()} and
   * {@link BrokerDynamicConfig#getQueryContext()}, recomputed on each config sync.
   *
   * <p>volatile, not synchronized: read on the query hot path, see {@link BaseBrokerViewOfConfig}. Both halves
   * live in one field so a query cannot observe them from different generations.
   */
  private volatile QueryConfigSnapshot querySnapshot;

  @Inject
  public BrokerViewOfBrokerConfig(
      final CoordinatorClient coordinatorClient,
      final DefaultQueryConfig defaultQueryConfig,
      final QueryContextConfig queryContextConfig
  )
  {
    validateDefaultQueryConfig(defaultQueryConfig, queryContextConfig);
    this.coordinatorClient = coordinatorClient;
    this.defaultQueryConfig = defaultQueryConfig;
    this.querySnapshot = new QueryConfigSnapshot(QueryContext.of(defaultQueryConfig.getContext()).asMap(), null);
  }

  private static void validateDefaultQueryConfig(
      final DefaultQueryConfig defaultQueryConfig,
      final QueryContextConfig queryContextConfig
  )
  {
    try {
      QueryContextParameterValidator.validate(
          defaultQueryConfig.getContext(),
          queryContextConfig.isRejectUnknownParameters()
      );
    }
    catch (BadQueryContextException e) {
      throw new IAE(e, "Invalid default query context [%s]: %s", DEFAULT_QUERY_CONTEXT_PROPERTY, e.getMessage());
    }
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

  /**
   * Update the config view with a new broker dynamic config snapshot, and recompute the
   * resolved default query context by merging static defaults with dynamic overrides.
   */
  @Override
  public void setDynamicConfig(@NotNull BrokerDynamicConfig updatedConfig)
  {
    super.setDynamicConfig(updatedConfig);
    querySnapshot = new QueryConfigSnapshot(
        QueryContext.of(defaultQueryConfig.getContext()).override(updatedConfig.getQueryContext()).asMap(),
        updatedConfig
    );
  }

  /**
   * Returns the pre-computed merge of static {@link DefaultQueryConfig} context and dynamic
   * {@link BrokerDynamicConfig#getQueryContext()}. Dynamic values take precedence over static defaults.
   */
  @Override
  public Map<String, Object> getContext()
  {
    return querySnapshot.getResolvedDefaultQueryContext();
  }

  /**
   * Returns the resolved context and the {@link BrokerDynamicConfig#getQueryContext()} of one snapshot. The overrides
   * are empty before the first config sync.
   */
  @Override
  public ContextWithOverrides getContextWithOverrides()
  {
    final QueryConfigSnapshot snapshot = querySnapshot;
    final BrokerDynamicConfig dynamicConfig = snapshot.getDynamicConfig();
    return new ContextWithOverrides(
        snapshot.getResolvedDefaultQueryContext(),
        dynamicConfig == null ? Map.of() : dynamicConfig.getQueryContext().asMap()
    );
  }

  /**
   * Snapshot for a single query to resolve its context and blocklist against, instead of re-reading the live config.
   */
  public QueryConfigSnapshot snapshotForQuery()
  {
    return querySnapshot;
  }

  /**
   * Reads through {@link #querySnapshot} so this and {@link #snapshotForQuery()} always agree.
   */
  @Override
  public BrokerDynamicConfig getDynamicConfig()
  {
    return querySnapshot.getDynamicConfig();
  }
}
