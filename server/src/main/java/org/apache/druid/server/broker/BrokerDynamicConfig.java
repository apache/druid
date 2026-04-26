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

package org.apache.druid.server.broker;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.QueryBlocklistRule;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Dynamic configuration for brokers that can be changed at runtime.
 * This configuration is stored in the metadata store and managed through the coordinator,
 * but is consumed by brokers.
 *
 * @see org.apache.druid.common.config.JacksonConfigManager
 */
public class BrokerDynamicConfig
{
  public static final String CONFIG_KEY = "broker.config";

  /**
   * List of query blocklist rules for blocking queries on brokers dynamically.
   */
  private final List<QueryBlocklistRule> queryBlocklist;

  /**
   * Default query context values set dynamically by operators. These override static defaults from
   * {@link org.apache.druid.query.DefaultQueryConfig} but are overridden by per-query context.
   */
  private final QueryContext queryContext;

  /**
   * Per-datasource per-segment timeout configuration. Maps datasource name to its
   * {@link PerSegmentTimeoutConfig}. When a query targets a datasource in this map,
   * the broker injects the configured {@code perSegmentTimeout} into the query context
   * (unless the caller already set it explicitly).
   */
  private final Map<String, PerSegmentTimeoutConfig> perSegmentTimeoutConfig;

  @JsonCreator
  public BrokerDynamicConfig(
      @JsonProperty("queryBlocklist") @Nullable List<QueryBlocklistRule> queryBlocklist,
      @JsonProperty("queryContext") @Nullable QueryContext queryContext,
      @JsonProperty("perSegmentTimeoutConfig") @Nullable Map<String, PerSegmentTimeoutConfig> perSegmentTimeoutConfig
  )
  {
    this.queryBlocklist = Configs.valueOrDefault(queryBlocklist, Collections.emptyList());
    this.queryContext = Configs.valueOrDefault(queryContext, QueryContext.empty());
    this.perSegmentTimeoutConfig = Configs.valueOrDefault(perSegmentTimeoutConfig, Collections.emptyMap());
  }

  @JsonProperty
  public List<QueryBlocklistRule> getQueryBlocklist()
  {
    return queryBlocklist;
  }

  @JsonProperty
  public QueryContext getQueryContext()
  {
    return queryContext;
  }

  @JsonProperty
  public Map<String, PerSegmentTimeoutConfig> getPerSegmentTimeoutConfig()
  {
    return perSegmentTimeoutConfig;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrokerDynamicConfig that = (BrokerDynamicConfig) o;
    return Objects.equals(queryBlocklist, that.queryBlocklist)
           && Objects.equals(queryContext, that.queryContext)
           && Objects.equals(perSegmentTimeoutConfig, that.perSegmentTimeoutConfig);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryBlocklist, queryContext, perSegmentTimeoutConfig);
  }

  @Override
  public String toString()
  {
    return "BrokerDynamicConfig{" +
           "queryBlocklist=" + queryBlocklist +
           ", queryContext=" + queryContext +
           ", perSegmentTimeoutConfig=" + perSegmentTimeoutConfig +
           '}';
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private List<QueryBlocklistRule> queryBlocklist;
    private QueryContext queryContext;
    private Map<String, PerSegmentTimeoutConfig> perSegmentTimeoutConfig;

    public Builder()
    {
    }

    @JsonCreator
    public Builder(
        @JsonProperty("queryBlocklist") @Nullable List<QueryBlocklistRule> queryBlocklist,
        @JsonProperty("queryContext") @Nullable QueryContext queryContext,
        @JsonProperty("perSegmentTimeoutConfig") @Nullable Map<String, PerSegmentTimeoutConfig> perSegmentTimeoutConfig
    )
    {
      this.queryBlocklist = queryBlocklist;
      this.queryContext = queryContext;
      this.perSegmentTimeoutConfig = perSegmentTimeoutConfig;
    }

    public Builder withQueryBlocklist(List<QueryBlocklistRule> queryBlocklist)
    {
      this.queryBlocklist = queryBlocklist;
      return this;
    }

    public Builder withQueryContext(QueryContext queryContext)
    {
      this.queryContext = queryContext;
      return this;
    }

    public Builder withPerSegmentTimeoutConfig(Map<String, PerSegmentTimeoutConfig> perSegmentTimeoutConfig)
    {
      this.perSegmentTimeoutConfig = perSegmentTimeoutConfig;
      return this;
    }

    public BrokerDynamicConfig build()
    {
      return new BrokerDynamicConfig(queryBlocklist, queryContext, perSegmentTimeoutConfig);
    }

    /**
     * Builds a BrokerDynamicConfig using either the configured values, or
     * the default value if not configured.
     */
    public BrokerDynamicConfig build(@Nullable BrokerDynamicConfig defaults)
    {
      return new BrokerDynamicConfig(
          Configs.valueOrDefault(queryBlocklist, defaults != null ? defaults.getQueryBlocklist() : null),
          Configs.valueOrDefault(queryContext, defaults != null ? defaults.getQueryContext() : null),
          Configs.valueOrDefault(perSegmentTimeoutConfig, defaults != null ? defaults.getPerSegmentTimeoutConfig() : null)
      );
    }
  }
}
