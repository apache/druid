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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.server.QueryBlocklistRule;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
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

  @JsonCreator
  public BrokerDynamicConfig(
      @JsonProperty("queryBlocklist") @Nullable List<QueryBlocklistRule> queryBlocklist
  )
  {
    this.queryBlocklist = Configs.valueOrDefault(queryBlocklist, Collections.emptyList());
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public List<QueryBlocklistRule> getQueryBlocklist()
  {
    return queryBlocklist;
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
    return Objects.equals(queryBlocklist, that.queryBlocklist);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryBlocklist);
  }

  @Override
  public String toString()
  {
    return "BrokerDynamicConfig{" +
           "queryBlocklist=" + queryBlocklist +
           '}';
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public static class Builder
  {
    private List<QueryBlocklistRule> queryBlocklist;

    public Builder()
    {
    }

    @JsonCreator
    public Builder(
        @JsonProperty("queryBlocklist") @Nullable List<QueryBlocklistRule> queryBlocklist
    )
    {
      this.queryBlocklist = queryBlocklist;
    }

    public Builder withQueryBlocklist(List<QueryBlocklistRule> queryBlocklist)
    {
      this.queryBlocklist = queryBlocklist;
      return this;
    }

    public BrokerDynamicConfig build()
    {
      return new BrokerDynamicConfig(queryBlocklist);
    }

    /**
     * Builds a BrokerDynamicConfig using either the configured values, or
     * the default value if not configured.
     */
    public BrokerDynamicConfig build(@Nullable BrokerDynamicConfig defaults)
    {
      return new BrokerDynamicConfig(
        Configs.valueOrDefault(queryBlocklist, defaults != null ? defaults.getQueryBlocklist() : null)
      );
    }
  }
}
