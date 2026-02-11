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

package org.apache.druid.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.error.InvalidInput;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Dynamic configuration for brokers. This configuration is persisted in the metadata store
 * and can be updated at runtime without restarting brokers.
 *
 * @see JacksonConfigManager
 * @see org.apache.druid.common.config.ConfigManager
 */
public class BrokerDynamicConfig
{
  public static final String CONFIG_KEY = "broker.config";

  private final List<QueryBlocklistRule> queryBlocklist;

  @JsonCreator
  public BrokerDynamicConfig(
      @JsonProperty("queryBlocklist") @Nullable List<QueryBlocklistRule> queryBlocklist
  )
  {
    if (queryBlocklist != null) {
      Set<String> ruleNames = new HashSet<>();
      for (QueryBlocklistRule rule : queryBlocklist) {
        if (rule == null) {
          throw InvalidInput.exception("Query blocklist contains null rule");
        }
        if (Strings.isNullOrEmpty(rule.getRuleName())) {
          throw InvalidInput.exception("Query blocklist rule must have a non-empty name");
        }
        if (!ruleNames.add(rule.getRuleName())) {
          throw InvalidInput.exception(
              "Query blocklist contains duplicate rule name: %s",
              rule.getRuleName()
          );
        }
      }
      this.queryBlocklist = ImmutableList.copyOf(queryBlocklist);
    } else {
      this.queryBlocklist = Defaults.QUERY_BLOCKLIST;
    }
  }

  @JsonProperty
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

  private static class Defaults
  {
    static final List<QueryBlocklistRule> QUERY_BLOCKLIST = ImmutableList.of();
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
      return new BrokerDynamicConfig(
          queryBlocklist != null ? queryBlocklist : Defaults.QUERY_BLOCKLIST
      );
    }

    public BrokerDynamicConfig build(BrokerDynamicConfig defaults)
    {
      return new BrokerDynamicConfig(
          queryBlocklist != null ? queryBlocklist : defaults.getQueryBlocklist()
      );
    }
  }
}
