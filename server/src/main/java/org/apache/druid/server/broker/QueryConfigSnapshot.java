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

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.server.QueryBlocklistRule;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A snapshot of the broker's resolved default query context and dynamic config, captured once for a single query so
 * that context resolution and the blocklist read one consistent view (rather than re-reading the live config at
 * different points in the query lifecycle). The context-resolution logic lives here, keeping it out of
 * {@code QueryLifecycle}. On non-broker nodes {@code dynamicConfig} is null (defaults only, no per-query overrides).
 */
public class QueryConfigSnapshot
{
  private final Map<String, Object> defaultContext;
  @Nullable
  private final BrokerDynamicConfig dynamicConfig;

  public QueryConfigSnapshot(Map<String, Object> defaultContext, @Nullable BrokerDynamicConfig dynamicConfig)
  {
    this.defaultContext = defaultContext;
    this.dynamicConfig = dynamicConfig;
  }

  /**
   * The final query context. Precedence high to low: a key the client set > per-query dynamic override > the query's
   * own context > defaults. Dynamic overrides are applied over the merged context but skipped for keys the client set,
   * so a client-provided value always wins.
   */
  public Map<String, Object> resolveContext(Query<?> query, Set<String> clientProvidedQueryContextKeys)
  {
    Map<String, Object> result = QueryContexts.override(defaultContext, query.getContext());
    if (dynamicConfig != null) {
      for (Map.Entry<String, Object> override : dynamicConfig.getQuerySpecificContextOverrides(query).asMap().entrySet()) {
        if (!clientProvidedQueryContextKeys.contains(override.getKey())) {
          result.put(override.getKey(), override.getValue());
        }
      }
    }
    return result;
  }

  public List<QueryBlocklistRule> getQueryBlocklist()
  {
    return dynamicConfig == null ? Collections.emptyList() : dynamicConfig.getQueryBlocklist();
  }
}
