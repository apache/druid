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
 * A snapshot of the {@link BrokerDynamicConfig} (null on non-Broker nodes)
 * and the resolved default query context that is used for the entire {@code QueryLifecycle}
 * of a single query.
 */
public class QueryConfigSnapshot
{
  /** Already resolved against {@link BrokerDynamicConfig#getQueryContext()}. */
  private final Map<String, Object> resolvedDefaultQueryContext;
  @Nullable
  private final BrokerDynamicConfig dynamicConfig;

  public QueryConfigSnapshot(
      Map<String, Object> resolvedDefaultQueryContext,
      @Nullable BrokerDynamicConfig dynamicConfig
  )
  {
    this.resolvedDefaultQueryContext = resolvedDefaultQueryContext;
    this.dynamicConfig = dynamicConfig;
  }

  public Map<String, Object> getResolvedDefaultQueryContext()
  {
    return resolvedDefaultQueryContext;
  }

  @Nullable
  public BrokerDynamicConfig getDynamicConfig()
  {
    return dynamicConfig;
  }

  /**
   * The final query context for the given query. Precedence, highest to lowest:
   * <ol>
   *   <li>Keys the client set on the query payload</li>
   *   <li>Per-query overrides from {@link BrokerDynamicConfig#getContextOverridesForQuery}</li>
   *   <li>Remaining keys on the query context (defaults merged in by the SQL layer)</li>
   *   <li>{@link #resolvedDefaultQueryContext}, i.e. runtime properties overridden by
   *       {@link BrokerDynamicConfig#getQueryContext()}</li>
   * </ol>
   */
  public Map<String, Object> resolveContext(Query<?> query, Set<String> clientProvidedQueryContextKeys)
  {
    final Map<String, Object> result = QueryContexts.override(resolvedDefaultQueryContext, query.getContext());
    if (dynamicConfig != null) {
      for (Map.Entry<String, Object> override : dynamicConfig.getContextOverridesForQuery(query).asMap().entrySet()) {
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
