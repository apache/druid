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

package org.apache.druid.queryng.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.scan.ScanQuery;

/**
 * Configuration for the "NG" query engine.
 */
public class QueryNGConfig
{
  @SuppressWarnings("unused") // To be used later
  public static final String CONFIG_ROOT = "druid.queryng";

  /**
   * Whether the engine is enabled. It is disabled by default.
   */
  @JsonProperty("enabled")
  private boolean enabled;

  public static final String CONTEXT_VAR = "queryng";

  /**
   * Create an instance for testing.
   */
  @SuppressWarnings("unused") // To be used later
  public static QueryNGConfig create(boolean enabled)
  {
    QueryNGConfig config = new QueryNGConfig();
    config.enabled = enabled;
    return config;
  }

  public boolean enabled()
  {
    return enabled;
  }

  /**
   * Determine if Query NG should be enabled for the given query;
   * that is, if the query should have a fragment context attached.
   * At present, Query NG is enabled if the query is a scan query and
   * the query has the "queryng" context variable set. The caller
   * should already have checked if the Query NG engine is enabled
   * globally. If Query NG is enabled for a query, then the caller
   * will attach a fragment context to the query's QueryPlus.
   */
  public static boolean isEnabled(Query<?> query)
  {
    // Query has to be of the currently-supported type
    if (!(query instanceof ScanQuery)) {
      return false;
    }
    return query.getContextBoolean(CONTEXT_VAR, false);
  }

  /**
   * Determine if the Query NG (operator-based) engine is enabled for the
   * given query (given as a QueryPlus). Query NG is enabled if the QueryPlus
   * includes the fragment context needed by the Query NG engine.
   */
  public static boolean enabledFor(final QueryPlus<?> queryPlus)
  {
    return queryPlus.fragmentBuilder() != null;
  }
}
