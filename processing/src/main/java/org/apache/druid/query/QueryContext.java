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

package org.apache.druid.query;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Holder for query context parameters. There are 3 ways to set context params today.
 *
 * - Default parameters. These are set mostly via {@link DefaultQueryConfig#context}.
 *   Auto-generated queryId or sqlQueryId are also set as default parameters. These default parameters can
 *   be overridden by user or system parameters.
 * - User parameters. These are the params set by the user. User params override default parameters but
 *   are overridden by system parameters.
 * - System parameters. These are the params set by the Druid query engine for internal use only.
 *
 * You can use {@code getX} methods or {@link #getMergedParams()} to compute the context params
 * merging 3 types of params above.
 *
 * Currently, this class is mainly used for query context parameter authorization,
 * such as HTTP query endpoints or JDBC endpoint. Its usage can be expanded in the future if we
 * want to track user parameters and separate them from others during query processing.
 */
public class QueryContext
{
  private final Map<String, Object> defaultParams;
  private final Map<String, Object> userParams;
  private final Map<String, Object> systemParams;

  /**
   * Cache of params merged.
   */
  @Nullable
  private Map<String, Object> mergedParams;

  public QueryContext()
  {
    this(null);
  }

  public QueryContext(@Nullable Map<String, Object> userParams)
  {
    this.defaultParams = new TreeMap<>();
    this.userParams = userParams == null ? new TreeMap<>() : new TreeMap<>(userParams);
    this.systemParams = new TreeMap<>();
    invalidateMergedParams();
  }

  private void invalidateMergedParams()
  {
    this.mergedParams = null;
  }

  public boolean isEmpty()
  {
    return defaultParams.isEmpty() && userParams.isEmpty() && systemParams.isEmpty();
  }

  public void addDefaultParam(String key, Object val)
  {
    invalidateMergedParams();
    defaultParams.put(key, val);
  }

  public void addDefaultParams(Map<String, Object> defaultParams)
  {
    invalidateMergedParams();
    this.defaultParams.putAll(defaultParams);
  }

  public void addSystemParam(String key, Object val)
  {
    invalidateMergedParams();
    this.systemParams.put(key, val);
  }

  public Object removeUserParam(String key)
  {
    invalidateMergedParams();
    return userParams.remove(key);
  }

  /**
   * Returns only the context parameters the user sets.
   * The returned map does not include the parameters that have been removed via {@link #removeUserParam}.
   *
   * Callers should use {@code getX} methods or {@link #getMergedParams()} instead to use the whole context params.
   */
  public Map<String, Object> getUserParams()
  {
    return userParams;
  }

  public boolean isDebug()
  {
    return getAsBoolean(QueryContexts.ENABLE_DEBUG, QueryContexts.DEFAULT_ENABLE_DEBUG);
  }

  public boolean isEnableJoinLeftScanDirect()
  {
    return getAsBoolean(
        QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT,
        QueryContexts.DEFAULT_ENABLE_SQL_JOIN_LEFT_SCAN_DIRECT
    );
  }
  @SuppressWarnings("unused")
  public boolean containsKey(String key)
  {
    return get(key) != null;
  }

  @Nullable
  public Object get(String key)
  {
    Object val = systemParams.get(key);
    if (val != null) {
      return val;
    }
    val = userParams.get(key);
    return val == null ? defaultParams.get(key) : val;
  }

  @SuppressWarnings("unused")
  public Object getOrDefault(String key, Object defaultValue)
  {
    final Object val = get(key);
    return val == null ? defaultValue : val;
  }

  @Nullable
  public String getAsString(String key)
  {
    return (String) get(key);
  }

  public boolean getAsBoolean(
      final String parameter,
      final boolean defaultValue
  )
  {
    return QueryContexts.getAsBoolean(parameter, get(parameter), defaultValue);
  }

  public int getAsInt(
      final String parameter,
      final int defaultValue
  )
  {
    return QueryContexts.getAsInt(parameter, get(parameter), defaultValue);
  }

  public long getAsLong(final String parameter, final long defaultValue)
  {
    return QueryContexts.getAsLong(parameter, get(parameter), defaultValue);
  }

  public Map<String, Object> getMergedParams()
  {
    if (mergedParams == null) {
      final Map<String, Object> merged = new TreeMap<>(defaultParams);
      merged.putAll(userParams);
      merged.putAll(systemParams);
      mergedParams = Collections.unmodifiableMap(merged);
    }
    return mergedParams;
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
    QueryContext context = (QueryContext) o;
    return getMergedParams().equals(context.getMergedParams());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getMergedParams());
  }

  @Override
  public String toString()
  {
    return "QueryContext{" +
           "defaultParams=" + defaultParams +
           ", userParams=" + userParams +
           ", systemParams=" + systemParams +
           '}';
  }
}
