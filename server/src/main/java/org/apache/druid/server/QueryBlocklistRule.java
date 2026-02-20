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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Sets;
import org.apache.druid.query.Query;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A rule for matching queries against blocklist criteria. A query matches this rule if ALL
 * specified criteria match (AND logic). Null or empty criteria match everything.
 */
public class QueryBlocklistRule
{
  private final String ruleName;
  @Nullable
  private final Set<String> dataSources;
  @Nullable
  private final Set<String> queryTypes;
  @Nullable
  private final Map<String, String> contextMatches;

  private final boolean hasDataSourceCriteria;
  private final boolean hasQueryTypeCriteria;
  private final boolean hasContextCriteria;

  @JsonCreator
  public QueryBlocklistRule(
      @JsonProperty("ruleName") String ruleName,
      @JsonProperty("dataSources") @Nullable Set<String> dataSources,
      @JsonProperty("queryTypes") @Nullable Set<String> queryTypes,
      @JsonProperty("contextMatches") @Nullable Map<String, String> contextMatches
  )
  {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(ruleName),
        "ruleName must not be null or empty"
    );

    // At least one criterion must be specified to prevent accidentally blocking all queries
    this.hasDataSourceCriteria = dataSources != null && !dataSources.isEmpty();
    this.hasQueryTypeCriteria = queryTypes != null && !queryTypes.isEmpty();
    this.hasContextCriteria = contextMatches != null && !contextMatches.isEmpty();

    Preconditions.checkArgument(
        hasDataSourceCriteria || hasQueryTypeCriteria || hasContextCriteria,
        "At least one criterion (dataSources, queryTypes, or contextMatches) must be specified. "
        + "A rule with all null/empty criteria would block ALL queries."
    );

    this.ruleName = ruleName;
    this.dataSources = dataSources;
    this.queryTypes = queryTypes;
    this.contextMatches = contextMatches;
  }

  @JsonProperty
  public String getRuleName()
  {
    return ruleName;
  }

  @JsonProperty
  @Nullable
  public Set<String> getDataSources()
  {
    return dataSources;
  }

  @JsonProperty
  @Nullable
  public Set<String> getQueryTypes()
  {
    return queryTypes;
  }

  @JsonProperty
  @Nullable
  public Map<String, String> getContextMatches()
  {
    return contextMatches;
  }

  /**
   * Returns true if the query matches ALL specified criteria (AND logic).
   * Null or empty criteria match everything.
   *
   * @param query the query to check
   * @return true if the query matches this rule, false otherwise
   */
  public boolean matches(Query<?> query)
  {
    if (hasDataSourceCriteria) {
      Set<String> queryDatasources = query.getDataSource().getTableNames();
      if (Sets.intersection(dataSources, queryDatasources).isEmpty()) {
        return false;
      }
    }

    if (hasQueryTypeCriteria) {
      if (!queryTypes.contains(query.getType())) {
        return false;
      }
    }

    if (hasContextCriteria) {
      for (Map.Entry<String, String> entry : contextMatches.entrySet()) {
        Object contextValue = query.getContext().get(entry.getKey());
        // If the query context doesn't have this key or has a null value, it doesn't match
        if (contextValue == null || !entry.getValue().equals(String.valueOf(contextValue))) {
          return false;
        }
      }
    }

    return true;
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
    QueryBlocklistRule that = (QueryBlocklistRule) o;
    return Objects.equals(ruleName, that.ruleName)
           && Objects.equals(dataSources, that.dataSources)
           && Objects.equals(queryTypes, that.queryTypes)
           && Objects.equals(contextMatches, that.contextMatches);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(ruleName, dataSources, queryTypes, contextMatches);
  }

  @Override
  public String toString()
  {
    return "QueryBlocklistRule{" +
           "ruleName='" + ruleName + '\'' +
           ", dataSources=" + dataSources +
           ", queryTypes=" + queryTypes +
           ", contextMatches=" + contextMatches +
           '}';
  }
}
