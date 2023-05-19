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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.QueryContexts;
import org.joda.time.DateTimeZone;

import java.util.Map;
import java.util.Objects;

public class PlannerConfig
{
  public static final String CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT = "useApproximateCountDistinct";
  public static final String CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT = "useGroupingSetForExactDistinct";
  public static final String CTX_KEY_USE_APPROXIMATE_TOPN = "useApproximateTopN";
  public static final String CTX_COMPUTE_INNER_JOIN_COST_AS_FILTER = "computeInnerJoinCostAsFilter";
  public static final String CTX_KEY_USE_NATIVE_QUERY_EXPLAIN = "useNativeQueryExplain";
  public static final String CTX_KEY_FORCE_EXPRESSION_VIRTUAL_COLUMNS = "forceExpressionVirtualColumns";
  public static final String CTX_MAX_NUMERIC_IN_FILTERS = "maxNumericInFilters";
  public static final int NUM_FILTER_NOT_USED = -1;

  @JsonProperty
  private int maxTopNLimit = 100_000;

  @JsonProperty
  private boolean useApproximateCountDistinct = true;

  @JsonProperty
  private boolean useApproximateTopN = true;

  @JsonProperty
  private boolean requireTimeCondition = false;

  @JsonProperty
  private DateTimeZone sqlTimeZone = DateTimeZone.UTC;

  @JsonProperty
  private boolean useGroupingSetForExactDistinct = false;

  @JsonProperty
  private boolean computeInnerJoinCostAsFilter = true;

  @JsonProperty
  private boolean authorizeSystemTablesDirectly = false;

  @JsonProperty
  private boolean useNativeQueryExplain = true;

  @JsonProperty
  private boolean forceExpressionVirtualColumns = false;

  @JsonProperty
  private int maxNumericInFilters = NUM_FILTER_NOT_USED;

  private boolean serializeComplexValues = true;

  public int getMaxNumericInFilters()
  {
    return maxNumericInFilters;
  }

  public int getMaxTopNLimit()
  {
    return maxTopNLimit;
  }

  public boolean isUseApproximateCountDistinct()
  {
    return useApproximateCountDistinct;
  }

  public boolean isUseGroupingSetForExactDistinct()
  {
    return useGroupingSetForExactDistinct;
  }

  public boolean isUseApproximateTopN()
  {
    return useApproximateTopN;
  }

  public boolean isRequireTimeCondition()
  {
    return requireTimeCondition;
  }

  public DateTimeZone getSqlTimeZone()
  {
    return sqlTimeZone;
  }

  public boolean shouldSerializeComplexValues()
  {
    return serializeComplexValues;
  }

  public boolean isComputeInnerJoinCostAsFilter()
  {
    return computeInnerJoinCostAsFilter;
  }

  public boolean isAuthorizeSystemTablesDirectly()
  {
    return authorizeSystemTablesDirectly;
  }

  public boolean isUseNativeQueryExplain()
  {
    return useNativeQueryExplain;
  }

  /**
   * @return true if special virtual columns should not be optimized and should
   * always be of type "expressions", false otherwise.
   */
  public boolean isForceExpressionVirtualColumns()
  {
    return forceExpressionVirtualColumns;
  }

  public PlannerConfig withOverrides(final Map<String, Object> queryContext)
  {
    if (queryContext.isEmpty()) {
      return this;
    }
    return toBuilder()
        .withOverrides(queryContext)
        .build();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final PlannerConfig that = (PlannerConfig) o;
    return maxTopNLimit == that.maxTopNLimit &&
           useApproximateCountDistinct == that.useApproximateCountDistinct &&
           useApproximateTopN == that.useApproximateTopN &&
           requireTimeCondition == that.requireTimeCondition &&
           serializeComplexValues == that.serializeComplexValues &&
           Objects.equals(sqlTimeZone, that.sqlTimeZone) &&
           useNativeQueryExplain == that.useNativeQueryExplain &&
           forceExpressionVirtualColumns == that.forceExpressionVirtualColumns &&
           useGroupingSetForExactDistinct == that.useGroupingSetForExactDistinct &&
           computeInnerJoinCostAsFilter == that.computeInnerJoinCostAsFilter &&
           authorizeSystemTablesDirectly == that.authorizeSystemTablesDirectly &&
           maxNumericInFilters == that.maxNumericInFilters;
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(
        maxTopNLimit,
        useApproximateCountDistinct,
        useApproximateTopN,
        requireTimeCondition,
        sqlTimeZone,
        serializeComplexValues,
        useNativeQueryExplain,
        forceExpressionVirtualColumns
    );
  }

  @Override
  public String toString()
  {
    return "PlannerConfig{" +
           "maxTopNLimit=" + maxTopNLimit +
           ", useApproximateCountDistinct=" + useApproximateCountDistinct +
           ", useApproximateTopN=" + useApproximateTopN +
           ", requireTimeCondition=" + requireTimeCondition +
           ", sqlTimeZone=" + sqlTimeZone +
           ", serializeComplexValues=" + serializeComplexValues +
           ", useNativeQueryExplain=" + useNativeQueryExplain +
           '}';
  }

  public static Builder builder()
  {
    return new PlannerConfig().toBuilder();
  }

  public Builder toBuilder()
  {
    return new Builder(this);
  }

  /**
   * Builder for {@link PlannerConfig}, primarily for use in tests to
   * allow setting options programmatically rather than from the command
   * line or a properties file. Starts with values from an existing
   * (typically default) config.
   */
  public static class Builder
  {
    private int maxTopNLimit;
    private boolean useApproximateCountDistinct;
    private boolean useApproximateTopN;
    private boolean requireTimeCondition;
    private DateTimeZone sqlTimeZone;
    private boolean useGroupingSetForExactDistinct;
    private boolean computeInnerJoinCostAsFilter;
    private boolean authorizeSystemTablesDirectly;
    private boolean useNativeQueryExplain;
    private boolean forceExpressionVirtualColumns;
    private int maxNumericInFilters;
    private boolean serializeComplexValues;

    public Builder(PlannerConfig base)
    {
      // Note: use accessors, not fields, since some tests change the
      // config by defining a subclass.

      maxTopNLimit = base.getMaxTopNLimit();
      useApproximateCountDistinct = base.isUseApproximateCountDistinct();
      useApproximateTopN = base.isUseApproximateTopN();
      requireTimeCondition = base.isRequireTimeCondition();
      sqlTimeZone = base.getSqlTimeZone();
      useGroupingSetForExactDistinct = base.isUseGroupingSetForExactDistinct();
      computeInnerJoinCostAsFilter = base.computeInnerJoinCostAsFilter;
      authorizeSystemTablesDirectly = base.isAuthorizeSystemTablesDirectly();
      useNativeQueryExplain = base.isUseNativeQueryExplain();
      forceExpressionVirtualColumns = base.isForceExpressionVirtualColumns();
      maxNumericInFilters = base.getMaxNumericInFilters();
      serializeComplexValues = base.shouldSerializeComplexValues();
    }

    public Builder requireTimeCondition(boolean option)
    {
      this.requireTimeCondition = option;
      return this;
    }

    public Builder maxTopNLimit(int value)
    {
      this.maxTopNLimit = value;
      return this;
    }

    public Builder maxNumericInFilters(int value)
    {
      this.maxNumericInFilters = value;
      return this;
    }

    public Builder useApproximateCountDistinct(boolean option)
    {
      this.useApproximateCountDistinct = option;
      return this;
    }

    public Builder useApproximateTopN(boolean option)
    {
      this.useApproximateTopN = option;
      return this;
    }

    public Builder useGroupingSetForExactDistinct(boolean option)
    {
      this.useGroupingSetForExactDistinct = option;
      return this;
    }

    public Builder computeInnerJoinCostAsFilter(boolean option)
    {
      this.computeInnerJoinCostAsFilter = option;
      return this;
    }

    public Builder sqlTimeZone(DateTimeZone value)
    {
      this.sqlTimeZone = value;
      return this;
    }

    public Builder authorizeSystemTablesDirectly(boolean option)
    {
      this.authorizeSystemTablesDirectly = option;
      return this;
    }

    public Builder serializeComplexValues(boolean option)
    {
      this.serializeComplexValues = option;
      return this;
    }

    public Builder useNativeQueryExplain(boolean option)
    {
      this.useNativeQueryExplain = option;
      return this;
    }

    public Builder withOverrides(final Map<String, Object> queryContext)
    {
      useApproximateCountDistinct = QueryContexts.parseBoolean(
          queryContext,
          CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT,
          useApproximateCountDistinct
      );
      useGroupingSetForExactDistinct = QueryContexts.parseBoolean(
          queryContext,
          CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
          useGroupingSetForExactDistinct
      );
      useApproximateTopN = QueryContexts.parseBoolean(
          queryContext,
          CTX_KEY_USE_APPROXIMATE_TOPN,
          useApproximateTopN
      );
      computeInnerJoinCostAsFilter = QueryContexts.parseBoolean(
          queryContext,
          CTX_COMPUTE_INNER_JOIN_COST_AS_FILTER,
          computeInnerJoinCostAsFilter
      );
      useNativeQueryExplain = QueryContexts.parseBoolean(
          queryContext,
          CTX_KEY_USE_NATIVE_QUERY_EXPLAIN,
          useNativeQueryExplain
      );
      forceExpressionVirtualColumns = QueryContexts.parseBoolean(
          queryContext,
          CTX_KEY_FORCE_EXPRESSION_VIRTUAL_COLUMNS,
          forceExpressionVirtualColumns
      );
      final int queryContextMaxNumericInFilters = QueryContexts.parseInt(
          queryContext,
          CTX_MAX_NUMERIC_IN_FILTERS,
          maxNumericInFilters
      );
      maxNumericInFilters = validateMaxNumericInFilters(
          queryContextMaxNumericInFilters,
          maxNumericInFilters);
      return this;
    }

    private static int validateMaxNumericInFilters(int queryContextMaxNumericInFilters, int systemConfigMaxNumericInFilters)
    {
      // if maxNumericInFIlters through context == 0 catch exception
      // else if query context exceeds system set value throw error
      if (queryContextMaxNumericInFilters == 0) {
        throw new UOE("[%s] must be greater than 0", CTX_MAX_NUMERIC_IN_FILTERS);
      } else if (queryContextMaxNumericInFilters > systemConfigMaxNumericInFilters
                 && systemConfigMaxNumericInFilters != NUM_FILTER_NOT_USED) {
        throw new UOE(
            "Expected parameter[%s] cannot exceed system set value of [%d]",
            CTX_MAX_NUMERIC_IN_FILTERS,
            systemConfigMaxNumericInFilters
        );
      }
      // if system set value is not present, thereby inferring default of -1
      if (systemConfigMaxNumericInFilters == NUM_FILTER_NOT_USED) {
        return systemConfigMaxNumericInFilters;
      }
      // all other cases return the valid query context value
      return queryContextMaxNumericInFilters;
    }

    public PlannerConfig build()
    {
      PlannerConfig config = new PlannerConfig();
      config.maxTopNLimit = maxTopNLimit;
      config.useApproximateCountDistinct = useApproximateCountDistinct;
      config.useApproximateTopN = useApproximateTopN;
      config.requireTimeCondition = requireTimeCondition;
      config.sqlTimeZone = sqlTimeZone;
      config.useGroupingSetForExactDistinct = useGroupingSetForExactDistinct;
      config.computeInnerJoinCostAsFilter = computeInnerJoinCostAsFilter;
      config.authorizeSystemTablesDirectly = authorizeSystemTablesDirectly;
      config.useNativeQueryExplain = useNativeQueryExplain;
      config.maxNumericInFilters = maxNumericInFilters;
      config.forceExpressionVirtualColumns = forceExpressionVirtualColumns;
      config.serializeComplexValues = serializeComplexValues;
      return config;
    }
  }
}
