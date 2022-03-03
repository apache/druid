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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.UOE;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.util.Map;
import java.util.Objects;

public class PlannerConfig
{
  public static final String CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT = "useApproximateCountDistinct";
  public static final String CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT = "useGroupingSetForExactDistinct";
  public static final String CTX_KEY_USE_APPROXIMATE_TOPN = "useApproximateTopN";
  public static final String CTX_COMPUTE_INNER_JOIN_COST_AS_FILTER = "computeInnerJoinCostAsFilter";
  public static final String CTX_KEY_USE_NATIVE_QUERY_EXPLAIN = "useNativeQueryExplain";
  public static final String CTX_MAX_NUMERIC_IN_FILTERS = "maxNumericInFilters";
  public static final int NUM_FILTER_NOT_USED = -1;

  @JsonProperty
  private Period metadataRefreshPeriod = new Period("PT1M");

  @JsonProperty
  private int maxTopNLimit = 100000;

  @JsonProperty
  private boolean useApproximateCountDistinct = true;

  @JsonProperty
  private boolean useApproximateTopN = true;

  @JsonProperty
  private boolean requireTimeCondition = false;

  @JsonProperty
  private boolean awaitInitializationOnStart = true;

  @JsonProperty
  private DateTimeZone sqlTimeZone = DateTimeZone.UTC;

  @JsonProperty
  private boolean metadataSegmentCacheEnable = false;

  @JsonProperty
  private long metadataSegmentPollPeriod = 60000;

  @JsonProperty
  private boolean useGroupingSetForExactDistinct = false;

  @JsonProperty
  private boolean computeInnerJoinCostAsFilter = true;

  @JsonProperty
  private boolean authorizeSystemTablesDirectly = false;

  @JsonProperty
  private boolean useNativeQueryExplain = false;

  @JsonProperty
  private int maxNumericInFilters = NUM_FILTER_NOT_USED;

  public long getMetadataSegmentPollPeriod()
  {
    return metadataSegmentPollPeriod;
  }

  public int getMaxNumericInFilters()
  {
    return maxNumericInFilters;
  }

  public boolean isMetadataSegmentCacheEnable()
  {
    return metadataSegmentCacheEnable;
  }

  private boolean serializeComplexValues = true;

  public Period getMetadataRefreshPeriod()
  {
    return metadataRefreshPeriod;
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

  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
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

  public PlannerConfig withOverrides(final Map<String, Object> context)
  {
    if (context == null) {
      return this;
    }

    final PlannerConfig newConfig = new PlannerConfig();
    newConfig.metadataRefreshPeriod = getMetadataRefreshPeriod();
    newConfig.maxTopNLimit = getMaxTopNLimit();
    newConfig.useApproximateCountDistinct = getContextBoolean(
        context,
        CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT,
        isUseApproximateCountDistinct()
    );
    newConfig.useGroupingSetForExactDistinct = getContextBoolean(
        context,
        CTX_KEY_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
        isUseGroupingSetForExactDistinct()
    );
    newConfig.useApproximateTopN = getContextBoolean(
        context,
        CTX_KEY_USE_APPROXIMATE_TOPN,
        isUseApproximateTopN()
    );
    newConfig.computeInnerJoinCostAsFilter = getContextBoolean(
        context,
        CTX_COMPUTE_INNER_JOIN_COST_AS_FILTER,
        computeInnerJoinCostAsFilter
    );
    newConfig.useNativeQueryExplain = getContextBoolean(
        context,
        CTX_KEY_USE_NATIVE_QUERY_EXPLAIN,
        isUseNativeQueryExplain()
    );
    final int systemConfigMaxNumericInFilters = getMaxNumericInFilters();
    final int queryContextMaxNumericInFilters = getContextInt(
        context,
        CTX_MAX_NUMERIC_IN_FILTERS,
        getMaxNumericInFilters()
    );
    newConfig.maxNumericInFilters = validateMaxNumericInFilters(queryContextMaxNumericInFilters,
                                                                systemConfigMaxNumericInFilters);
    newConfig.requireTimeCondition = isRequireTimeCondition();
    newConfig.sqlTimeZone = getSqlTimeZone();
    newConfig.awaitInitializationOnStart = isAwaitInitializationOnStart();
    newConfig.metadataSegmentCacheEnable = isMetadataSegmentCacheEnable();
    newConfig.metadataSegmentPollPeriod = getMetadataSegmentPollPeriod();
    newConfig.serializeComplexValues = shouldSerializeComplexValues();
    newConfig.authorizeSystemTablesDirectly = isAuthorizeSystemTablesDirectly();
    return newConfig;
  }

  private int validateMaxNumericInFilters(int queryContextMaxNumericInFilters, int systemConfigMaxNumericInFilters)
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

  private static int getContextInt(
      final Map<String, Object> context,
      final String parameter,
      final int defaultValue
  )
  {
    final Object value = context.get(parameter);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof String) {
      return Numbers.parseInt(value);
    } else if (value instanceof Integer) {
      return (Integer) value;
    } else {
      throw new IAE("Expected parameter[%s] to be integer", parameter);
    }
  }

  private static boolean getContextBoolean(
      final Map<String, Object> context,
      final String parameter,
      final boolean defaultValue
  )
  {
    final Object value = context.get(parameter);
    if (value == null) {
      return defaultValue;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    } else if (value instanceof Boolean) {
      return (Boolean) value;
    } else {
      throw new IAE("Expected parameter[%s] to be boolean", parameter);
    }
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
           awaitInitializationOnStart == that.awaitInitializationOnStart &&
           metadataSegmentCacheEnable == that.metadataSegmentCacheEnable &&
           metadataSegmentPollPeriod == that.metadataSegmentPollPeriod &&
           serializeComplexValues == that.serializeComplexValues &&
           Objects.equals(metadataRefreshPeriod, that.metadataRefreshPeriod) &&
           Objects.equals(sqlTimeZone, that.sqlTimeZone) &&
           useNativeQueryExplain == that.useNativeQueryExplain;
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(
        metadataRefreshPeriod,
        maxTopNLimit,
        useApproximateCountDistinct,
        useApproximateTopN,
        requireTimeCondition,
        awaitInitializationOnStart,
        sqlTimeZone,
        metadataSegmentCacheEnable,
        metadataSegmentPollPeriod,
        serializeComplexValues,
        useNativeQueryExplain
    );
  }

  @Override
  public String toString()
  {
    return "PlannerConfig{" +
           "metadataRefreshPeriod=" + metadataRefreshPeriod +
           ", maxTopNLimit=" + maxTopNLimit +
           ", useApproximateCountDistinct=" + useApproximateCountDistinct +
           ", useApproximateTopN=" + useApproximateTopN +
           ", requireTimeCondition=" + requireTimeCondition +
           ", awaitInitializationOnStart=" + awaitInitializationOnStart +
           ", metadataSegmentCacheEnable=" + metadataSegmentCacheEnable +
           ", metadataSegmentPollPeriod=" + metadataSegmentPollPeriod +
           ", sqlTimeZone=" + sqlTimeZone +
           ", serializeComplexValues=" + serializeComplexValues +
           ", useNativeQueryExplain=" + useNativeQueryExplain +
           '}';
  }
}
