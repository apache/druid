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
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import java.util.Map;
import java.util.Objects;

public class PlannerConfig
{
  public static final String CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT = "useApproximateCountDistinct";
  public static final String CTX_KEY_USE_APPROXIMATE_TOPN = "useApproximateTopN";
  public static final String CTX_KEY_USE_FALLBACK = "useFallback";

  @JsonProperty
  private Period metadataRefreshPeriod = new Period("PT1M");

  @JsonProperty
  private int maxSemiJoinRowsInMemory = 100000;

  @JsonProperty
  private int maxTopNLimit = 100000;

  @JsonProperty
  private int maxQueryCount = 8;

  @JsonProperty
  private int selectThreshold = 1000;

  @JsonProperty
  private boolean useApproximateCountDistinct = true;

  @JsonProperty
  private boolean useApproximateTopN = true;

  @JsonProperty
  private boolean useFallback = false;

  @JsonProperty
  private boolean requireTimeCondition = false;

  @JsonProperty
  private DateTimeZone sqlTimeZone = DateTimeZone.UTC;

  public Period getMetadataRefreshPeriod()
  {
    return metadataRefreshPeriod;
  }

  public int getMaxSemiJoinRowsInMemory()
  {
    return maxSemiJoinRowsInMemory;
  }

  public int getMaxTopNLimit()
  {
    return maxTopNLimit;
  }

  public int getMaxQueryCount()
  {
    return maxQueryCount;
  }

  public int getSelectThreshold()
  {
    return selectThreshold;
  }

  public boolean isUseApproximateCountDistinct()
  {
    return useApproximateCountDistinct;
  }

  public boolean isUseApproximateTopN()
  {
    return useApproximateTopN;
  }

  public boolean isUseFallback()
  {
    return useFallback;
  }

  public boolean isRequireTimeCondition()
  {
    return requireTimeCondition;
  }

  public DateTimeZone getSqlTimeZone()
  {
    return sqlTimeZone;
  }

  public PlannerConfig withOverrides(final Map<String, Object> context)
  {
    if (context == null) {
      return this;
    }

    final PlannerConfig newConfig = new PlannerConfig();
    newConfig.metadataRefreshPeriod = getMetadataRefreshPeriod();
    newConfig.maxSemiJoinRowsInMemory = getMaxSemiJoinRowsInMemory();
    newConfig.maxTopNLimit = getMaxTopNLimit();
    newConfig.maxQueryCount = getMaxQueryCount();
    newConfig.selectThreshold = getSelectThreshold();
    newConfig.useApproximateCountDistinct = getContextBoolean(
        context,
        CTX_KEY_USE_APPROXIMATE_COUNT_DISTINCT,
        isUseApproximateCountDistinct()
    );
    newConfig.useApproximateTopN = getContextBoolean(
        context,
        CTX_KEY_USE_APPROXIMATE_TOPN,
        isUseApproximateTopN()
    );
    newConfig.useFallback = getContextBoolean(
        context,
        CTX_KEY_USE_FALLBACK,
        isUseFallback()
    );
    newConfig.requireTimeCondition = isRequireTimeCondition();
    newConfig.sqlTimeZone = getSqlTimeZone();
    return newConfig;
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
    return maxSemiJoinRowsInMemory == that.maxSemiJoinRowsInMemory &&
           maxTopNLimit == that.maxTopNLimit &&
           maxQueryCount == that.maxQueryCount &&
           selectThreshold == that.selectThreshold &&
           useApproximateCountDistinct == that.useApproximateCountDistinct &&
           useApproximateTopN == that.useApproximateTopN &&
           useFallback == that.useFallback &&
           requireTimeCondition == that.requireTimeCondition &&
           Objects.equals(metadataRefreshPeriod, that.metadataRefreshPeriod) &&
           Objects.equals(sqlTimeZone, that.sqlTimeZone);
  }

  @Override
  public int hashCode()
  {

    return Objects.hash(
        metadataRefreshPeriod,
        maxSemiJoinRowsInMemory,
        maxTopNLimit,
        maxQueryCount,
        selectThreshold,
        useApproximateCountDistinct,
        useApproximateTopN,
        useFallback,
        requireTimeCondition,
        sqlTimeZone
    );
  }

  @Override
  public String toString()
  {
    return "PlannerConfig{" +
           "metadataRefreshPeriod=" + metadataRefreshPeriod +
           ", maxSemiJoinRowsInMemory=" + maxSemiJoinRowsInMemory +
           ", maxTopNLimit=" + maxTopNLimit +
           ", maxQueryCount=" + maxQueryCount +
           ", selectThreshold=" + selectThreshold +
           ", useApproximateCountDistinct=" + useApproximateCountDistinct +
           ", useApproximateTopN=" + useApproximateTopN +
           ", useFallback=" + useFallback +
           ", requireTimeCondition=" + requireTimeCondition +
           ", sqlTimeZone=" + sqlTimeZone +
           '}';
  }
}
