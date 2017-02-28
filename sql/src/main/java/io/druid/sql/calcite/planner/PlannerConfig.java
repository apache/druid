/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.IAE;
import org.joda.time.Period;

import java.util.Map;

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

    if (maxSemiJoinRowsInMemory != that.maxSemiJoinRowsInMemory) {
      return false;
    }
    if (maxTopNLimit != that.maxTopNLimit) {
      return false;
    }
    if (maxQueryCount != that.maxQueryCount) {
      return false;
    }
    if (selectThreshold != that.selectThreshold) {
      return false;
    }
    if (useApproximateCountDistinct != that.useApproximateCountDistinct) {
      return false;
    }
    if (useApproximateTopN != that.useApproximateTopN) {
      return false;
    }
    if (useFallback != that.useFallback) {
      return false;
    }
    return metadataRefreshPeriod != null
           ? metadataRefreshPeriod.equals(that.metadataRefreshPeriod)
           : that.metadataRefreshPeriod == null;
  }

  @Override
  public int hashCode()
  {
    int result = metadataRefreshPeriod != null ? metadataRefreshPeriod.hashCode() : 0;
    result = 31 * result + maxSemiJoinRowsInMemory;
    result = 31 * result + maxTopNLimit;
    result = 31 * result + maxQueryCount;
    result = 31 * result + selectThreshold;
    result = 31 * result + (useApproximateCountDistinct ? 1 : 0);
    result = 31 * result + (useApproximateTopN ? 1 : 0);
    result = 31 * result + (useFallback ? 1 : 0);
    return result;
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
           '}';
  }
}
