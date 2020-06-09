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

  public long getMetadataSegmentPollPeriod()
  {
    return metadataSegmentPollPeriod;
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
    newConfig.useApproximateTopN = getContextBoolean(
        context,
        CTX_KEY_USE_APPROXIMATE_TOPN,
        isUseApproximateTopN()
    );
    newConfig.requireTimeCondition = isRequireTimeCondition();
    newConfig.sqlTimeZone = getSqlTimeZone();
    newConfig.awaitInitializationOnStart = isAwaitInitializationOnStart();
    newConfig.metadataSegmentCacheEnable = isMetadataSegmentCacheEnable();
    newConfig.metadataSegmentPollPeriod = getMetadataSegmentPollPeriod();
    newConfig.serializeComplexValues = shouldSerializeComplexValues();
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
    return maxTopNLimit == that.maxTopNLimit &&
           useApproximateCountDistinct == that.useApproximateCountDistinct &&
           useApproximateTopN == that.useApproximateTopN &&
           requireTimeCondition == that.requireTimeCondition &&
           awaitInitializationOnStart == that.awaitInitializationOnStart &&
           metadataSegmentCacheEnable == that.metadataSegmentCacheEnable &&
           metadataSegmentPollPeriod == that.metadataSegmentPollPeriod &&
           serializeComplexValues == that.serializeComplexValues &&
           Objects.equals(metadataRefreshPeriod, that.metadataRefreshPeriod) &&
           Objects.equals(sqlTimeZone, that.sqlTimeZone);
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
        serializeComplexValues
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
           '}';
  }
}
