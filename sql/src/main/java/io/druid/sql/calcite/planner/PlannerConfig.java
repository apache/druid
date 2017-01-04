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
import org.joda.time.Period;

public class PlannerConfig
{
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
    return maxTopNLimit;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PlannerConfig that = (PlannerConfig) o;

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
