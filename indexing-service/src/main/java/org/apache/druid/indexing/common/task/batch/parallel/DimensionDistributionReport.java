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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.joda.time.Interval;

import java.util.Map;
import java.util.Objects;

public class DimensionDistributionReport implements SubTaskReport
{
  static final String TYPE = "dimension_distribution";
  private static final String PROP_DISTRIBUTIONS = "distributions";

  private final String taskId;
  private final Map<Interval, StringDistribution> intervalToDistribution;

  @JsonCreator
  public DimensionDistributionReport(
      @JsonProperty("taskId") String taskId,
      @JsonProperty(PROP_DISTRIBUTIONS) Map<Interval, StringDistribution> intervalToDistribution
  )
  {
    this.taskId = taskId;
    this.intervalToDistribution = intervalToDistribution;
  }

  @Override
  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty(PROP_DISTRIBUTIONS)
  public Map<Interval, StringDistribution> getIntervalToDistribution()
  {
    return intervalToDistribution;
  }

  @Override
  public String toString()
  {
    return "DimensionDistributionReport{" +
           "taskId='" + taskId + '\'' +
           ", intervalToDistribution=" + intervalToDistribution +
           '}';
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
    DimensionDistributionReport that = (DimensionDistributionReport) o;
    return Objects.equals(taskId, that.taskId) &&
           Objects.equals(intervalToDistribution, that.intervalToDistribution);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, intervalToDistribution);
  }
}
