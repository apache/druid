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
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.task.batch.parallel.distribution.StringDistribution;
import org.apache.druid.indexing.stats.IngestionMetricsSnapshot;
import org.apache.druid.indexing.stats.NoopIngestionMetricsSnapshot;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class DimensionDistributionReport implements SucceededSubtaskReport
{
  static final String TYPE = "dimension_distribution";
  private static final String PROP_DISTRIBUTIONS = "distributions";

  private final long createdTimeNs;
  private final String taskId;
  private final Map<Interval, StringDistribution> intervalToDistribution;
  private final IngestionMetricsSnapshot metrics;

  public DimensionDistributionReport(
      String taskId,
      Map<Interval, StringDistribution> intervalToDistribution,
      IngestionMetricsSnapshot metrics
  )
  {
    this(System.nanoTime(), taskId, intervalToDistribution, metrics);
  }

  @JsonCreator
  DimensionDistributionReport(
      @JsonProperty("createdTimeNs") long createdTimeNs,
      @JsonProperty("taskId") String taskId,
      @JsonProperty(PROP_DISTRIBUTIONS) Map<Interval, StringDistribution> intervalToDistribution,
      // Metrics can be null when you have middleManagers of mixed versions during rolling update
      @JsonProperty("metrics") @Nullable IngestionMetricsSnapshot metrics
  )
  {
    this.createdTimeNs = createdTimeNs;
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
    this.intervalToDistribution = Preconditions.checkNotNull(intervalToDistribution, "intervalToDistribution");
    this.metrics = metrics == null ? NoopIngestionMetricsSnapshot.INSTANCE : metrics;
  }

  @Override
  @JsonProperty
  public long getCreatedTimeNs()
  {
    return createdTimeNs;
  }

  @Override
  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @Override
  @JsonProperty
  public IngestionMetricsSnapshot getMetrics()
  {
    return metrics;
  }

  @JsonProperty(PROP_DISTRIBUTIONS)
  public Map<Interval, StringDistribution> getIntervalToDistribution()
  {
    return intervalToDistribution;
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
    return createdTimeNs == that.createdTimeNs &&
           Objects.equals(taskId, that.taskId) &&
           Objects.equals(intervalToDistribution, that.intervalToDistribution) &&
           Objects.equals(metrics, that.metrics);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(createdTimeNs, taskId, intervalToDistribution, metrics);
  }

  @Override
  public String toString()
  {
    return "DimensionDistributionReport{" +
           "createdTimeNs=" + createdTimeNs +
           ", taskId='" + taskId + '\'' +
           ", intervalToDistribution=" + intervalToDistribution +
           ", metrics=" + metrics +
           '}';
  }
}
