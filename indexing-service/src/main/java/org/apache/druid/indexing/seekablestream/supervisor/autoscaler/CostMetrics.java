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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler;

import java.util.Objects;

/**
 * Data class that encapsulates all metrics collected for cost-based auto-scaling decisions.
 * This includes lag metrics, task counts, partition information, and idle time measurements.
 */
public class CostMetrics
{
  private final double avgPartitionLag;
  private final int currentTaskCount;
  private final int partitionCount;
  private final double pollIdleRatio;

  public CostMetrics(
      double avgPartitionLag,
      int currentTaskCount,
      int partitionCount,
      double pollIdleRatio
  )
  {
    this.avgPartitionLag = avgPartitionLag;
    this.currentTaskCount = currentTaskCount;
    this.partitionCount = partitionCount;
    this.pollIdleRatio = pollIdleRatio;
  }

  /**
   * Returns the average partition lag (ingest/kafka/partitionLag equivalent).
   * This is the average lag per partition across all partitions.
   */
  public double getAvgPartitionLag()
  {
    return avgPartitionLag;
  }

  public int getCurrentTaskCount()
  {
    return currentTaskCount;
  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  /**
   * Returns the poll idle ratio (equivalent to Kafka's poll-idle-ratio-avg).
   * Value between 0.0 and 1.0 where higher values indicate more idle time.
   */
  public double getPollIdleRatio()
  {
    return pollIdleRatio;
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
    CostMetrics that = (CostMetrics) o;
    return timestamp == that.timestamp
           && Double.compare(that.avgPartitionLag, avgPartitionLag) == 0
           && currentTaskCount == that.currentTaskCount
           && partitionCount == that.partitionCount
           && Double.compare(that.pollIdleRatio, pollIdleRatio) == 0;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        timestamp,
        avgPartitionLag,
        currentTaskCount,
        partitionCount,
        pollIdleRatio
    );
  }

  @Override
  public String toString()
  {
    return "CostMetrics{" +
           "timestamp=" + timestamp +
           ", avgPartitionLag=" + avgPartitionLag +
           ", currentTaskCount=" + currentTaskCount +
           ", partitionCount=" + partitionCount +
           ", pollIdleRatio=" + pollIdleRatio +
           '}';
  }
}
