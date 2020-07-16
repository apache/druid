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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Statistics about a partition created by {@link PartialSegmentGenerateTask}. Each partition is a
 * set of data of the same time chunk (primary partition key) and the same secondary partition key
 * ({@link T}). This class holds the statistics of a single partition created by a task.
 */
abstract class PartitionStat<T>
{
  // Host and port of the task executor
  private final String taskExecutorHost;
  private final int taskExecutorPort;
  private final boolean useHttps;

  // Primary partition key
  private final Interval interval;

  // numRows and sizeBytes are always null currently and will be filled properly in the future.
  @Nullable
  private final Integer numRows;
  @Nullable
  private final Long sizeBytes;

  PartitionStat(
      String taskExecutorHost,
      int taskExecutorPort,
      boolean useHttps,
      Interval interval,
      @Nullable Integer numRows,
      @Nullable Long sizeBytes
  )
  {
    this.taskExecutorHost = taskExecutorHost;
    this.taskExecutorPort = taskExecutorPort;
    this.useHttps = useHttps;
    this.interval = interval;
    this.numRows = numRows == null ? 0 : numRows;
    this.sizeBytes = sizeBytes == null ? 0 : sizeBytes;
  }

  @JsonProperty
  public final String getTaskExecutorHost()
  {
    return taskExecutorHost;
  }

  @JsonProperty
  public final int getTaskExecutorPort()
  {
    return taskExecutorPort;
  }

  @JsonProperty
  public final boolean isUseHttps()
  {
    return useHttps;
  }

  @JsonProperty
  public final Interval getInterval()
  {
    return interval;
  }

  @Nullable
  @JsonProperty
  public final Integer getNumRows()
  {
    return numRows;
  }

  @Nullable
  @JsonProperty
  public final Long getSizeBytes()
  {
    return sizeBytes;
  }

  /**
   * @return Uniquely identifying index from 0..N-1 of the N partitions
   */
  abstract int getBucketId();

  /**
   * @return Definition of secondary partition. For example, for range partitioning, this should include the start/end.
   */
  abstract T getSecondaryPartition();

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PartitionStat that = (PartitionStat) o;
    return taskExecutorPort == that.taskExecutorPort &&
           useHttps == that.useHttps &&
           Objects.equals(taskExecutorHost, that.taskExecutorHost) &&
           Objects.equals(interval, that.interval) &&
           Objects.equals(numRows, that.numRows) &&
           Objects.equals(sizeBytes, that.sizeBytes);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskExecutorHost, taskExecutorPort, useHttps, interval, numRows, sizeBytes);
  }
}
