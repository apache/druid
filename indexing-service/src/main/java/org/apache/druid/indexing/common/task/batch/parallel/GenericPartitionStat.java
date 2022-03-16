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
import org.apache.druid.timeline.partition.BucketNumberedShardSpec;
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Generic partition description ({@link ShardSpec}) and statistics created by {@link PartialSegmentGenerateTask}. Each
 * partition is a set of data of the same time chunk (primary partition key) and the same {@link ShardSpec} (secondary
 * partition key). The {@link ShardSpec} is later used by {@link PartialGenericSegmentMergeTask} to merge the partial
 * segments.
 */
public class GenericPartitionStat implements PartitionStat
{
  public static final String TYPE = "local";
  static final String PROP_SHARD_SPEC = "shardSpec";

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
  // Secondary partition key
  private final BucketNumberedShardSpec shardSpec;

  @JsonCreator
  public GenericPartitionStat(
      @JsonProperty("taskExecutorHost") String taskExecutorHost,
      @JsonProperty("taskExecutorPort") int taskExecutorPort,
      @JsonProperty("useHttps") boolean useHttps,
      @JsonProperty("interval") Interval interval,
      @JsonProperty(PROP_SHARD_SPEC) BucketNumberedShardSpec shardSpec,
      @JsonProperty("numRows") @Nullable Integer numRows,
      @JsonProperty("sizeBytes") @Nullable Long sizeBytes
  )
  {
    this.taskExecutorHost = taskExecutorHost;
    this.taskExecutorPort = taskExecutorPort;
    this.useHttps = useHttps;
    this.interval = interval;
    this.numRows = numRows == null ? 0 : numRows;
    this.sizeBytes = sizeBytes == null ? 0 : sizeBytes;
    this.shardSpec = shardSpec;
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
  @Override
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

  @Override
  public int getBucketId()
  {
    return shardSpec.getBucketId();
  }

  @JsonProperty(PROP_SHARD_SPEC)
  @Override
  public BucketNumberedShardSpec getSecondaryPartition()
  {
    return shardSpec;
  }

  @Override
  public GenericPartitionLocation toPartitionLocation(String subtaskId, BuildingShardSpec secondaryParition)
  {
    return new GenericPartitionLocation(
        getTaskExecutorHost(),
        getTaskExecutorPort(),
        isUseHttps(),
        subtaskId,
        getInterval(),
        secondaryParition
    );
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
    GenericPartitionStat that = (GenericPartitionStat) o;
    return taskExecutorPort == that.taskExecutorPort
           && useHttps == that.useHttps
           && taskExecutorHost.equals(that.taskExecutorHost)
           && interval.equals(that.interval)
           && Objects.equals(numRows, that.numRows)
           && Objects.equals(sizeBytes, that.sizeBytes)
           && shardSpec.equals(that.shardSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskExecutorHost, taskExecutorPort, useHttps, interval, numRows, sizeBytes, shardSpec);
  }

  @Override
  public String toString()
  {
    return "GenericPartitionStat{" +
           "taskExecutorHost='" + taskExecutorHost + '\'' +
           ", taskExecutorPort=" + taskExecutorPort +
           ", useHttps=" + useHttps +
           ", interval=" + interval +
           ", numRows=" + numRows +
           ", sizeBytes=" + sizeBytes +
           ", shardSpec=" + shardSpec +
           '}';
  }
}
