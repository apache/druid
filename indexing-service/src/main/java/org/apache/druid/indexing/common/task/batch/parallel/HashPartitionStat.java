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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Statistics about a partition created by {@link PartialHashSegmentGenerateTask}. Each partition is a set of data
 * of the same time chunk (primary partition key) and the same partitionId (secondary partition key). This class
 * holds the statistics of a single partition created by a task.
 */
public class HashPartitionStat extends PartitionStat<Integer>
{
  // Secondary partition key
  private final int partitionId;

  @JsonCreator
  public HashPartitionStat(
      @JsonProperty("taskExecutorHost") String taskExecutorHost,
      @JsonProperty("taskExecutorPort") int taskExecutorPort,
      @JsonProperty("useHttps") boolean useHttps,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("partitionId") int partitionId,
      @JsonProperty("numRows") @Nullable Integer numRows,
      @JsonProperty("sizeBytes") @Nullable Long sizeBytes
  )
  {
    super(taskExecutorHost, taskExecutorPort, useHttps, interval, numRows, sizeBytes);
    this.partitionId = partitionId;
  }

  @JsonProperty
  @Override
  public int getPartitionId()
  {
    return partitionId;
  }

  @JsonIgnore
  @Override
  Integer getSecondaryPartition()
  {
    return partitionId;
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
    if (!super.equals(o)) {
      return false;
    }
    HashPartitionStat that = (HashPartitionStat) o;
    return partitionId == that.partitionId;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), partitionId);
  }
}
