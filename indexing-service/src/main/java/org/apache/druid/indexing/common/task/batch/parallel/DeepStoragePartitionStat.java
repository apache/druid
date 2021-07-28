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
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class DeepStoragePartitionStat extends GenericPartitionStat
{
  private final Map<String, Object> loadSpec;

  @JsonCreator
  public DeepStoragePartitionStat(
      @JsonProperty("taskExecutorHost") String taskExecutorHost,
      @JsonProperty("taskExecutorPort") int taskExecutorPort,
      @JsonProperty("useHttps") boolean useHttps,
      @JsonProperty("interval") Interval interval,
      @JsonProperty(PROP_SHARD_SPEC) BucketNumberedShardSpec shardSpec,
      @JsonProperty("numRows") @Nullable Integer numRows,
      @JsonProperty("sizeBytes") @Nullable Long sizeBytes,
      @JsonProperty("loadSpec") Map<String, Object> loadSpec
  )
  {
    super(taskExecutorHost, taskExecutorPort, useHttps, interval, shardSpec, numRows, sizeBytes);
    this.loadSpec = loadSpec;
  }

  @JsonProperty
  public Map<String, Object> getLoadSpec()
  {
    return loadSpec;
  }

  @Override
  public DeepStoragePartitionLocation toPartitionLocation(String subtaskId, BuildingShardSpec secondaryParition)
  {
    return new DeepStoragePartitionLocation(
        getTaskExecutorHost(),
        getTaskExecutorPort(),
        isUseHttps(),
        subtaskId,
        getInterval(),
        secondaryParition,
        getLoadSpec()
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
    if (!super.equals(o)) {
      return false;
    }
    DeepStoragePartitionStat that = (DeepStoragePartitionStat) o;
    return loadSpec.equals(that.loadSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), loadSpec);
  }

  @Override
  public String toString()
  {
    return "DeepStoragePartitionStat{" +
        "loadSpec=" + loadSpec +
        "} " + super.toString();
  }
}
