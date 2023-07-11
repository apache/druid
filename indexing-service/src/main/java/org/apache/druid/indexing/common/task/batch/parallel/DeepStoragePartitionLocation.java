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
import org.apache.druid.timeline.partition.BuildingShardSpec;
import org.joda.time.Interval;

import java.util.Map;
import java.util.Objects;

/**
 * This class represents the intermediary deep storage location where the partition of {@code interval} and {@code shardSpec}
 * is stored.
 */
public class DeepStoragePartitionLocation implements PartitionLocation
{
  private final String subTaskId;
  private final Interval interval;
  private final BuildingShardSpec shardSpec;
  private final Map<String, Object> loadSpec;

  @JsonCreator
  public DeepStoragePartitionLocation(
      @JsonProperty("subTaskId") String subTaskId,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("shardSpec") BuildingShardSpec shardSpec,
      @JsonProperty("loadSpec") Map<String, Object> loadSpec
  )
  {
    this.subTaskId = subTaskId;
    this.interval = interval;
    this.shardSpec = shardSpec;
    this.loadSpec = loadSpec;
  }

  @JsonIgnore
  @Override
  public int getBucketId()
  {
    return shardSpec.getBucketId();
  }

  @JsonProperty
  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  @Override
  public BuildingShardSpec getShardSpec()
  {
    return shardSpec;
  }

  @JsonProperty
  @Override
  public String getSubTaskId()
  {
    return subTaskId;
  }

  @JsonProperty
  public Map<String, Object> getLoadSpec()
  {
    return loadSpec;
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
    DeepStoragePartitionLocation that = (DeepStoragePartitionLocation) o;
    return subTaskId.equals(that.subTaskId)
           && interval.equals(that.interval)
           && shardSpec.equals(that.shardSpec)
           && loadSpec.equals(that.loadSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(subTaskId, interval, shardSpec, loadSpec);
  }

  @Override
  public String toString()
  {
    return "DeepStoragePartitionLocation{" +
           "subTaskId='" + subTaskId + '\'' +
           ", interval=" + interval +
           ", shardSpec=" + shardSpec +
           ", loadSpec=" + loadSpec +
           '}';
  }
}
