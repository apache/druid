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

import java.util.Map;
import java.util.Objects;

/**
 * Similar to {@link GenericPartitionStat} but contains information about deep storage location where it is stored
 */
public class DeepStoragePartitionStat implements PartitionStat
{
  public static final String TYPE = "deepstore";
  static final String PROP_SHARD_SPEC = "shardSpec";
  private final Map<String, Object> loadSpec;
  // Primary partition key
  private final Interval interval;
  // Secondary partition key
  private final BucketNumberedShardSpec shardSpec;

  @JsonCreator
  public DeepStoragePartitionStat(
      @JsonProperty("interval") Interval interval,
      @JsonProperty(PROP_SHARD_SPEC) BucketNumberedShardSpec shardSpec,
      @JsonProperty("loadSpec") Map<String, Object> loadSpec
  )
  {
    this.interval = interval;
    this.shardSpec = shardSpec;
    this.loadSpec = loadSpec;
  }

  @JsonProperty
  public Map<String, Object> getLoadSpec()
  {
    return loadSpec;
  }

  @JsonProperty
  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty(PROP_SHARD_SPEC)
  @Override
  public BucketNumberedShardSpec getSecondaryPartition()
  {
    return shardSpec;
  }

  @Override
  public int getBucketId()
  {
    return shardSpec.getBucketId();
  }

  @Override
  public DeepStoragePartitionLocation toPartitionLocation(String subtaskId, BuildingShardSpec secondaryParition)
  {
    return new DeepStoragePartitionLocation(subtaskId, interval, secondaryParition, loadSpec);
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
    DeepStoragePartitionStat that = (DeepStoragePartitionStat) o;
    return loadSpec.equals(that.loadSpec) && interval.equals(that.interval) && shardSpec.equals(that.shardSpec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(loadSpec, interval, shardSpec);
  }

  @Override
  public String toString()
  {
    return "DeepStoragePartitionStat{" +
           "loadSpec=" + loadSpec +
           ", interval=" + interval +
           ", shardSpec=" + shardSpec +
           '}';
  }
}
