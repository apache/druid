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

package org.apache.druid.indexing.common.task.batch.partition;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.timeline.partition.BuildingHashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Interval;

import java.util.List;

public class HashBucket implements PartitionBucket
{
  private final Interval interval;
  private final int bucketId;
  private final int numBuckets;
  private final List<String> partitionDimensions;
  private final ObjectMapper jsonMapper;

  public HashBucket(
      Interval interval,
      int bucketId,
      int numBuckets,
      List<String> partitionDimensions,
      ObjectMapper jsonMapper
  )
  {
    this.interval = interval;
    this.bucketId = bucketId;
    this.numBuckets = numBuckets;
    this.partitionDimensions = partitionDimensions;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public Interval getInterval()
  {
    return interval;
  }

  @Override
  public int getBucketId()
  {
    return bucketId;
  }

  @Override
  public ShardSpec toShardSpec(int partitionId)
  {
    return new BuildingHashBasedNumberedShardSpec(partitionId, bucketId, numBuckets, partitionDimensions, jsonMapper);
  }

  @Override
  public String toString()
  {
    return "HashBucket{" +
           "interval=" + interval +
           ", bucketId=" + bucketId +
           ", numBuckets=" + numBuckets +
           ", partitionDimensions=" + partitionDimensions +
           '}';
  }
}
