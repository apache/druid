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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.joda.time.Interval;

/**
 * Statistics about a partition created by {@link PartialSegmentGenerateTask}. Each partition is a
 * set of data of the same time chunk (primary partition key) and the same secondary partition key
 * ({@link T}). This class holds the statistics of a single partition created by a task.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = GenericPartitionStat.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "generic", value = GenericPartitionStat.class),
    @JsonSubTypes.Type(name = "deepstore", value = DeepStoragePartitionStat.class)
})
public interface PartitionStat<T, U>
{
  /**
   * @return Uniquely identifying index from 0..N-1 of the N partitions
   */
  int getBucketId();

  /**
   * @return Definition of secondary partition. For example, for range partitioning, this should include the start/end.
   */
  T getSecondaryPartition();

  /**
   * @return interval for the partition
   */
  Interval getInterval();

  /**
   * Converts partition stat to PartitionLocation
   * */
  PartitionLocation<U> toPartitionLocation(String subtaskId, U shardSpec);
}
