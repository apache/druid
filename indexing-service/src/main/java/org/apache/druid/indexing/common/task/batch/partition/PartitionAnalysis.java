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

import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.joda.time.Interval;

import java.util.Set;

/**
 * Analysis of the partitions to create. The implementation is mutable and updated by the indexing
 * {@link org.apache.druid.indexing.common.task.Task}.
 *
 * This interface provides all time chunks for the primary partitioning and the bucket information per time chunk
 * for the secondary partitioning.
 */
public interface PartitionAnalysis<T, P extends PartitionsSpec>
{
  P getPartitionsSpec();

  void updateBucket(Interval interval, T bucketAnalysis);

  /**
   * Returns the analysis of the secondary bucket for the given time chunk.
   *
   * @throws IllegalArgumentException if the bucket analysis is missing for the given interval
   */
  T getBucketAnalysis(Interval interval);

  Set<Interval> getAllIntervalsToIndex();

  int getNumTimePartitions();
}
