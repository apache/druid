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

package org.apache.druid.msq.statistics;

import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.frame.key.RowKey;

/**
 * Collects statistics that help determine how to optimally partition a dataset to achieve a desired {@link ClusterBy}.
 *
 * Not thread-safe.
 */
public interface ClusterByStatisticsCollector
{
  /**
   * Returns the {@link ClusterBy} that this collector uses to bucket and sort keys.
   */
  ClusterBy getClusterBy();

  /**
   * Add a key to this collector.
   *
   * The "weight" parameter must be a positive integer. It should be 1 for "normal" reasonably-sized rows, and a
   * larger-than-1-but-still-small integer for "jumbo" rows. This allows {@link #generatePartitionsWithTargetWeight}
   * to behave reasonably when passed a row count for the target weight: if all rows are reasonably sized, weight
   * is equivalent to rows; however, if rows are jumbo then the generated partition ranges will have fewer rows to
   * accommodate the extra weight.
   */
  ClusterByStatisticsCollector add(RowKey key, int weight);

  /**
   * Add another collector's data to this collector. Does not modify the other collector.
   */
  ClusterByStatisticsCollector addAll(ClusterByStatisticsCollector other);

  /**
   * Add a snapshot to this collector.
   */
  ClusterByStatisticsCollector addAll(ClusterByStatisticsSnapshot other);

  /**
   * Estimated total amount of row weight in the dataset, based on what keys have been added so far.
   */
  long estimatedTotalWeight();

  /**
   * Whether this collector has encountered any multi-valued input at a particular key position.
   *
   * This method exists because {@link org.apache.druid.timeline.partition.DimensionRangeShardSpec} does not
   * support partitioning on multi-valued strings, so we need to know if any multi-valued strings exist in order
   * to decide whether we can use this kind of shard spec.
   *
   * @throws IllegalArgumentException if keyPosition is outside the range of {@link #getClusterBy()}
   * @throws IllegalStateException    if this collector was not checking keys for multiple-values
   */
  boolean hasMultipleValues(int keyPosition);

  /**
   * Removes all data from this collector.
   */
  ClusterByStatisticsCollector clear();

  /**
   * Generates key ranges, targeting a particular row weight per range. The actual amount of row weight per range
   * may be higher or lower than the provided target.
   */
  ClusterByPartitions generatePartitionsWithTargetWeight(long targetWeight);

  /**
   * Generates up to "maxNumPartitions" key ranges. The actual number of generated partitions may be less than the
   * provided maximum.
   */
  ClusterByPartitions generatePartitionsWithMaxCount(int maxNumPartitions);

  /**
   * Returns an immutable, JSON-serializable snapshot of this collector.
   */
  ClusterByStatisticsSnapshot snapshot();
}
