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

package org.apache.druid.msq.kernel;

import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;

import javax.annotation.Nullable;

/**
 * Additional methods for {@link ShuffleSpec} of kind {@link ShuffleKind#GLOBAL_SORT}.
 */
public interface GlobalSortShuffleSpec extends ShuffleSpec
{
  /**
   * Whether {@link #generatePartitionsForGlobalSort} needs a nonnull collector in order to do its work.
   */
  boolean mustGatherResultKeyStatistics();

  /**
   * Whether the {@link ClusterByStatisticsCollector} for this stage collects keys in aggregating mode or
   * non-aggregating mode.
   */
  boolean doesAggregate();

  /**
   * Generates a set of partitions based on the provided statistics.
   *
   * Only valid if {@link #kind()} is {@link ShuffleKind#GLOBAL_SORT}. Otherwise, throws {@link IllegalStateException}.
   *
   * @param collector        must be nonnull if {@link #mustGatherResultKeyStatistics()} is true; ignored otherwise
   * @param maxNumPartitions maximum number of partitions to generate
   *
   * @return either the partition assignment, or (as an error) a number of partitions, greater than maxNumPartitions,
   * that would be expected to be created
   *
   * @throws IllegalStateException if {@link #kind()} is not {@link ShuffleKind#GLOBAL_SORT}.
   */
  Either<Long, ClusterByPartitions> generatePartitionsForGlobalSort(
      @Nullable ClusterByStatisticsCollector collector,
      int maxNumPartitions
  );
}
