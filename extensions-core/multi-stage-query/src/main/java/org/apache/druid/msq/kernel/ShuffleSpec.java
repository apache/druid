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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.frame.key.ClusterBy;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.msq.statistics.ClusterByStatisticsCollector;

import javax.annotation.Nullable;

/**
 * Describes how outputs of a stage are shuffled. Property of {@link StageDefinition}.
 *
 * When the output of a stage is shuffled, it is globally sorted and partitioned according to the {@link ClusterBy}.
 * Hash-based (non-sorting) shuffle is not currently implemented.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = MixShuffleSpec.TYPE, value = MixShuffleSpec.class),
    @JsonSubTypes.Type(name = HashShuffleSpec.TYPE, value = HashShuffleSpec.class),
    @JsonSubTypes.Type(name = GlobalSortMaxCountShuffleSpec.TYPE, value = GlobalSortMaxCountShuffleSpec.class),
    @JsonSubTypes.Type(name = GlobalSortTargetSizeShuffleSpec.TYPE, value = GlobalSortTargetSizeShuffleSpec.class)
})
public interface ShuffleSpec
{
  /**
   * The nature of this shuffle: hash vs. range based partitioning; whether the data are sorted or not.
   */
  ShuffleKind kind();

  /**
   * Partitioning key for the shuffle.
   *
   * If {@link #kind()} is {@link ShuffleKind#HASH}, data are partitioned using a hash of this key, but not sorted.
   *
   * If {@link #kind()} is {@link ShuffleKind#HASH_LOCAL_SORT}, data are partitioned using a hash of this key, and sorted
   * within each partition.
   *
   * If {@link #kind()} is {@link ShuffleKind#GLOBAL_SORT}, data are partitioned using ranges of this key, and are
   * sorted within each partition; therefore, the data are also globally sorted.
   */
  ClusterBy clusterBy();

  /**
   * Whether this stage aggregates by the {@link #clusterBy()} key.
   */
  boolean doesAggregate();

  /**
   * Whether {@link #generatePartitionsForGlobalSort} needs a nonnull collector.
   */
  boolean needsStatistics();

  /**
   * Number of partitions, if known.
   *
   * Partition count is always known if {@link #kind()} is {@link ShuffleKind#MIX}, {@link ShuffleKind#HASH}, or
   * {@link ShuffleKind#HASH_LOCAL_SORT}. It is sometimes known if the kind is {@link ShuffleKind#GLOBAL_SORT}: in
   * particular, it is known if {@link GlobalSortMaxCountShuffleSpec} is used.
   *
   * @throws IllegalStateException if number of partitions is not known ahead of time
   */
  int partitionCount();

  /**
   * Generates a set of partitions based on the provided statistics.
   *
   * Only valid if {@link #kind()} is {@link ShuffleKind#GLOBAL_SORT}. Otherwise, throws {@link IllegalStateException}.
   *
   * @param collector        must be nonnull if {@link #needsStatistics()} is true; ignored otherwise
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
