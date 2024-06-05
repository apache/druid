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

/**
 * Describes how outputs of a stage are shuffled. Property of {@link StageDefinition}.
 *
 * When the output of a stage is shuffled, it is globally sorted and partitioned according to the {@link ClusterBy}.
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
   *
   * If this method returns {@link ShuffleKind#GLOBAL_SORT}, then this spec is also an instance of
   * {@link GlobalSortShuffleSpec}, and additional methods are available.
   */
  ShuffleKind kind();

  /**
   * Partitioning key for the shuffle.
   *
   * If {@link #kind()} is {@link ShuffleKind#HASH}, data are partitioned using a hash of this key, but not sorted.
   *
   * If {@link #kind()} is {@link ShuffleKind#HASH_LOCAL_SORT}, data are partitioned using a hash of this key, and
   * sorted within each partition.
   *
   * If {@link #kind()} is {@link ShuffleKind#GLOBAL_SORT}, data are partitioned using ranges of this key, and are
   * sorted within each partition; therefore, the data are also globally sorted.
   */
  ClusterBy clusterBy();

  /**
   * Number of partitions, if known in advance.
   *
   * Partition count is always known if {@link #kind()} is {@link ShuffleKind#MIX}, {@link ShuffleKind#HASH}, or
   * {@link ShuffleKind#HASH_LOCAL_SORT}. For {@link ShuffleKind#GLOBAL_SORT}, it is known if we have a single
   * output partition.
   *
   * @throws IllegalStateException if kind is {@link ShuffleKind#GLOBAL_SORT} with more than one target partition
   */
  int partitionCount();
}
