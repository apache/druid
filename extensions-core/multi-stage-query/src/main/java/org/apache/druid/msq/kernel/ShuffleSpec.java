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
    @JsonSubTypes.Type(name = "maxCount", value = MaxCountShuffleSpec.class),
    @JsonSubTypes.Type(name = "targetSize", value = TargetSizeShuffleSpec.class)
})
public interface ShuffleSpec
{
  /**
   * Clustering key that will determine how data are partitioned during the shuffle.
   */
  ClusterBy getClusterBy();

  /**
   * Whether this stage aggregates by the clustering key or not.
   */
  boolean doesAggregateByClusterKey();

  /**
   * Whether {@link #generatePartitions} needs a nonnull collector.
   */
  boolean needsStatistics();

  /**
   * Generates a set of partitions based on the provided statistics.
   *
   * @param collector        must be nonnull if {@link #needsStatistics()} is true; may be null otherwise
   * @param maxNumPartitions maximum number of partitions to generate
   *
   * @return either the partition assignment, or (as an error) a number of partitions, greater than maxNumPartitions,
   * that would be expected to be created
   */
  Either<Long, ClusterByPartitions> generatePartitions(
      @Nullable ClusterByStatisticsCollector collector,
      int maxNumPartitions
  );
}
