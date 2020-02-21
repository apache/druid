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

package org.apache.druid.indexing.common.task.batch.parallel.distribution;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.timeline.partition.PartitionBoundaries;

/**
 * Counts frequencies of {@link String}s.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
    @JsonSubTypes.Type(name = StringSketch.NAME, value = StringSketch.class)
})
public interface StringDistribution
{
  /**
   * Record occurrence of {@link String}
   */
  void put(String element);

  /**
   * Record occurrence of {@link String} if it will become the new minimum element.
   */
  void putIfNewMin(String element);

  /**
   * Record occurrence of {@link String} if it will become the new maximum element;
   */
  void putIfNewMax(String element);

  /**
   * Split the distribution in the fewest number of evenly-sized partitions while honoring a max
   * partition size.
   *
   * @return List of elements that correspond to the endpoints of evenly-sized partitions of the
   * sorted elements.
   */
  PartitionBoundaries getEvenPartitionsByMaxSize(int maxSize);

  /**
   * Split the distribution in the fewest number of evenly-sized partitions while honoring a target
   * partition size (actual partition sizes may be slightly lower or higher).
   *
   * @return List of elements that correspond to the endpoints of evenly-sized partitions of the
   * sorted elements.
   */
  PartitionBoundaries getEvenPartitionsByTargetSize(int targetSize);
}
