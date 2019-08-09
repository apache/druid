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

package org.apache.druid.timeline;

/**
 * Interface to represent a class which can have overshadow relation between its instances.
 * In {@link VersionedIntervalTimeline}, Overshadowable is used to represent each {@link DataSegment}
 * which has the same major version in the same time chunk.
 *
 * An Overshadowable overshadows another if its root partition range contains that of another
 * and has a higher minorVersion. For more details, check https://github.com/apache/incubator-druid/issues/7491.
 */
public interface Overshadowable<T extends Overshadowable>
{
  /**
   * Returns true if this overshadowable overshadows the other.
   */
  default boolean overshadows(T other)
  {
    final int majorVersionCompare = getVersion().compareTo(other.getVersion());
    if (majorVersionCompare == 0) {
      return containsRootPartition(other) && getMinorVersion() > other.getMinorVersion();
    } else {
      return majorVersionCompare > 0;
    }
  }

  default boolean containsRootPartition(T other)
  {
    return getStartRootPartitionId() <= other.getStartRootPartitionId()
        && getEndRootPartitionId() >= other.getEndRootPartitionId();
  }

  /**
   * All overshadowables have root partition range.
   * First-generation overshadowables have (partitionId, partitionId + 1) as their root partition range.
   * Non-first-generation overshadowables are the overshadowables that overwrite first or non-first generation
   * overshadowables, and they have the merged root partition range of all overwritten first-generation overshadowables.
   *
   * Note that first-generation overshadowables can be overwritten by a single non-first-generation overshadowable
   * if they have consecutive partitionId. Non-first-generation overshadowables can be overwritten by another
   * if their root partition ranges are consecutive.
   */
  int getStartRootPartitionId();

  /**
   * See doc of {@link #getStartRootPartitionId()}.
   */
  int getEndRootPartitionId();

  String getVersion();

  short getMinorVersion();

  /**
   * Return the size of atomicUpdateGroup.
   * An atomicUpdateGroup is a set of segments which should be updated all together atomically in
   * {@link VersionedIntervalTimeline}.
   */
  short getAtomicUpdateGroupSize();
}
