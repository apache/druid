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

package org.apache.druid.timeline.partition;

/**
 * ShardSpec for non-first-generation segments.
 * This shardSpec is allocated a partitionId between {@link PartitionIds#NON_ROOT_GEN_START_PARTITION_ID} and
 * {@link PartitionIds#NON_ROOT_GEN_END_PARTITION_ID}.
 *
 * @see org.apache.druid.timeline.Overshadowable
 */
public interface OverwriteShardSpec extends ShardSpec
{
  /**
   * The core partition concept is not used with segment locking. Instead, the {@link AtomicUpdateGroup} is used
   * to atomically overshadow segments. Here, we always return 0 so that the {@link PartitionHolder} skips checking
   * the completeness of the core partitions.
   */
  @Override
  default int getNumCorePartitions()
  {
    return 0;
  }

  default OverwriteShardSpec withAtomicUpdateGroupSize(int atomicUpdateGroupSize)
  {
    return withAtomicUpdateGroupSize((short) atomicUpdateGroupSize);
  }

  OverwriteShardSpec withAtomicUpdateGroupSize(short atomicUpdateGroupSize);

  /**
   * Returns true if this shardSpec and the given {@link PartialShardSpec} share the same partition space.
   * This shardSpec uses non-root-generation partition space and thus does not share the space with other shardSpecs.
   *
   * @see PartitionIds
   */
  @Override
  default boolean sharePartitionSpace(PartialShardSpec partialShardSpec)
  {
    return partialShardSpec.useNonRootGenerationPartitionSpace();
  }
}
