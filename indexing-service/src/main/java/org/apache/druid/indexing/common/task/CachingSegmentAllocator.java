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

package org.apache.druid.indexing.common.task;

import org.apache.druid.indexing.common.task.IndexTask.ShardSpecs;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;

/**
 * SegmentAllocator that allocates all necessary segments upfront. This allocator should be used for the hash or range
 * secondary partitioning.
 *
 * In the hash or range secondary partitioning, the information about all partition buckets should be known before
 * the task starts to allocate segments. For example, for the hash partitioning, the task should know how many hash
 * buckets it will create, what is the hash value allocated for each bucket, etc. Similar for the range partitioning.
 */
public interface CachingSegmentAllocator extends SegmentAllocator
{
  /**
   * Returns the {@link org.apache.druid.timeline.partition.ShardSpec}s of all segments allocated upfront.
   */
  ShardSpecs getShardSpecs();
}
