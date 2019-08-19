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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.segment.realtime.appenderator.SegmentAllocator;
import org.joda.time.Interval;

/**
 * Segment allocator interface for {@link IndexTask}. It has 3 different modes for allocating segments.
 */
public interface IndexTaskSegmentAllocator extends SegmentAllocator
{
  /**
   * SequenceName is the key to create the segmentId. If previousSegmentId is given, {@link SegmentAllocator} allocates
   * segmentId depending on sequenceName and previousSegmentId. If it's missing, it allocates segmentId using
   * sequenceName and interval. For {@link IndexTask}, it always provides the previousSegmentId to
   * SegmentAllocator.
   * See {@link org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator#allocatePendingSegment} for details.
   *
   * Implementations should return the correct sequenceName based on the given interval and inputRow, which is passed
   * to SegmentAllocator.
   */
  String getSequenceName(Interval interval, InputRow inputRow);
}
