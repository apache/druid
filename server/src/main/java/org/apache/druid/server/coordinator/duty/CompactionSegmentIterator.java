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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.server.coordinator.CompactionStatistics;
import org.apache.druid.timeline.DataSegment;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Segments in the lists which are the elements of this iterator are sorted according to the natural segment order
 * (see {@link DataSegment#compareTo}).
 */
public interface CompactionSegmentIterator extends Iterator<List<DataSegment>>
{
  /**
   * Iterate through all the remaining segments for all datasources currently in this iterator.
   * This method can be use before calling {@code totalRemainingStatistics} and {@code totalProcessedStatistics}
   * to get correct statistics for the datasources regardless of
   * how many (or none) times this iterator was iterated.
   * This method should determines remaining segments as needs compaction or does not needs compaction
   * and aggregates the segment's statistics accordingly.
   *
   * WARNING: this method iterate the underlying iterator and causes segments to be iterated.
   * This method should only be called when iteration of this iterator is no longer required.
   */
  void flushAllSegments();

  /**
   * Return a map of (dataSource, CompactionStatistics) for all dataSources.
   * This method should consider only segments that was not iterated by {@link #next()} and was not deemed as
   * needs compaction by {@link #flushAllSegments()}.
   */
  Map<String, CompactionStatistics> totalRemainingStatistics();

  /**
   * Return a map of (dataSource, CompactionStatistics) for all dataSources.
   * This method should consider only segments that has one of the following:
   * - iterated and returned by {@link #next()}
   * - Segments was already compacted and does not need to be compacted again
   */
  Map<String, CompactionStatistics> totalCompactedStatistics();

  /**
   * Return a map of (dataSource, CompactionStatistics) for all dataSources.
   * This method should consider only segments that cannot be compacted and hence was skipped from iteration
   */
  Map<String, CompactionStatistics> totalSkippedStatistics();

}
