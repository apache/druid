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

package org.apache.druid.metadata.segment;

import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Performs read operations on the segment metadata for a single datasource.
 */
public interface DatasourceSegmentMetadataReader
{
  /**
   * Returns the IDs of segments (out of the given set) which already exist in
   * the metadata store.
   */
  Set<String> findExistingSegmentIds(Set<DataSegment> segments);

  /**
   * Retrieves IDs of used segments that belong to the datasource and overlap
   * the given interval.
   */
  Set<SegmentId> findUsedSegmentIdsOverlapping(Interval interval);

  /**
   * Retrieves the ID of the unused segment that has the highest partition
   * number amongst all unused segments that exactly match the given interval
   * and version.
   */
  @Nullable
  SegmentId findHighestUnusedSegmentId(Interval interval, String version);

  /**
   * Finds used segments that overlap with any of the given intervals.
   */
  CloseableIterator<DataSegment> findUsedSegmentsOverlappingAnyOf(List<Interval> intervals);

  List<DataSegment> findUsedSegments(Set<String> segmentIds);

  /**
   * Finds used segments that overlap with any of the given intervals. If the
   * given list of intervals is empty, all used segments are considered eligible.
   */
  Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(List<Interval> intervals);

  @Nullable
  DataSegment findSegment(String segmentId);

  @Nullable
  DataSegment findUsedSegment(String segmentId);

  List<DataSegmentPlus> findSegments(Set<String> segmentIds);

  List<DataSegmentPlus> findSegmentsWithSchema(Set<String> segmentIds);

  List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  );

  List<SegmentIdWithShardSpec> findPendingSegmentIds(
      String sequenceName,
      String sequencePreviousId
  );

  List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(
      String sequenceName,
      Interval interval
  );

  List<PendingSegmentRecord> findPendingSegmentsOverlapping(Interval interval);

  List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(Interval interval);

  List<PendingSegmentRecord> findPendingSegments(String taskAllocatorId);
}
