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
   * Retrieves the IDs of segments (out of the given set) which already exist in
   * the metadata store.
   */
  Set<String> findExistingSegmentIds(Set<SegmentId> segments);

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
   * Retrieves used segments that overlap with any of the given intervals.
   * If the given list of intervals is empty, all used segments are included in
   * the result.
   */
  Set<DataSegment> findUsedSegmentsOverlappingAnyOf(
      List<Interval> intervals
  );

  /**
   * Retrieves used segments for the given segment IDs.
   */
  List<DataSegmentPlus> findUsedSegments(Set<SegmentId> segmentIds);

  /**
   * Retrieves used segments that overlap with any of the given intervals. If the
   * given list of intervals is empty, all used segments are included in the result.
   */
  Set<DataSegmentPlus> findUsedSegmentsPlusOverlappingAnyOf(
      List<Interval> intervals
  );

  /**
   * Retrieves the segment for the given segment ID.
   *
   * @return null if no such segment exists in the metadata store.
   */
  @Nullable
  DataSegment findSegment(SegmentId segmentId);

  /**
   * Retrieves the used segment for the given segment ID.
   *
   * @return null if no such segment exists in the metadata store.
   */
  @Nullable
  DataSegment findUsedSegment(SegmentId segmentId);

  /**
   * Retrieves segments for the given segment IDs.
   */
  List<DataSegmentPlus> findSegments(
      Set<SegmentId> segmentIds
  );

  /**
   * Retrieves segments with additional metadata info such as number of rows and
   * schema fingerprint for the given segment IDs.
   */
  List<DataSegmentPlus> findSegmentsWithSchema(
      Set<SegmentId> segmentIds
  );

  /**
   * Retrieves unused segments that are fully contained within the given interval.
   *
   * @param interval       Returned segments must be fully contained within this
   *                       interval
   * @param versions       Optional list of segment versions. If passed as null,
   *                       all segment versions are eligible.
   * @param limit          Maximum number of segments to return. If passed as null,
   *                       all segments are returned.
   * @param maxUpdatedTime Returned segments must have a {@code used_status_last_updated}
   *                       which is either null or earlier than this value.
   */
  List<DataSegment> findUnusedSegments(
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUpdatedTime
  );

  /**
   * Retrieves pending segment IDs for the given sequence name and previous ID.
   */
  List<SegmentIdWithShardSpec> findPendingSegmentIds(
      String sequenceName,
      String sequencePreviousId
  );

  /**
   * Retrieves pending segment IDs that exactly match the given interval and
   * sequence name.
   */
  List<SegmentIdWithShardSpec> findPendingSegmentIdsWithExactInterval(
      String sequenceName,
      Interval interval
  );

  /**
   * Retrieves pending segments overlapping the given interval.
   */
  List<PendingSegmentRecord> findPendingSegmentsOverlapping(
      Interval interval
  );

  /**
   * Retrieves pending segments whose interval exactly aligns with the given
   * interval.
   */
  List<PendingSegmentRecord> findPendingSegmentsWithExactInterval(
      Interval interval
  );

  /**
   * Retrieves pending segments that were allocated for the specified
   * {@code taskAllocatorId}.
   */
  List<PendingSegmentRecord> findPendingSegments(
      String taskAllocatorId
  );
}
