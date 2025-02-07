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
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

/**
 * Performs write operations on the segment metadata of a single datasource.
 */
public interface DatasourceSegmentMetadataWriter
{
  /**
   * Inserts the given segments into the metadata store.
   *
   * @return Number of new segments inserted
   */
  int insertSegments(Set<DataSegmentPlus> segments);

  /**
   * Inserts the given segments into the metadata store while also persisting
   * additional metadata values such as number of rows and schema fingerprint.
   *
   * @return Number of new segments inserted
   */
  int insertSegmentsWithMetadata(Set<DataSegmentPlus> segments);

  /**
   * Marks the segments fully contained in the given interval as unused.
   *
   * @return Number of segments updated successfully
   */
  int markSegmentsWithinIntervalAsUnused(Interval interval, DateTime updateTime);

  /**
   * Deletes the segments for the given IDs from the metadata store.
   *
   * @return Number of segments deleted successfully
   */
  int deleteSegments(Set<SegmentId> segmentsIdsToDelete);

  /**
   * Updates the payload of the given segment in the metadata store.
   * This method is used only by legacy tasks "move", "archive" and "restore".
   *
   * @return true if the segment payload was updated successfully, false otherwise
   */
  boolean updateSegmentPayload(DataSegment segment);

  /**
   * Inserts a pending segment into the metadata store.
   *
   * @return true if the pending segment was inserted successfully, false otherwise
   */
  boolean insertPendingSegment(
      PendingSegmentRecord pendingSegment,
      boolean skipSegmentLineageCheck
  );

  /**
   * Inserts pending segments into the metadata store.
   *
   * @return Number of new pending segments inserted
   */
  int insertPendingSegments(
      List<PendingSegmentRecord> pendingSegments,
      boolean skipSegmentLineageCheck
  );

  /**
   * Deletes all pending segments from the metadata store.
   *
   * @return Number of pending segments deleted
   */
  int deleteAllPendingSegments();

  /**
   * Deletes pending segments for the given IDs from the metadata store.
   *
   * @return Number of pending segments deleted.
   */
  int deletePendingSegments(Set<String> segmentIdsToDelete);

  /**
   * Deletes pending segments allocated for the given {@code taskAllocatorID}
   * from the metadata store.
   *
   * @return Number of pending segments deleted
   */
  int deletePendingSegments(String taskAllocatorId);

  /**
   * Deletes all pending segments which were created during the given interval
   * from the metadata store.
   *
   * @return Number of pending segments deleted.
   */
  int deletePendingSegmentsCreatedIn(Interval interval);
}
