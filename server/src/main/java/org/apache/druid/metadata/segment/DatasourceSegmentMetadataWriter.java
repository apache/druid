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
   */
  int insertSegments(Set<DataSegmentPlus> segments);

  /**
   * Inserts the given segments into the metadata store while also persisting
   * additional metadata values such as number of rows and schema fingerprint.
   */
  int insertSegmentsWithMetadata(Set<DataSegmentPlus> segments);

  /**
   * Marks the segments fully contained in the given interval as unused.
   */
  int markSegmentsWithinIntervalAsUnused(Interval interval, DateTime updateTime);

  int deleteSegments(Set<String> segmentsIdsToDelete);

  boolean updateSegmentPayload(DataSegment segment);

  boolean insertPendingSegment(
      PendingSegmentRecord pendingSegment,
      boolean skipSegmentLineageCheck
  );

  int insertPendingSegments(
      List<PendingSegmentRecord> pendingSegments,
      boolean skipSegmentLineageCheck
  );

  int deleteAllPendingSegments();

  int deletePendingSegments(Set<String> segmentIdsToDelete);

  int deletePendingSegments(String taskAllocatorId);

  int deletePendingSegmentsCreatedIn(Interval interval);
}
