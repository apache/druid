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

package org.apache.druid.indexing.overlord;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.metadata.PendingSegmentRecord;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles metadata transactions performed by the Overlord.
 */
public interface IndexerMetadataStorageCoordinator
{
  /**
   * Retrieves all published segments that have partial or complete overlap with
   * the given interval and are marked as used.
   */
  default Set<DataSegment> retrieveUsedSegmentsForInterval(
      String dataSource,
      Interval interval,
      Segments visibility
  )
  {
    return retrieveUsedSegmentsForIntervals(dataSource, Collections.singletonList(interval), visibility);
  }

  /**
   * Retrieves all published used segments for the given data source.
   *
   * @see #retrieveUsedSegmentsForInterval(String, Interval, Segments)
   */
  Set<DataSegment> retrieveAllUsedSegments(String dataSource, Segments visibility);

  /**
   * Retrieve all published segments which are marked as used and the created_date of these segments belonging to the
   * given data source and list of intervals from the metadata store.
   * <p>
   * Unlike other similar methods in this interface, this method doesn't accept a {@link Segments} "visibility"
   * parameter. The returned collection may include overshadowed segments and their created_dates, as if {@link
   * Segments#INCLUDING_OVERSHADOWED} was passed. It's the responsibility of the caller to filter out overshadowed ones
   * if needed.
   *
   * @param dataSource The data source to query
   * @param intervals The list of interval to query
   *
   * @return The DataSegments and the related created_date of segments
   */
  Collection<Pair<DataSegment, String>> retrieveUsedSegmentsAndCreatedDates(
      String dataSource,
      List<Interval> intervals
  );

  /**
   * Retrieves all published segments that have partial or complete overlap with
   * the given intervals and are marked as used.
   */
  Set<DataSegment> retrieveUsedSegmentsForIntervals(
      String dataSource,
      List<Interval> intervals,
      Segments visibility
  );

  /**
   * Retrieve all published segments which include ONLY data within the given interval and are marked as unused from the
   * metadata store.
   *
   * @param dataSource  The data source the segments belong to
   * @param interval    Filter the data segments to ones that include data in this interval exclusively.
   * @param limit The maximum number of unused segments to retreive. If null, no limit is applied.
   * @param maxUsedStatusLastUpdatedTime The maximum {@code used_status_last_updated} time. Any unused segment in {@code interval}
   *                                     with {@code used_status_last_updated} no later than this time will be included in the
   *                                     kill task. Segments without {@code used_status_last_updated} time (due to an upgrade
   *                                     from legacy Druid) will have {@code maxUsedStatusLastUpdatedTime} ignored
   * @return DataSegments which include ONLY data within the requested interval and are marked as unused. Segments NOT
   * returned here may include data in the interval
   */
  default List<DataSegment> retrieveUnusedSegmentsForInterval(
      String dataSource,
      Interval interval,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  )
  {
    return retrieveUnusedSegmentsForInterval(dataSource, interval, null, limit, maxUsedStatusLastUpdatedTime);
  }

  /**
   * Retrieve all published segments which include ONLY data within the given interval and are marked as unused from the
   * metadata store.
   *
   * @param dataSource  The data source the segments belong to
   * @param interval    Filter the data segments to ones that include data in this interval exclusively.
   * @param versions    An optional list of segment versions to retrieve in the given {@code interval}. If unspecified, all
   *                    versions of unused segments in the {@code interval} must be retrieved. If an empty list is passed,
   *                    no segments are retrieved.
   * @param limit The maximum number of unused segments to retreive. If null, no limit is applied.
   * @param maxUsedStatusLastUpdatedTime The maximum {@code used_status_last_updated} time. Any unused segment in {@code interval}
   *                                     with {@code used_status_last_updated} no later than this time will be included in the
   *                                     kill task. Segments without {@code used_status_last_updated} time (due to an upgrade
   *                                     from legacy Druid) will have {@code maxUsedStatusLastUpdatedTime} ignored
   * @return DataSegments which include ONLY data within the requested interval and are marked as unused. Segments NOT
   * returned here may include data in the interval
   */
  List<DataSegment> retrieveUnusedSegmentsForInterval(
      String dataSource,
      Interval interval,
      @Nullable List<String> versions,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  );

  /**
   * Retrieves segments for the given IDs, regardless of their visibility
   * (visible, overshadowed or unused).
   */
  Set<DataSegment> retrieveSegmentsById(String dataSource, Set<String> segmentIds);

  /**
   * Mark as unused segments which include ONLY data within the given interval.
   *
   * @param dataSource The data source the segments belong to
   * @param interval   Filter the data segments to ones that include data in this interval exclusively.
   *
   * @return number of segments marked unused
   */
  int markSegmentsAsUnusedWithinInterval(String dataSource, Interval interval);

  /**
   * Attempts to insert a set of segments and corresponding schema to the metadata storage.
   * Returns the set of segments actually added (segments with identifiers already in the metadata storage will not be added).
   *
   * @param segments set of segments to add
   * @param segmentSchemaMapping segment schema information to add
   *
   * @return set of segments actually added
   */
  Set<DataSegment> commitSegments(Set<DataSegment> segments, @Nullable SegmentSchemaMapping segmentSchemaMapping);

  /**
   * Allocates pending segments for the given requests in the pending segments table.
   * The segment id allocated for a request will not be given out again unless a
   * request is made with the same {@link SegmentCreateRequest}.
   *
   * @param dataSource              dataSource for which to allocate a segment
   * @param interval                interval for which to allocate a segment
   * @param skipSegmentLineageCheck if true, perform lineage validation using previousSegmentId for this sequence.
   *                                Should be set to false if replica tasks would index events in same order
   * @param requests                Requests for which to allocate segments. All
   *                                the requests must share the same partition space.
   * @param reduceMetadataIO        If true, try to use the segment ids instead of fetching every segment
   *                                payload from the metadata store
   * @return Map from request to allocated segment id. The map does not contain
   * entries for failed requests.
   */
  Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatePendingSegments(
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      List<SegmentCreateRequest> requests,
      boolean reduceMetadataIO
  );

  /**
   * Return a segment timeline of all used segments including overshadowed ones for a given datasource and interval
   * if skipSegmentPayloadFetchForAllocation is set to true, do not fetch all the segment payloads for allocation
   * Instead fetch all the ids and numCorePartitions using exactly one segment per version per interval
   * return a dummy DataSegment for each id that holds only the SegmentId and a NumberedShardSpec with numCorePartitions
   */
  SegmentTimeline getSegmentTimelineForAllocation(
      String dataSource,
      Interval interval,
      boolean skipSegmentPayloadFetchForAllocation
  );

  /**
   * Allocate a new pending segment in the pending segments table. This segment identifier will never be given out
   * again, <em>unless</em> another call is made with the same dataSource, sequenceName, and previousSegmentId.
   * <p/>
   * The sequenceName and previousSegmentId parameters are meant to make it easy for two independent ingestion tasks
   * to produce the same series of segments.
   * <p/>
   * Note that a segment sequence may include segments with a variety of different intervals and versions.
   *
   * @param dataSource              dataSource for which to allocate a segment
   * @param interval                interval for which to allocate a segment
   * @param skipSegmentLineageCheck if true, perform lineage validation using previousSegmentId for this sequence.
   *                                Should be set to false if replica tasks would index events in same order
   * @return the pending segment identifier, or null if it was impossible to allocate a new segment
   */
  @Nullable
  SegmentIdWithShardSpec allocatePendingSegment(
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      SegmentCreateRequest createRequest
  );

  /**
   * Delete pending segments created in the given interval belonging to the given data source from the pending segments
   * table. The {@code created_date} field of the pending segments table is checked to find segments to be deleted.
   * <p>
   * Note that the semantic of the interval (for `created_date`s) is different from the semantic of the interval
   * parameters in some other methods in this class, such as {@link #retrieveUsedSegmentsForInterval} (where the
   * interval is about the time column value in rows belonging to the segment).
   *
   * @param dataSource     dataSource
   * @param deleteInterval interval to check the {@code created_date} of pendingSegments
   *
   * @return number of deleted pending segments
   */
  int deletePendingSegmentsCreatedInInterval(String dataSource, Interval deleteInterval);

  /**
   * Delete all pending segments belonging to the given data source from the pending segments table.
   *
   * @return number of deleted pending segments
   * @see #deletePendingSegmentsCreatedInInterval(String, Interval) similar to this method but also accepts interval for
   * segments' `created_date`s
   */
  int deletePendingSegments(String dataSource);

  /**
   * Attempts to insert a set of segments and corresponding schema to the metadata storage.
   * Returns the set of segments actually added (segments with identifiers already in the metadata storage will not be added).
   * <p/>
   * If startMetadata and endMetadata are set, this insertion will be atomic with a compare-and-swap on dataSource
   * commit metadata.
   * <p>
   * If segmentsToDrop is not null and not empty, this insertion will be atomic with a insert-and-drop on inserting
   * {@param segments} and dropping {@param segmentsToDrop}.
   *
   * @param segments       set of segments to add, must all be from the same dataSource
   * @param startMetadata  dataSource metadata pre-insert must match this startMetadata according to
   *                       {@link DataSourceMetadata#matches(DataSourceMetadata)}. If null, this insert will
   *                       not involve a metadata transaction
   * @param endMetadata    dataSource metadata post-insert will have this endMetadata merged in with
   *                       {@link DataSourceMetadata#plus(DataSourceMetadata)}. If null, this insert will not
   *                       involve a metadata transaction
   * @param segmentSchemaMapping segment schema information to persist.
   *
   * @return segment publish result indicating transaction success or failure, and set of segments actually published.
   * This method must only return a failure code if it is sure that the transaction did not happen. If it is not sure,
   * it must throw an exception instead.
   *
   * @throws IllegalArgumentException if startMetadata and endMetadata are not either both null or both non-null
   * @throws RuntimeException         if the state of metadata storage after this call is unknown
   */
  SegmentPublishResult commitSegmentsAndMetadata(
      Set<DataSegment> segments,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  );

  /**
   * Commits segments and corresponding schema created by an APPEND task.
   * This method also handles segment upgrade scenarios that may result
   * from concurrent append and replace.
   * <ul>
   * <li>If a REPLACE task committed a segment that overlaps with any of the
   * appendSegments while this APPEND task was in progress, the appendSegments
   * are upgraded to the version of the replace segment.</li>
   * <li>If an appendSegment is covered by a currently active REPLACE lock, then
   * an entry is created for it in the upgrade_segments table, so that when the
   * REPLACE task finishes, it can upgrade the appendSegment as required.</li>
   * </ul>
   *
   * @param appendSegments             All segments created by an APPEND task that
   *                                   must be committed in a single transaction.
   * @param appendSegmentToReplaceLock Map from append segment to the currently
   *                                   active REPLACE lock (if any) covering it
   * @param taskAllocatorId            allocator id of the task committing the segments to be appended
   * @param segmentSchemaMapping       schema of append segments
   */
  SegmentPublishResult commitAppendSegments(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      String taskAllocatorId,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  );

  /**
   * Commits segments created by an APPEND task. This method also handles segment
   * upgrade scenarios that may result from concurrent append and replace. Also
   * commits start and end {@link DataSourceMetadata}.
   *
   * @see #commitAppendSegments
   * @see #commitSegmentsAndMetadata
   */
  SegmentPublishResult commitAppendSegmentsAndMetadata(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata,
      String taskGroup,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  );

  /**
   * Commits segments and corresponding schema created by a REPLACE task.
   * This method also handles the segment upgrade scenarios that may result
   * from concurrent append and replace.
   * <ul>
   * <li>If an APPEND task committed a segment to an interval locked by this task,
   * the append segment is upgraded to the version of the corresponding lock.
   * This is done with the help of entries created in the upgrade_segments table
   * in {@link #commitAppendSegments}</li>
   * </ul>
   *
   * @param replaceSegments        All segments created by a REPLACE task that
   *                               must be committed in a single transaction.
   * @param locksHeldByReplaceTask All active non-revoked REPLACE locks held by the task
   * @param segmentSchemaMapping  Segment schema to add.
   */
  SegmentPublishResult commitReplaceSegments(
      Set<DataSegment> replaceSegments,
      Set<ReplaceTaskLock> locksHeldByReplaceTask,
      @Nullable SegmentSchemaMapping segmentSchemaMapping
  );

  /**
   * Creates and inserts new IDs for the pending segments hat overlap with the given
   * replace segments being committed. The newly created pending segment IDs:
   * <ul>
   * <li>Have the same interval and version as that of an overlapping segment
   * committed by the REPLACE task.</li>
   * <li>Cannot be committed but are only used to serve realtime queries against
   * those versions.</li>
   * </ul>
   *
   * @param replaceSegments Segments being committed by a REPLACE task
   * @return List of inserted pending segment records
   */
  List<PendingSegmentRecord> upgradePendingSegmentsOverlappingWith(
      Set<DataSegment> replaceSegments
  );

  /**
   * Retrieves data source's metadata from the metadata store. Returns null if there is no metadata.
   */
  @Nullable DataSourceMetadata retrieveDataSourceMetadata(String dataSource);

  /**
   * Removes entry for 'dataSource' from the dataSource metadata table.
   *
   * @param dataSource identifier
   *
   * @return true if the entry was deleted, false otherwise
   */
  boolean deleteDataSourceMetadata(String dataSource);

  /**
   * Resets dataSourceMetadata entry for 'dataSource' to the one supplied.
   *
   * @param dataSource         identifier
   * @param dataSourceMetadata value to set
   *
   * @return true if the entry was reset, false otherwise
   */
  boolean resetDataSourceMetadata(String dataSource, DataSourceMetadata dataSourceMetadata) throws IOException;

  /**
   * Insert dataSourceMetadata entry for 'dataSource'.
   *
   * @param dataSource         identifier
   * @param dataSourceMetadata value to set
   *
   * @return true if the entry was inserted, false otherwise
   */
  boolean insertDataSourceMetadata(String dataSource, DataSourceMetadata dataSourceMetadata);

  /**
   * Remove datasource metadata created before the given timestamp and not in given excludeDatasources set.
   *
   * @param timestamp timestamp in milliseconds
   * @param excludeDatasources set of datasource names to exclude from removal
   * @return number of datasource metadata removed
   */
  int removeDataSourceMetadataOlderThan(long timestamp, @NotNull Set<String> excludeDatasources);

  /**
   * Similar to {@link #commitSegments}, but meant for streaming ingestion tasks for handling
   * the case where the task ingested no records and created no segments, but still needs to update the metadata
   * with the progress that the task made.
   * <p>
   * The metadata should undergo the same validation checks as performed by {@link #commitSegments}.
   *
   *
   * @param dataSource the datasource
   * @param startMetadata dataSource metadata pre-insert must match this startMetadata according to
   *                      {@link DataSourceMetadata#matches(DataSourceMetadata)}.
   * @param endMetadata   dataSource metadata post-insert will have this endMetadata merged in with
   *                      {@link DataSourceMetadata#plus(DataSourceMetadata)}.
   *
   * @return segment publish result indicating transaction success or failure.
   * This method must only return a failure code if it is sure that the transaction did not happen. If it is not sure,
   * it must throw an exception instead.
   *
   * @throws IllegalArgumentException if either startMetadata and endMetadata are null
   * @throws RuntimeException         if the state of metadata storage after this call is unknown
   */
  SegmentPublishResult commitMetadataOnly(
      String dataSource,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  );

  void updateSegmentMetadata(Set<DataSegment> segments);

  void deleteSegments(Set<DataSegment> segments);

  /**
   * Retrieve the segment for a given id from the metadata store. Return null if no such segment exists
   * <br>
   * If {@code includeUnused} is set, the segment {@code id} retrieval should also consider the set of unused segments
   * in the metadata store. Unused segments could be deleted by a kill task at any time and might lead to unexpected behaviour.
   * This option exists mainly to provide a consistent view of the metadata, for example, in calls from MSQ controller
   * and worker and would generally not be required.
   *
   * @param id The segment id to retrieve
   *
   * @return DataSegment used segment corresponding to given id
   */
  DataSegment retrieveSegmentForId(String id, boolean includeUnused);

  /**
   * Delete entries from the upgrade segments table after the corresponding replace task has ended
   * @param taskId - id of the task with replace locks
   * @return number of deleted entries from the metadata store
   */
  int deleteUpgradeSegmentsForTask(String taskId);

  /**
   * Delete pending segment for a give task group after all the tasks belonging to it have completed.
   * @param datasource datasource of the task
   * @param taskAllocatorId task id / task group / replica group for an appending task
   * @return number of pending segments deleted from the metadata store
   */
  int deletePendingSegmentsForTaskAllocatorId(String datasource, String taskAllocatorId);

  /**
   * Fetches all the pending segments of the datasource that overlap with a given interval.
   * @param datasource datasource to be queried
   * @param interval interval with which segments overlap
   * @return List of pending segment records
   */
  List<PendingSegmentRecord> getPendingSegments(String datasource, Interval interval);

  /**
   * Map from a segment ID to the segment ID from which it was upgraded
   * There should be no entry in the map for an original non-upgraded segment
   * @param dataSource data source
   * @param segmentIds ids of segments
   */
  Map<String, String> retrieveUpgradedFromSegmentIds(String dataSource, Set<String> segmentIds);

  /**
   * Map from a segment ID to a set containing
   * 1) all segment IDs that were upgraded from it AND are still present in the metadata store
   * 2) the segment ID itself if and only if it is still present in the metadata store
   * @param dataSource data source
   * @param segmentIds ids of the first segments which had the corresponding load spec
   */
  Map<String, Set<String>> retrieveUpgradedToSegmentIds(String dataSource, Set<String> segmentIds);
}
