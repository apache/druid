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
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.PartialShardSpec;
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
 */
public interface IndexerMetadataStorageCoordinator
{
  /**
   * Retrieve all published segments which may include any data in the interval and are marked as used from the
   * metadata store.
   *
   * The order of segments within the returned collection is unspecified, but each segment is guaranteed to appear in
   * the collection only once.
   *
   * @param dataSource The data source to query
   * @param interval   The interval for which all applicable and used segmented are requested.
   * @param visibility Whether only visible or visible as well as overshadowed segments should be returned. The
   *                   visibility is considered within the specified interval: that is, a segment which is visible
   *                   outside of the specified interval, but overshadowed within the specified interval will not be
   *                   returned if {@link Segments#ONLY_VISIBLE} is passed. See more precise description in the doc for
   *                   {@link Segments}.
   * @return The DataSegments which include data in the requested interval. These segments may contain data outside the
   *         requested interval.
   *
   * @implNote This method doesn't return a {@link Set} because there may be an expectation that {@code Set.contains()}
   * is O(1) operation, while it's not the case for the returned collection unless it copies all segments into a new
   * {@link java.util.HashSet} or {@link com.google.common.collect.ImmutableSet} which may in turn be unnecessary in
   * other use cases. So clients should perform such copy themselves if they need {@link Set} semantics.
   */
  default Collection<DataSegment> retrieveUsedSegmentsForInterval(
      String dataSource,
      Interval interval,
      Segments visibility
  )
  {
    return retrieveUsedSegmentsForIntervals(dataSource, Collections.singletonList(interval), visibility);
  }

  /**
   * Retrieve all published used segments in the data source from the metadata store.
   *
   * @param dataSource The data source to query
   *
   * @return all segments belonging to the given data source
   * @see #retrieveUsedSegmentsForInterval(String, Interval, Segments) similar to this method but also accepts data
   * interval.
   */
  Collection<DataSegment> retrieveAllUsedSegments(String dataSource, Segments visibility);

  /**
   *
   * Retrieve all published segments which are marked as used and the created_date of these segments belonging to the
   * given data source and list of intervals from the metadata store.
   *
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
  Collection<Pair<DataSegment, String>> retrieveUsedSegmentsAndCreatedDates(String dataSource, List<Interval> intervals);

  /**
   * Retrieve all published segments which may include any data in the given intervals and are marked as used from the
   * metadata store.
   *
   * The order of segments within the returned collection is unspecified, but each segment is guaranteed to appear in
   * the collection only once.
   *
   * @param dataSource The data source to query
   * @param intervals  The intervals for which all applicable and used segments are requested.
   * @param visibility Whether only visible or visible as well as overshadowed segments should be returned. The
   *                   visibility is considered within the specified intervals: that is, a segment which is visible
   *                   outside of the specified intervals, but overshadowed on the specified intervals will not be
   *                   returned if {@link Segments#ONLY_VISIBLE} is passed. See more precise description in the doc for
   *                   {@link Segments}.
   * @return The DataSegments which include data in the requested intervals. These segments may contain data outside the
   *         requested intervals.
   *
   * @implNote This method doesn't return a {@link Set} because there may be an expectation that {@code Set.contains()}
   * is O(1) operation, while it's not the case for the returned collection unless it copies all segments into a new
   * {@link java.util.HashSet} or {@link com.google.common.collect.ImmutableSet} which may in turn be unnecessary in
   * other use cases. So clients should perform such copy themselves if they need {@link Set} semantics.
   */
  Collection<DataSegment> retrieveUsedSegmentsForIntervals(
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
   *                                   with {@code used_status_last_updated} no later than this time will be included in the
   *                                   kill task. Segments without {@code used_status_last_updated} time (due to an upgrade
   *                                   from legacy Druid) will have {@code maxUsedStatusLastUpdatedTime} ignored
   *
   * @return DataSegments which include ONLY data within the requested interval and are marked as unused. Segments NOT
   * returned here may include data in the interval
   */
  List<DataSegment> retrieveUnusedSegmentsForInterval(
      String dataSource,
      Interval interval,
      @Nullable Integer limit,
      @Nullable DateTime maxUsedStatusLastUpdatedTime
  );

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
   * Attempts to insert a set of segments to the metadata storage. Returns the set of segments actually added (segments
   * with identifiers already in the metadata storage will not be added).
   *
   * @param segments set of segments to add
   *
   * @return set of segments actually added
   */
  Set<DataSegment> commitSegments(Set<DataSegment> segments) throws IOException;

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
   * @return Map from request to allocated segment id. The map does not contain
   * entries for failed requests.
   */
  Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatePendingSegments(
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      List<SegmentCreateRequest> requests
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
   * @param sequenceName            name of the group of ingestion tasks producing a segment series
   * @param previousSegmentId       previous segment in the series; may be null or empty, meaning this is the first
   *                                segment
   * @param interval                interval for which to allocate a segment
   * @param partialShardSpec        partialShardSpec containing all necessary information to create a shardSpec for the
   *                                new segmentId
   * @param maxVersion              use this version if we have no better version to use. The returned segment
   *                                identifier may have a version lower than this one, but will not have one higher.
   * @param skipSegmentLineageCheck if true, perform lineage validation using previousSegmentId for this sequence.
   *                                Should be set to false if replica tasks would index events in same order
   *
   * @return the pending segment identifier, or null if it was impossible to allocate a new segment
   */
  SegmentIdWithShardSpec allocatePendingSegment(
      String dataSource,
      String sequenceName,
      @Nullable String previousSegmentId,
      Interval interval,
      PartialShardSpec partialShardSpec,
      String maxVersion,
      boolean skipSegmentLineageCheck
  );

  /**
   * Delete pending segments created in the given interval belonging to the given data source from the pending segments
   * table. The {@code created_date} field of the pending segments table is checked to find segments to be deleted.
   *
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
   * Attempts to insert a set of segments to the metadata storage. Returns the set of segments actually added (segments
   * with identifiers already in the metadata storage will not be added).
   * <p/>
   * If startMetadata and endMetadata are set, this insertion will be atomic with a compare-and-swap on dataSource
   * commit metadata.
   *
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
      @Nullable DataSourceMetadata endMetadata
  ) throws IOException;

  /**
   * Commits segments created by an APPEND task. This method also handles segment
   * upgrade scenarios that may result from concurrent append and replace.
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
   */
  SegmentPublishResult commitAppendSegments(
      Set<DataSegment> appendSegments,
      Map<DataSegment, ReplaceTaskLock> appendSegmentToReplaceLock
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
      DataSourceMetadata endMetadata
  );

  /**
   * Commits segments created by a REPLACE task. This method also handles the
   * segment upgrade scenarios that may result from concurrent append and replace.
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
   */
  SegmentPublishResult commitReplaceSegments(
      Set<DataSegment> replaceSegments,
      Set<ReplaceTaskLock> locksHeldByReplaceTask
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
   * @param activeRealtimeSequencePrefixes Set of sequence prefixes of active and pending completion task groups
   *                                       of the supervisor (if any) for this datasource
   * @return Map from originally allocated pending segment to its new upgraded ID.
   */
  Map<SegmentIdWithShardSpec, SegmentIdWithShardSpec> upgradePendingSegmentsOverlappingWith(
      Set<DataSegment> replaceSegments,
      Set<String> activeRealtimeSequencePrefixes
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
   *
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
}
