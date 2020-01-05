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
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.ShardSpecFactory;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 */
public interface IndexerMetadataStorageCoordinator
{
  /**
   * Get all published segments which may include any data in the interval and are marked as used.
   *
   * The order of segments within the returned collection is unspecified, but each segment is guaranteed to appear in
   * the collection only once.
   *
<<<<<<< HEAD
   * @return The DataSegments which include data in the requested interval. These segments may contain data outside the requested interval.
=======
   * @param dataSource The datasource to query
   * @param interval   The interval for which all applicable and used datasources are requested. Start is inclusive,
   *                   end is exclusive
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
>>>>>>> 66657012bf50c42b5d847a379f33f8a5bdda2dda
   */
  default Collection<DataSegment> getUsedSegmentsForInterval(String dataSource, Interval interval, Segments visibility)
  {
    return getUsedSegmentsForIntervals(dataSource, Collections.singletonList(interval), visibility);
  }

  /**
   * Get all published segments which are marked as used and the created_date of these segments in a given datasource
   * and interval.
   *
   * @param dataSource The datasource to query
   * @param interval   The interval for which all applicable and used datasources are requested. Start is inclusive,
   *                   end is exclusive
   *
   * @return The DataSegments and the related created_date of segments which include data in the requested interval
   */
  Collection<Pair<DataSegment, String>> getUsedSegmentAndCreatedDateForInterval(String dataSource, Interval interval);

  /**
   * Get all published segments which may include any data in the interval and are marked as used.
   *
   * The order of segments within the returned collection is unspecified, but each segment is guaranteed to appear in
   * the collection only once.
   *
   * @param dataSource The datasource to query
   * @param intervals  The intervals for which all applicable and used datasources are requested.
   * @param visibility Whether only visible or visible as well as overshadowed segments should be returned. The
   *                   visibility is considered within the specified intervals: that is, a segment which is visible
   *                   outside of the specified intervals, but overshadowed on the specified intervals will not be
   *                   returned if {@link Segments#ONLY_VISIBLE} is passed. See more precise description in the doc for
   *                   {@link Segments}.
   * @return The DataSegments which include data in the requested intervals. These segments may contain data outside the
   *         requested interval.
   *
<<<<<<< HEAD
   * @return The DataSegments which include data in the requested intervals. These segments may contain data outside the requested interval.
=======
   * @implNote This method doesn't return a {@link Set} because there may be an expectation that {@code Set.contains()}
   * is O(1) operation, while it's not the case for the returned collection unless it copies all segments into a new
   * {@link java.util.HashSet} or {@link com.google.common.collect.ImmutableSet} which may in turn be unnecessary in
   * other use cases. So clients should perform such copy themselves if they need {@link Set} semantics.
>>>>>>> 66657012bf50c42b5d847a379f33f8a5bdda2dda
   */
  Collection<DataSegment> getUsedSegmentsForIntervals(String dataSource, List<Interval> intervals, Segments visibility);

  /**
   * Attempts to insert a set of segments to the metadata storage. Returns the set of segments actually added (segments
   * with identifiers already in the metadata storage will not be added).
   *
   * @param segments set of segments to add
   *
   * @return set of segments actually added
   *
   * @throws IOException
   */
  Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments) throws IOException;

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
   * @param previousSegmentId       previous segment in the series; may be null or empty, meaning this is the first segment
   * @param interval                interval for which to allocate a segment
   * @param shardSpecFactory        shardSpecFactory containing all necessary information to create a shardSpec for the
   *                                new segmentId
   * @param maxVersion              use this version if we have no better version to use. The returned segment identifier may
   *                                have a version lower than this one, but will not have one higher.
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
      ShardSpecFactory shardSpecFactory,
      String maxVersion,
      boolean skipSegmentLineageCheck
  );

  /**
   * Delete pending segments created in the given interval for the given dataSource from the pending segments table.
   * The {@code created_date} field of the pending segments table is checked to find segments to be deleted.
   *
   * @param dataSource     dataSource
   * @param deleteInterval interval to check the {@code created_date} of pendingSegments
   *
   * @return number of deleted pending segments
   */
  int deletePendingSegments(String dataSource, Interval deleteInterval);

  /**
   * Attempts to insert a set of segments to the metadata storage. Returns the set of segments actually added (segments
   * with identifiers already in the metadata storage will not be added).
   * <p/>
   * If startMetadata and endMetadata are set, this insertion will be atomic with a compare-and-swap on dataSource
   * commit metadata.
   *
   * @param segments      set of segments to add, must all be from the same dataSource
   * @param startMetadata dataSource metadata pre-insert must match this startMetadata according to
   *                      {@link DataSourceMetadata#matches(DataSourceMetadata)}. If null, this insert will
   *                      not involve a metadata transaction
   * @param endMetadata   dataSource metadata post-insert will have this endMetadata merged in with
   *                      {@link DataSourceMetadata#plus(DataSourceMetadata)}. If null, this insert will not
   *                      involve a metadata transaction
   *
   * @return segment publish result indicating transaction success or failure, and set of segments actually published.
   * This method must only return a failure code if it is sure that the transaction did not happen. If it is not sure,
   * it must throw an exception instead.
   *
   * @throws IllegalArgumentException if startMetadata and endMetadata are not either both null or both non-null
   * @throws RuntimeException         if the state of metadata storage after this call is unknown
   */
  SegmentPublishResult announceHistoricalSegments(
      Set<DataSegment> segments,
      @Nullable DataSourceMetadata startMetadata,
      @Nullable DataSourceMetadata endMetadata
  ) throws IOException;

  /**
   * Similar to {@link #announceHistoricalSegments(Set)}, but meant for streaming ingestion tasks for handling
   * the case where the task ingested no records and created no segments, but still needs to update the metadata
   * with the progress that the task made.
   *
   * The metadata should undergo the same validation checks as performed by announceHistoricalSegments.
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

  /**
   * Read dataSource metadata. Returns null if there is no metadata.
   */
  DataSourceMetadata getDataSourceMetadata(String dataSource);

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

  void updateSegmentMetadata(Set<DataSegment> segments);

  void deleteSegments(Set<DataSegment> segments);

  /**
   * Get all published segments which include ONLY data within the given interval and are not marked as used.
   *
   * @param dataSource The datasource the segments belong to
   * @param interval   Filter the data segments to ones that include data in this interval exclusively. Start is
   *                   inclusive, end is exclusive
   *
   * @return DataSegments which include ONLY data within the requested interval and are not marked as used.
   *         Data segments NOT returned here may include data in the interval
   */
  List<DataSegment> getUnusedSegmentsForInterval(String dataSource, Interval interval);
}
