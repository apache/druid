/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord;

import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 */
public interface IndexerMetadataStorageCoordinator
{
  /**
   * Get all segments which may include any data in the interval and are flagged as used.
   *
   * @param dataSource The datasource to query
   * @param interval   The interval for which all applicable and used datasources are requested. Start is inclusive, end is exclusive
   *
   * @return The DataSegments which include data in the requested interval. These segments may contain data outside the requested interval.
   *
   * @throws IOException
   */
  List<DataSegment> getUsedSegmentsForInterval(String dataSource, Interval interval)
      throws IOException;

  /**
   * Get all segments which may include any data in the interval and are flagged as used.
   *
   * @param dataSource The datasource to query
   * @param intervals  The intervals for which all applicable and used datasources are requested.
   *
   * @return The DataSegments which include data in the requested intervals. These segments may contain data outside the requested interval.
   *
   * @throws IOException
   */
  List<DataSegment> getUsedSegmentsForIntervals(String dataSource, List<Interval> intervals)
      throws IOException;

  /**
   * Attempts to insert a set of segments to the metadata storage. Returns the set of segments actually added (segments
   * with identifiers already in the metadata storage will not be added).
   *
   * @param segments set of segments to add
   *
   * @return set of segments actually added
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
   * @param maxVersion              use this version if we have no better version to use. The returned segment identifier may
   *                                have a version lower than this one, but will not have one higher.
   * @param skipSegmentLineageCheck if true, perform lineage validation using previousSegmentId for this sequence.
   *                                Should be set to false if replica tasks would index events in same order
   *
   * @return the pending segment identifier, or null if it was impossible to allocate a new segment
   */
  SegmentIdentifier allocatePendingSegment(
      String dataSource,
      String sequenceName,
      String previousSegmentId,
      Interval interval,
      String maxVersion,
      boolean skipSegmentLineageCheck
  ) throws IOException;

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
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  ) throws IOException;

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

  void updateSegmentMetadata(Set<DataSegment> segments) throws IOException;

  void deleteSegments(Set<DataSegment> segments) throws IOException;

  /**
   * Get all segments which include ONLY data within the given interval and are not flagged as used.
   *
   * @param dataSource The datasource the segments belong to
   * @param interval   Filter the data segments to ones that include data in this interval exclusively. Start is inclusive, end is exclusive
   *
   * @return DataSegments which include ONLY data within the requested interval and are not flagged as used. Data segments NOT returned here may include data in the interval
   */
  List<DataSegment> getUnusedSegmentsForInterval(String dataSource, Interval interval);
}
