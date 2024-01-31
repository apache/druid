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

package org.apache.druid.metadata;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.druid.client.DataSourcesSnapshot;
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * The difference between this class and org.apache.druid.sql.calcite.schema.MetadataSegmentView is that this class
 * resides in Coordinator's memory, while org.apache.druid.sql.calcite.schema.MetadataSegmentView resides in Broker's
 * memory.
 */
public interface SegmentsMetadataManager
{
  void startPollingDatabasePeriodically();

  void stopPollingDatabasePeriodically();

  boolean isPollingDatabasePeriodically();

  /**
   * Returns the number of segment entries in the database whose state was changed as the result of this call (that is,
   * the segments were marked as used). If the call results in a database error, an exception is relayed to the caller.
   */
  int markAsUsedAllNonOvershadowedSegmentsInDataSource(String dataSource);

  int markAsUsedNonOvershadowedSegmentsInInterval(String dataSource, Interval interval);

  int markAsUsedNonOvershadowedSegments(String dataSource, Set<String> segmentIds)
      throws UnknownSegmentIdsException;

  /**
   * Returns true if the state of the segment entry is changed in the database as the result of this call (that is, the
   * segment was marked as used), false otherwise. If the call results in a database error, an exception is relayed to
   * the caller.
   */
  boolean markSegmentAsUsed(String segmentId);

  /**
   * Returns the number of segment entries in the database whose state was changed as the result of this call (that is,
   * the segments were marked as unused). If the call results in a database error, an exception is relayed to the
   * caller.
   */
  int markAsUnusedAllSegmentsInDataSource(String dataSource);

  int markAsUnusedSegmentsInInterval(String dataSource, Interval interval);

  int markSegmentsAsUnused(Set<SegmentId> segmentIds);

  /**
   * Returns true if the state of the segment entry is changed in the database as the result of this call (that is, the
   * segment was marked as unused), false otherwise. If the call results in a database error, an exception is relayed to
   * the caller.
   */
  boolean markSegmentAsUnused(SegmentId segmentId);

  /**
   * If there are used segments belonging to the given data source this method returns them as an {@link
   * ImmutableDruidDataSource} object. If there are no used segments belonging to the given data source this method
   * returns null.
   */
  @Nullable ImmutableDruidDataSource getImmutableDataSourceWithUsedSegments(String dataSource);

  /**
   * Returns a set of {@link ImmutableDruidDataSource} objects containing information about all used segments. {@link
   * ImmutableDruidDataSource} objects in the returned collection are unique. If there are no used segments, this method
   * returns an empty collection.
   */
  Collection<ImmutableDruidDataSource> getImmutableDataSourcesWithAllUsedSegments();

  /**
   * Returns a snapshot of DruidDataSources and overshadowed segments
   */
  DataSourcesSnapshot getSnapshotOfDataSourcesWithAllUsedSegments();

  /**
   * Returns an iterable to go over all segments in all data sources. The order in which segments are iterated is
   * unspecified. Note: the iteration may not be as trivially cheap as, for example, iteration over an ArrayList. Try
   * (to some reasonable extent) to organize the code so that it iterates the returned iterable only once rather than
   * several times.
   */
  Iterable<DataSegment> iterateAllUsedSegments();

  /**
   * Returns an iterable to go over all used and non-overshadowed segments of given data sources over given interval.
   * The order in which segments are iterated is unspecified. Note: the iteration may not be as trivially cheap as,
   * for example, iteration over an ArrayList. Try (to some reasonable extent) to organize the code so that it
   * iterates the returned iterable only once rather than several times.
   * If {@param requiresLatest} is true then a force metadatastore poll will be triggered. This can cause a longer
   * response time but will ensure that the latest segment information (at the time this method is called) is returned.
   * If {@param requiresLatest} is false then segment information from stale snapshot of up to the last periodic poll
   * period {@link SqlSegmentsMetadataManager#periodicPollDelay} will be used.
   */
  Optional<Iterable<DataSegment>> iterateAllUsedNonOvershadowedSegmentsForDatasourceInterval(
      String datasource,
      Interval interval,
      boolean requiresLatest
  );

  /**
   * Returns an iterable to go over un-used segments and their associated metadata for a given datasource over an
   * optional interval. The order in which segments are iterated is from earliest start-time, with ties being broken
   * with earliest end-time first. Note: the iteration may not be as trivially cheap as for example, iteration over an
   * ArrayList. Try (to some reasonable extent) to organize the code so that it iterates the returned iterable only
   * once rather than several times.
   *
   * @param datasource    the name of the datasource.
   * @param interval      an optional interval to search over. If none is specified, {@link org.apache.druid.java.util.common.Intervals#ETERNITY}
   * @param limit         an optional maximum number of results to return. If none is specified, the results are not limited.
   * @param lastSegmentId an optional last segment id from which to search for results. All segments returned are >
   *                      this segment lexigraphically if sortOrder is null or  {@link SortOrder#ASC}, or < this segment
   *                      lexigraphically if sortOrder is {@link SortOrder#DESC}. If none is specified, no such filter is used.
   * @param sortOrder     an optional order with which to return the matching segments by id, start time, end time.
   *                      If none is specified, the order of the results is not guarenteed.
   */
  Iterable<DataSegmentPlus> iterateAllUnusedSegmentsForDatasource(
      String datasource,
      @Nullable Interval interval,
      @Nullable Integer limit,
      @Nullable String lastSegmentId,
      @Nullable SortOrder sortOrder
  );

  /**
   * Retrieves all data source names for which there are segment in the database, regardless of whether those segments
   * are used or not. If there are no segments in the database, returns an empty set.
   *
   * Performance warning: this method makes a query into the database.
   *
   * This method might return a different set of data source names than may be observed via {@link
   * #getImmutableDataSourcesWithAllUsedSegments} method. This method will include a data source name even if there
   * are no used segments belonging to it, while {@link #getImmutableDataSourcesWithAllUsedSegments} won't return
   * such a data source.
   */
  Set<String> retrieveAllDataSourceNames();

  /**
   * Returns a list of up to {@code limit} unused segment intervals for the specified datasource. Segments are filtered based on the following criteria:
   *
   * <li> The start time of the segment must be no earlier than the specified {@code minStartTime} (if not null). </li>
   * <li> The end time of the segment must be no later than the specified {@code maxEndTime}. </li>
   * <li> The {@code used_status_last_updated} time of the segment must be no later than {@code maxUsedStatusLastUpdatedTime}.
   *      Segments that have no {@code used_status_last_updated} time (due to an upgrade from legacy Druid) will
   *      have {@code maxUsedStatusLastUpdatedTime} ignored. </li>
   *
   * <p>
   * The list of intervals is ordered by segment start time and then by end time.
   * </p>
   */
  List<Interval> getUnusedSegmentIntervals(
      String dataSource,
      DateTime minStartTime,
      DateTime maxEndTime,
      int limit,
      DateTime maxUsedStatusLastUpdatedTime
  );

  @VisibleForTesting
  void poll();

  /**
   * Populates used_status_last_updated column in the segments table iteratively until there are no segments with a NULL
   * value for that column.
   */
  void populateUsedFlagLastUpdatedAsync();

  void stopAsyncUsedFlagLastUpdatedUpdate();
}
