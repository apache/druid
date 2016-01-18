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
  public List<DataSegment> getUsedSegmentsForInterval(final String dataSource, final Interval interval)
      throws IOException;

  /**
   * Get all segments which may include any data in the interval and are flagged as used.
   *
   * @param dataSource The datasource to query
   * @param intervals   The intervals for which all applicable and used datasources are requested.
   *
   * @return The DataSegments which include data in the requested intervals. These segments may contain data outside the requested interval.
   *
   * @throws IOException
   */
  public List<DataSegment> getUsedSegmentsForIntervals(final String dataSource, final List<Interval> intervals)
      throws IOException;

  /**
   * Attempts to insert a set of segments to the metadata storage. Returns the set of segments actually added (segments
   * with identifiers already in the metadata storage will not be added).
   *
   * @param segments set of segments to add
   *
   * @return set of segments actually added
   */
  public Set<DataSegment> announceHistoricalSegments(final Set<DataSegment> segments) throws IOException;

  /**
   * Allocate a new pending segment in the pending segments table. This segment identifier will never be given out
   * again, <em>unless</em> another call is made with the same dataSource, sequenceName, and previousSegmentId.
   * <p/>
   * The sequenceName and previousSegmentId parameters are meant to make it easy for two independent ingestion tasks
   * to produce the same series of segments.
   * <p/>
   * Note that a segment sequence may include segments with a variety of different intervals and versions.
   *
   * @param dataSource        dataSource for which to allocate a segment
   * @param sequenceName      name of the group of ingestion tasks producing a segment series
   * @param previousSegmentId previous segment in the series; may be null or empty, meaning this is the first segment
   * @param interval          interval for which to allocate a segment
   * @param maxVersion        use this version if we have no better version to use. The returned segment identifier may
   *                          have a version lower than this one, but will not have one higher.
   *
   * @return the pending segment identifier, or null if it was impossible to allocate a new segment
   */
  SegmentIdentifier allocatePendingSegment(
      String dataSource,
      String sequenceName,
      String previousSegmentId,
      Interval interval,
      String maxVersion
  ) throws IOException;

  public void updateSegmentMetadata(final Set<DataSegment> segments) throws IOException;

  public void deleteSegments(final Set<DataSegment> segments) throws IOException;

  /**
   * Get all segments which include ONLY data within the given interval and are not flagged as used.
   *
   * @param dataSource The datasource the segments belong to
   * @param interval   Filter the data segments to ones that include data in this interval exclusively. Start is inclusive, end is exclusive
   *
   * @return DataSegments which include ONLY data within the requested interval and are not flagged as used. Data segments NOT returned here may include data in the interval
   */
  public List<DataSegment> getUnusedSegmentsForInterval(final String dataSource, final Interval interval);
}
