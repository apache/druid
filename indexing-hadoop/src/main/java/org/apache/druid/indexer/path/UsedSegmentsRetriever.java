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

package org.apache.druid.indexer.path;

import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 */
public interface UsedSegmentsRetriever
{
  /**
   * Retrieve (potentially, from a remote node) all segments which may include any data in the interval and are marked
   * as used.
   *
   * The order of segments within the returned collection is unspecified, but each segment is guaranteed to appear in
   * the collection only once.
   *
   * @param dataSource The datasource to query
   * @param intervals  The intervals for which used segments are to be returned
   * @param visibility Whether only visible or visible as well as overshadowed segments should be returned. The
   *                   visibility is considered within the specified intervals: that is, a segment which is visible
   *                   outside of the specified intervals, but overshadowed on the specified intervals will not be
   *                   returned if {@link Segments#ONLY_VISIBLE} is passed. See more precise description in the doc for
   *                   {@link Segments}.
   * @return The DataSegments which include data in the requested intervals. These segments may contain data outside the
   *         requested interval.
   *
   * @implNote This method doesn't return a {@link java.util.Set} because it's implemented via {@link
   * org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator#retrieveUsedSegmentsForIntervals} and which
   * returns a collection. Producing a {@link java.util.Set} would require an unnecessary copy of segments collection.
   */
  Collection<DataSegment> retrieveUsedSegmentsForIntervals(
      String dataSource,
      List<Interval> intervals,
      Segments visibility
  ) throws IOException;
}
