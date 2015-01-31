/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord;

import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 */
public interface IndexerMetadataStorageCoordinator
{
  public List<DataSegment> getUsedSegmentsForInterval(final String dataSource, final Interval interval)
      throws IOException;

  /**
   * Attempts to insert a set of segments to the metadata storage. Returns the set of segments actually added (segments
   * with identifiers already in the metadata storage will not be added).
   *
   * @param segments set of segments to add
   * @return set of segments actually added
   */
  public Set<DataSegment> announceHistoricalSegments(final Set<DataSegment> segments) throws IOException;


  public void updateSegmentMetadata(final Set<DataSegment> segments) throws IOException;

  public void deleteSegments(final Set<DataSegment> segments) throws IOException;

  public List<DataSegment> getUnusedSegmentsForInterval(final String dataSource, final Interval interval);
}
