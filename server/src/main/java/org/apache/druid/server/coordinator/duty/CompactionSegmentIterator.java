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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.server.coordinator.CompactionStatistics;
import org.apache.druid.timeline.DataSegment;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Segments in the lists which are the elements of this iterator are sorted according to the natural segment order
 * (see {@link DataSegment#compareTo}).
 */
public interface CompactionSegmentIterator extends Iterator<List<DataSegment>>
{
  /**
   * Return a map of dataSourceName to CompactionStatistics.
   * This method returns the aggregated statistics of segments that was already compacted and does not need to be compacted
   * again. Hence, segment that were not returned by the {@link Iterator#next()} becuase it does not needs compaction.
   * Note that the aggregations returned by this method is only up to the current point of the iterator being iterated.
   */
  Map<String, CompactionStatistics> totalCompactedStatistics();

  /**
   * Return a map of dataSourceName to CompactionStatistics.
   * This method returns the aggregated statistics of segments that was skipped as it cannot be compacted.
   * Hence, segment that were not returned by the {@link Iterator#next()} becuase it cannot be compacted.
   * Note that the aggregations returned by this method is only up to the current point of the iterator being iterated.
   */
  Map<String, CompactionStatistics> totalSkippedStatistics();

}
