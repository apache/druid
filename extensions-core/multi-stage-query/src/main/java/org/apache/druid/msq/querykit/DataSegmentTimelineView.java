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

package org.apache.druid.msq.querykit;

import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.TimelineLookup;
import org.joda.time.Interval;

import java.util.List;
import java.util.Optional;

public interface DataSegmentTimelineView
{
  /**
   * Returns the timeline for a datasource, if it 'exists'. The analysis object passed in must represent a scan-based
   * datasource of a single table. (i.e., {@link DataSourceAnalysis#getBaseTableDataSource()} must be present.)
   *
   * @param dataSource table data source name
   * @param intervals  relevant intervals. The returned timeline will *at least* include all segments that overlap
   *                   these intervals. It may also include more. Empty means the timeline may not contain any
   *                   segments at all.
   *
   * @return timeline, if it 'exists'
   *
   * @throws IllegalStateException if 'analysis' does not represent a scan-based datasource of a single table
   */
  Optional<TimelineLookup<String, DataSegment>> getTimeline(
      String dataSource,
      List<Interval> intervals
  );
}
