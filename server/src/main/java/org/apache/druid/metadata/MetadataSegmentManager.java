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
import org.apache.druid.client.ImmutableDruidDataSource;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

/**
 */
public interface MetadataSegmentManager
{
  void start();

  void stop();

  boolean enableDataSource(String dataSource);

  boolean enableSegment(String segmentId);

  boolean removeDataSource(String dataSource);

  /**
   * Prefer {@link #removeSegment(SegmentId)} to this method when possible.
   *
   * This method is not removed because {@link org.apache.druid.server.http.DataSourcesResource#deleteDatasourceSegment}
   * uses it and if it migrates to {@link #removeSegment(SegmentId)} the performance will be worse.
   */
  boolean removeSegment(String dataSource, String segmentId);

  boolean removeSegment(SegmentId segmentId);

  boolean isStarted();

  @Nullable
  ImmutableDruidDataSource getDataSource(String dataSourceName);

  Collection<ImmutableDruidDataSource> getDataSources();

  /**
   * Returns an iterable to go over all segments in all data sources. The order in which segments are iterated is
   * unspecified. Note: the iteration may not be as trivially cheap as, for example, iteration over an ArrayList. Try
   * (to some reasonable extent) to organize the code so that it iterates the returned iterable only once rather than
   * several times.
   */
  Iterable<DataSegment> iterateAllSegments();

  Collection<String> getAllDataSourceNames();

  /**
   * Returns top N unused segment intervals in given interval when ordered by segment start time, end time.
   */
  List<Interval> getUnusedSegmentIntervals(String dataSource, Interval interval, int limit);

  @VisibleForTesting
  void poll();
}
