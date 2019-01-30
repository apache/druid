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

  Collection<String> getAllDataSourceNames();

  /**
   * Returns top N unused segment intervals in given interval when ordered by segment start time, end time.
   */
  List<Interval> getUnusedSegmentIntervals(String dataSource, Interval interval, int limit);

  @VisibleForTesting
  void poll();
}
