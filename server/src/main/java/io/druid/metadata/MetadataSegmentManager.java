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

package io.druid.metadata;

import io.druid.client.DruidDataSource;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;

/**
 */

public interface MetadataSegmentManager
{
  void start();

  void stop();

  boolean enableDatasource(final String ds);

  boolean enableSegment(final String segmentId);

  boolean removeDatasource(final String ds);

  boolean removeSegment(String ds, final String segmentID);

  boolean isStarted();

  DruidDataSource getInventoryValue(String key);

  Collection<DruidDataSource> getInventory();

  Collection<String> getAllDatasourceNames();

  /**
   * Returns top N unused segment intervals in given interval when ordered by segment start time, end time.
   */
  List<Interval> getUnusedSegmentIntervals(
      final String dataSource,
      final Interval interval,
      final int limit
  );

  public void poll();
}
