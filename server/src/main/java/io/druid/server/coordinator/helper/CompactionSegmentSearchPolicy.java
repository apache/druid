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

package io.druid.server.coordinator.helper;

import io.druid.server.coordinator.CoordinatorCompactionConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.VersionedIntervalTimeline;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Segment searching policy used by {@link DruidCoordinatorSegmentCompactor}.
 */
public interface CompactionSegmentSearchPolicy
{
  /**
   * Reset the current states of this policy. This method should be called whenever iterating starts.
   */
  void reset(
      Map<String, CoordinatorCompactionConfig> compactionConfigs,
      Map<String, VersionedIntervalTimeline<String, DataSegment>> dataSources
  );

  /**
   * Return a next batch of segments for compaction.
   *
   * @return a list of segments if exist, null otherwise.
   */
  @Nullable
  List<DataSegment> nextSegments();

  /**
   * Return a map of (dataSource, number of remaining segments) for all dataSources.
   * This method should consider all segments except the segments returned by {@link #nextSegments()}.
   */
  Object2LongOpenHashMap<String> remainingSegments();
}
