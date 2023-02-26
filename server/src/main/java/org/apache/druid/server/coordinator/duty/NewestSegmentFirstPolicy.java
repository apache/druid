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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import java.time.Clock;
import java.util.List;
import java.util.Map;

/**
 * This policy searches segments for compaction from the newest one to oldest one.
 * The {@link #resetIfNeeded} functionality is inspired by {@link com.google.common.base.Suppliers.ExpiringMemoizingSupplier}.
 */
public class NewestSegmentFirstPolicy implements CompactionSegmentSearchPolicy
{
  private final ObjectMapper objectMapper;
  private final long durationMillis;
  private transient volatile NewestSegmentFirstIterator iterator;
  // The special value 0 means "not yet initialized".
  private transient volatile long expirationMillis;
  private final Clock clock;

  @Inject
  public NewestSegmentFirstPolicy(ObjectMapper objectMapper, DruidCoordinatorConfig config, Clock clock)
  {
    this.objectMapper = objectMapper;
    this.durationMillis = config.getCompactionSearchPolicyRefreshPeriod().getMillis();
    this.clock = clock;
    Preconditions.checkArgument(durationMillis > 0);
  }

  @Override
  public Pair<CompactionSegmentIterator, Boolean> resetIfNeeded(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, SegmentTimeline> dataSources,
      Map<String, List<Interval>> skipIntervals
  )
  {
    long millis = expirationMillis;
    long now = clock.millis();
    if (millis == 0 || now - millis >= 0) {
      synchronized (this) {
        if (millis == expirationMillis) {
          NewestSegmentFirstIterator t = reset(compactionConfigs, dataSources, skipIntervals);
          iterator = t;
          // reset can be slow
          expirationMillis = clock.millis() + durationMillis;
          return Pair.of(t, true);
        }
      }
    }
    return Pair.of(iterator, false);
  }

  @Override
  public synchronized NewestSegmentFirstIterator reset(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, SegmentTimeline> dataSources,
      Map<String, List<Interval>> skipIntervals
  )
  {
    NewestSegmentFirstIterator t = new NewestSegmentFirstIterator(
        objectMapper,
        compactionConfigs,
        dataSources,
        skipIntervals
    );
    iterator = t;
    expirationMillis = clock.millis() + durationMillis;
    return t;
  }
}
