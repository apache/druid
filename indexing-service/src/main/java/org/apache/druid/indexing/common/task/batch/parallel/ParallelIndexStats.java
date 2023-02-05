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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.Pair;
import org.joda.time.Interval;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class ParallelIndexStats
{
  private final Map<String, Object> rowStats;
  private final Map<String, Object> unparseableEvents;
  private final Set<Interval> ingestedIntervals;

  public ParallelIndexStats()
  {
    this.rowStats = ImmutableMap.of();
    this.unparseableEvents = ImmutableMap.of();
    this.ingestedIntervals = ImmutableSet.of();
  }

  public ParallelIndexStats(
      Map<String, Object> rowStats,
      Map<String, Object> unparseableEvents,
      Set<Interval> ingestedIntervals
  )
  {
    this.rowStats = rowStats;
    this.unparseableEvents = unparseableEvents;
    this.ingestedIntervals = ingestedIntervals;
  }

  public Pair<Map<String, Object>, Map<String, Object>> toRowStatsAndUnparseableEvents()
  {
    return Pair.of(rowStats, unparseableEvents);
  }

  public Map<String, Object> getRowStats()
  {
    return rowStats;
  }

  public Map<String, Object> getUnparseableEvents()
  {
    return unparseableEvents;
  }

  public Set<Interval> getIngestedIntervals()
  {
    return ingestedIntervals;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ParallelIndexStats that = (ParallelIndexStats) o;
    return rowStats.equals(that.rowStats)
           && unparseableEvents.equals(that.unparseableEvents)
           && ingestedIntervals.equals(
        that.ingestedIntervals);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(rowStats, unparseableEvents, ingestedIntervals);
  }
}
