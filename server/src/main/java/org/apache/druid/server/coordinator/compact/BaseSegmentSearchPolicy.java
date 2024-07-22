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

package org.apache.druid.server.coordinator.compact;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.timeline.SegmentTimeline;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public abstract class BaseSegmentSearchPolicy implements CompactionSegmentSearchPolicy
{
  private final String priorityDatasource;

  protected BaseSegmentSearchPolicy(
      @Nullable String priorityDatasource
  )
  {
    this.priorityDatasource = priorityDatasource;
  }

  @Nullable
  @JsonProperty
  public final String getPriorityDatasource()
  {
    return priorityDatasource;
  }

  @Override
  public CompactionSegmentIterator createIterator(
      Map<String, DataSourceCompactionConfig> compactionConfigs,
      Map<String, SegmentTimeline> dataSources,
      Map<String, List<Interval>> skipIntervals,
      CompactionStatusTracker statusTracker
  )
  {
    return new PriorityBasedCompactionSegmentIterator(
        compactionConfigs,
        dataSources,
        skipIntervals,
        getPriorityDatasource(),
        getSegmentComparator(),
        statusTracker
    );
  }

  protected abstract Comparator<SegmentsToCompact> getSegmentComparator();

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BaseSegmentSearchPolicy that = (BaseSegmentSearchPolicy) o;
    return Objects.equals(priorityDatasource, that.priorityDatasource);
  }

  @Override
  public int hashCode()
  {
    return Objects.hashCode(priorityDatasource);
  }
}
