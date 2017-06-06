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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import io.druid.common.utils.JodaUtils;
import io.druid.indexing.common.task.Task;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

public class SegmentListUsedAction implements TaskAction<List<DataSegment>>
{
  @JsonIgnore
  private final String dataSource;

  @JsonIgnore
  private final List<Interval> intervals;

  @JsonCreator
  public SegmentListUsedAction(
      @JsonProperty("dataSource") String dataSource,
      @Deprecated @JsonProperty("interval") Interval interval,
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.dataSource = dataSource;

    Preconditions.checkArgument(
        interval == null || intervals == null,
        "please specify intervals only"
    );

    List<Interval> theIntervals = null;
    if (interval != null) {
      theIntervals = ImmutableList.of(interval);
    } else if (intervals != null && intervals.size() > 0) {
      theIntervals = JodaUtils.condenseIntervals(intervals);
    }
    this.intervals = Preconditions.checkNotNull(theIntervals, "no intervals found");
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @Override
  public TypeReference<List<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<List<DataSegment>>() {};
  }

  @Override
  public List<DataSegment> perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    return toolbox.getIndexerMetadataStorageCoordinator().getUsedSegmentsForIntervals(dataSource, intervals);
  }

  @Override
  public boolean isAudited()
  {
    return false;
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

    SegmentListUsedAction that = (SegmentListUsedAction) o;

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    return intervals.equals(that.intervals);

  }

  @Override
  public int hashCode()
  {
    int result = dataSource.hashCode();
    result = 31 * result + intervals.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "SegmentListUsedAction{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           '}';
  }
}
