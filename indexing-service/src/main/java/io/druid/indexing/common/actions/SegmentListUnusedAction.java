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
import io.druid.indexing.common.task.Task;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;

public class SegmentListUnusedAction implements TaskAction<List<DataSegment>>
{
  @JsonIgnore
  private final String dataSource;

  @JsonIgnore
  private final Interval interval;

  @JsonIgnore
  private final DateTime unusedMarkThreshold;

  @JsonCreator
  public SegmentListUnusedAction(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("unusedMarkThreshold") DateTime unusedMarkThreshold
  )
  {
    this.dataSource = dataSource;
    this.interval = interval;
    this.unusedMarkThreshold = unusedMarkThreshold;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public DateTime getUnusedMarkThreshold()
  {
    return unusedMarkThreshold;
  }

  @Override
  public TypeReference<List<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<List<DataSegment>>() {};
  }

  @Override
  public List<DataSegment> perform(Task task, TaskActionToolbox toolbox) throws IOException
  {
    return toolbox.getIndexerMetadataStorageCoordinator().getUnusedSegmentsForInterval(dataSource, interval, unusedMarkThreshold);
  }

  @Override
  public boolean isAudited()
  {
    return false;
  }

  @Override
  public String toString()
  {
    return "SegmentListUnusedAction{" +
           "dataSource='" + dataSource + '\'' +
           ", interval=" + interval +
           ", unusedMarkThreshold=" + unusedMarkThreshold +
           '}';
  }
}
