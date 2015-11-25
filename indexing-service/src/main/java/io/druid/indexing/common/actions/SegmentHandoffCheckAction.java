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
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.OverlordServerView;
import io.druid.query.SegmentDescriptor;
import io.druid.query.TableDataSource;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.TimelineObjectHolder;
import io.druid.timeline.VersionedIntervalTimeline;

import java.io.IOException;
import java.util.List;
import java.util.Set;


public class SegmentHandoffCheckAction implements TaskAction<Boolean>
{
  private static final Logger log = new Logger(SegmentHandoffCheckAction.class);
  @JsonIgnore
  private final SegmentDescriptor segmentDescriptor;

  @JsonCreator
  public SegmentHandoffCheckAction(@JsonProperty("segmentDescriptor") SegmentDescriptor segmentDescriptor)
  {
    Preconditions.checkNotNull(segmentDescriptor, "segmentDescriptor");
    this.segmentDescriptor = segmentDescriptor;
  }

  @JsonProperty("segmentDescriptor")
  public SegmentDescriptor getSegmentDescriptor()
  {
    return segmentDescriptor;
  }

  @Override
  public TypeReference<Boolean> getReturnTypeReference()
  {
    return new TypeReference<Boolean>()
    {
    };
  }

  @Override
  public Boolean perform(
      Task task, TaskActionToolbox toolbox
  ) throws IOException
  {
    String dataSource = task.getDataSource();
    OverlordServerView serverView = toolbox.getServerView();
    VersionedIntervalTimeline<String, Set<DruidServerMetadata>> timeline = serverView.getTimeline(
        new TableDataSource(
            dataSource
        )
    );
    if (timeline == null) {
      log.debug("No timeline found for datasource[%s]", dataSource);
      return false;
    }

    List<TimelineObjectHolder<String, Set<DruidServerMetadata>>> lookup = timeline.lookup(
        segmentDescriptor.getInterval(),
        true
    );

    for (TimelineObjectHolder<String, Set<DruidServerMetadata>> timelineObjectHolder : lookup) {
      if (timelineObjectHolder.getInterval().contains(segmentDescriptor.getInterval()) &&
          timelineObjectHolder.getVersion().compareTo(segmentDescriptor.getVersion()) >= 0 &&
          timelineObjectHolder.getObject().getChunk(segmentDescriptor.getPartitionNumber()) != null &&
          Iterables.any(
              timelineObjectHolder.getObject().getChunk(segmentDescriptor.getPartitionNumber()).getObject(),
              new Predicate<DruidServerMetadata>()
              {
                @Override
                public boolean apply(DruidServerMetadata input)
                {
                  return input.isAssignable();
                }
              }
          )) {

        return true;
      }
    }
    return false;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentHandoffCheckAction{" +
           "segmentDescriptor=" + segmentDescriptor +
           '}';
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

    SegmentHandoffCheckAction that = (SegmentHandoffCheckAction) o;

    return segmentDescriptor.equals(that.segmentDescriptor);

  }

  @Override
  public int hashCode()
  {
    return segmentDescriptor.hashCode();
  }
}
