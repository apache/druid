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
import com.google.common.collect.ImmutableSet;
import io.druid.indexing.common.task.IndexTaskUtils;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.CriticalAction;
import io.druid.java.util.common.ISE;
import io.druid.java.util.emitter.service.ServiceMetricEvent;
import io.druid.query.DruidMetrics;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SegmentNukeAction implements TaskAction<Void>
{
  @JsonIgnore
  private final Set<DataSegment> segments;

  @JsonCreator
  public SegmentNukeAction(
      @JsonProperty("segments") Set<DataSegment> segments
  )
  {
    this.segments = ImmutableSet.copyOf(segments);
  }

  @JsonProperty
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @Override
  public TypeReference<Void> getReturnTypeReference()
  {
    return new TypeReference<Void>()
    {
    };
  }

  @Override
  public Void perform(Task task, TaskActionToolbox toolbox)
  {
    TaskActionPreconditions.checkLockCoversSegments(task, toolbox.getTaskLockbox(), segments);

    final List<Interval> intervals = segments.stream().map(DataSegment::getInterval).collect(Collectors.toList());

    try {
      toolbox.getTaskLockbox().doInCriticalSection(
          task,
          intervals,
          CriticalAction.builder()
                        .onValidLocks(
                            () -> {
                              toolbox.getIndexerMetadataStorageCoordinator().deleteSegments(segments);
                              return null;
                            }
                        )
                        .onInvalidLocks(
                            () -> {
                              throw new ISE("Some locks for task[%s] are already revoked", task.getId());
                            }
                        )
                        .build()
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Emit metrics
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);

    for (DataSegment segment : segments) {
      metricBuilder.setDimension(DruidMetrics.INTERVAL, segment.getInterval().toString());
      toolbox.getEmitter().emit(metricBuilder.build("segment/nuked/bytes", segment.getSize()));
    }

    return null;
  }

  @Override
  public boolean isAudited()
  {
    return true;
  }

  @Override
  public String toString()
  {
    return "SegmentNukeAction{" +
           "segments=" + segments +
           '}';
  }
}
