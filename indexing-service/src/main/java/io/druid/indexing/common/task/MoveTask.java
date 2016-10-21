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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentListUnusedAction;
import io.druid.indexing.common.actions.SegmentMetadataUpdateAction;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

public class MoveTask extends AbstractFixedIntervalTask
{
  private static final Logger log = new Logger(MoveTask.class);

  private final Map<String, Object> targetLoadSpec;

  @JsonCreator
  public MoveTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("target") Map<String, Object> targetLoadSpec,
      @JsonProperty("context") Map<String, Object> context,
      // See https://github.com/druid-io/druid/pull/1922
      @JsonProperty("targetLoadSpec") Map<String, Object> targetLoadSpecCOMPAT
  )
  {
    super(
        makeId(id, "move", dataSource, interval),
        dataSource,
        interval,
        context
    );
    this.targetLoadSpec = targetLoadSpec == null ? targetLoadSpecCOMPAT : targetLoadSpec;
  }

  @Override
  public String getType()
  {
    return "move";
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    // Confirm we have a lock (will throw if there isn't exactly one element)
    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));

    if(!myLock.getDataSource().equals(getDataSource())) {
      throw new ISE("WTF?! Lock dataSource[%s] != task dataSource[%s]", myLock.getDataSource(), getDataSource());
    }

    if(!myLock.getInterval().equals(getInterval())) {
      throw new ISE("WTF?! Lock interval[%s] != task interval[%s]", myLock.getInterval(), getInterval());
    }

    // List unused segments
    final List<DataSegment> unusedSegments = toolbox
        .getTaskActionClient()
        .submit(new SegmentListUnusedAction(myLock.getDataSource(), myLock.getInterval()));

    // Verify none of these segments have versions > lock version
    for(final DataSegment unusedSegment : unusedSegments) {
      if(unusedSegment.getVersion().compareTo(myLock.getVersion()) > 0) {
        throw new ISE(
            "WTF?! Unused segment[%s] has version[%s] > task version[%s]",
            unusedSegment.getIdentifier(),
            unusedSegment.getVersion(),
            myLock.getVersion()
        );
      }

      log.info("OK to move segment: %s", unusedSegment.getIdentifier());
    }

    // Move segments
    for (DataSegment segment : unusedSegments) {
      final DataSegment movedSegment = toolbox.getDataSegmentMover().move(segment, targetLoadSpec);
      toolbox.getTaskActionClient().submit(new SegmentMetadataUpdateAction(ImmutableSet.of(movedSegment)));
    }

    return TaskStatus.success(getId());
  }

  @JsonProperty("target")
  public Map<String, Object> getTargetLoadSpec()
  {
    return targetLoadSpec;
  }
}
