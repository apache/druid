/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.actions.SegmentListUnusedAction;
import io.druid.indexing.common.actions.SegmentMetadataUpdateAction;
import io.druid.timeline.DataSegment;
import java.util.Map;
import org.joda.time.Interval;

import java.util.List;

public class RestoreTask extends AbstractFixedIntervalTask
{
  private static final Logger log = new Logger(RestoreTask.class);

  public RestoreTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        makeId(id, "restore", dataSource, interval),
        dataSource,
        interval,
        context
    );
  }

  @Override
  public String getType()
  {
    return "restore";
  }

  @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      // Confirm we have a lock (will throw if there isn't exactly one element)
      final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));

      if (!myLock.getDataSource().equals(getDataSource())) {
        throw new ISE("WTF?! Lock dataSource[%s] != task dataSource[%s]", myLock.getDataSource(), getDataSource());
      }

      if (!myLock.getInterval().equals(getInterval())) {
        throw new ISE("WTF?! Lock interval[%s] != task interval[%s]", myLock.getInterval(), getInterval());
      }

      // List unused segments
      final List<DataSegment> unusedSegments = toolbox
          .getTaskActionClient()
          .submit(new SegmentListUnusedAction(myLock.getDataSource(), myLock.getInterval()));

      // Verify none of these segments have versions > lock version
      for (final DataSegment unusedSegment : unusedSegments) {
        if (unusedSegment.getVersion().compareTo(myLock.getVersion()) > 0) {
          throw new ISE(
              "WTF?! Unused segment[%s] has version[%s] > task version[%s]",
              unusedSegment.getIdentifier(),
              unusedSegment.getVersion(),
              myLock.getVersion()
          );
        }

        log.info("OK to restore segment: %s", unusedSegment.getIdentifier());
      }

      List<DataSegment> restoredSegments = Lists.newLinkedList();

      // Move segments
      for (DataSegment segment : unusedSegments) {
        restoredSegments.add(toolbox.getDataSegmentArchiver().restore(segment));
      }

      // Update metadata for moved segments
      toolbox.getTaskActionClient().submit(
          new SegmentMetadataUpdateAction(
              ImmutableSet.copyOf(restoredSegments)
          )
      );

      return TaskStatus.success(getId());
    }
}
