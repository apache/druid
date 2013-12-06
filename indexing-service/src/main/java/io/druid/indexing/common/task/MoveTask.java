/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
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
import io.druid.indexing.common.actions.SegmentMoveAction;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.util.List;

public class MoveTask extends AbstractTask
{
  private static final Logger log = new Logger(MoveTask.class);

  @JsonCreator
  public MoveTask(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    super(
        TaskUtils.makeId(id, "move", dataSource, interval),
        dataSource,
        interval
    );
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

    if(!myLock.getInterval().equals(getImplicitLockInterval().get())) {
      throw new ISE("WTF?! Lock interval[%s] != task interval[%s]", myLock.getInterval(), getImplicitLockInterval().get());
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

    List<DataSegment> movedSegments = Lists.newLinkedList();

    // Move segments
    for (DataSegment segment : unusedSegments) {
      movedSegments.add(toolbox.getDataSegmentMover().move(segment));
    }

    // Update metadata for moved segments
    toolbox.getTaskActionClient().submit(new SegmentMoveAction(
        ImmutableSet.copyOf(movedSegments)
    ));

    return TaskStatus.success(getId());
  }
}
