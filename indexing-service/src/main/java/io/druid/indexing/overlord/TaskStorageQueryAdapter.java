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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.SegmentInsertAction;
import io.druid.indexing.common.actions.TaskAction;
import io.druid.indexing.common.task.Task;
import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Set;

/**
 * Wraps a {@link TaskStorage}, providing a useful collection of read-only methods.
 */
public class TaskStorageQueryAdapter
{
  private final TaskStorage storage;

  @Inject
  public TaskStorageQueryAdapter(TaskStorage storage)
  {
    this.storage = storage;
  }

  public List<Task> getActiveTasks()
  {
    return storage.getActiveTasks();
  }

  public List<TaskStatus> getRecentlyFinishedTaskStatuses()
  {
    return storage.getRecentlyFinishedTaskStatuses();
  }

  public Optional<Task> getTask(final String taskid)
  {
    return storage.getTask(taskid);
  }

  public Optional<TaskStatus> getStatus(final String taskid)
  {
    return storage.getStatus(taskid);
  }

  /**
   * Returns all segments created by this task.
   *
   * This method is useful when you want to figure out all of the things a single task spawned.  It does pose issues
   * with the result set perhaps growing boundlessly and we do not do anything to protect against that.  Use at your
   * own risk and know that at some point, we might adjust this to actually enforce some sort of limits.
   *
   * @param taskid task ID
   * @return set of segments created by the specified task
   */
  public Set<DataSegment> getInsertedSegments(final String taskid)
  {
    final Set<DataSegment> segments = Sets.newHashSet();
    for (final TaskAction action : storage.getAuditLogs(taskid)) {
      if (action instanceof SegmentInsertAction) {
        segments.addAll(((SegmentInsertAction) action).getSegments());
      }
    }
    return segments;
  }
}
