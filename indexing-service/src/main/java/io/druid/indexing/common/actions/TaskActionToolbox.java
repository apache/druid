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

package io.druid.indexing.common.actions;

import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.IndexerDBCoordinator;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.timeline.DataSegment;

import java.util.List;
import java.util.Set;

public class TaskActionToolbox
{
  private final TaskLockbox taskLockbox;
  private final IndexerDBCoordinator indexerDBCoordinator;
  private final ServiceEmitter emitter;

  @Inject
  public TaskActionToolbox(
      TaskLockbox taskLockbox,
      IndexerDBCoordinator indexerDBCoordinator,
      ServiceEmitter emitter
  )
  {
    this.taskLockbox = taskLockbox;
    this.indexerDBCoordinator = indexerDBCoordinator;
    this.emitter = emitter;
  }

  public TaskLockbox getTaskLockbox()
  {
    return taskLockbox;
  }

  public IndexerDBCoordinator getIndexerDBCoordinator()
  {
    return indexerDBCoordinator;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public boolean segmentsAreFromSamePartitionSet(
      final Set<DataSegment> segments
  )
  {
    // Verify that these segments are all in the same partition set

    Preconditions.checkArgument(!segments.isEmpty(), "segments nonempty");
    final DataSegment firstSegment = segments.iterator().next();
    for (final DataSegment segment : segments) {
      if (!segment.getDataSource().equals(firstSegment.getDataSource())
          || !segment.getInterval().equals(firstSegment.getInterval())
          || !segment.getVersion().equals(firstSegment.getVersion())) {
        return false;
      }
    }
    return true;
  }

  public void verifyTaskLocksAndSinglePartitionSettitude(
      final Task task,
      final Set<DataSegment> segments,
      final boolean allowOlderVersions
  )
  {
    if (!taskLockCoversSegments(task, segments, allowOlderVersions)) {
      throw new ISE("Segments not covered by locks for task: %s", task.getId());
    }
    if (!segmentsAreFromSamePartitionSet(segments)) {
      throw new ISE("Segments are not in the same partition set: %s", segments);
    }
  }

  public boolean taskLockCoversSegments(
      final Task task,
      final Set<DataSegment> segments,
      final boolean allowOlderVersions
  )
  {
    // Verify that each of these segments falls under some lock

    // NOTE: It is possible for our lock to be revoked (if the task has failed and given up its locks) after we check
    // NOTE: it and before we perform the segment insert, but, that should be OK since the worst that happens is we
    // NOTE: insert some segments from the task but not others.

    final List<TaskLock> taskLocks = getTaskLockbox().findLocksForTask(task);
    for(final DataSegment segment : segments) {
      final boolean ok = Iterables.any(
          taskLocks, new Predicate<TaskLock>()
      {
        @Override
        public boolean apply(TaskLock taskLock)
        {
          final boolean versionOk = allowOlderVersions
                                    ? taskLock.getVersion().compareTo(segment.getVersion()) >= 0
                                    : taskLock.getVersion().equals(segment.getVersion());

          return versionOk
                 && taskLock.getDataSource().equals(segment.getDataSource())
                 && taskLock.getInterval().contains(segment.getInterval());
        }
      }
      );

      if (!ok) {
        return false;
      }
    }

    return true;
  }
}
