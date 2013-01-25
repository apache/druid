/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.merger.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wraps a {@link TaskStorage}, providing a useful collection of read-only methods.
 */
public class TaskStorageQueryAdapter
{
  private final TaskStorage storage;

  public TaskStorageQueryAdapter(TaskStorage storage)
  {
    this.storage = storage;
  }

  public Optional<TaskStatus> getStatus(final String taskid)
  {
    return storage.getStatus(taskid);
  }

  /**
   * Returns all recursive task statuses for a particular task from the same task group. Includes that task,
   * plus any tasks it spawned, and so on. Excludes spawned tasks that ended up in a different task group.
   */
  public Map<String, Optional<TaskStatus>> getGroupRecursiveStatuses(final String taskid)
  {
    final Optional<Task> taskOptional = storage.getTask(taskid);
    final Optional<TaskStatus> statusOptional = storage.getStatus(taskid);
    final ImmutableMap.Builder<String, Optional<TaskStatus>> resultBuilder = ImmutableMap.builder();

    resultBuilder.put(taskid, statusOptional);

    if(taskOptional.isPresent() && statusOptional.isPresent()) {
      for(final Task nextTask : statusOptional.get().getNextTasks()) {
        if(nextTask.getGroupId().equals(taskOptional.get().getGroupId())) {
          resultBuilder.putAll(getGroupRecursiveStatuses(nextTask.getId()));
        }
      }
    }

    return resultBuilder.build();
  }

  /**
   * Like {@link #getGroupRecursiveStatuses}, but flattens all recursive statuses for the same task group into a
   * single, merged status.
   */
  public Optional<TaskStatus> getGroupMergedStatus(final String taskid)
  {
    final Map<String, Optional<TaskStatus>> statuses = getGroupRecursiveStatuses(taskid);

    int nSuccesses = 0;
    int nFailures = 0;
    int nTotal = 0;

    final Set<DataSegment> segments = Sets.newHashSet();
    final Set<DataSegment> segmentsNuked = Sets.newHashSet();
    final List<Task> nextTasks = Lists.newArrayList();

    for(final Optional<TaskStatus> statusOption : statuses.values()) {
      nTotal ++;

      if(statusOption.isPresent()) {
        final TaskStatus status = statusOption.get();

        segments.addAll(status.getSegments());
        segmentsNuked.addAll(status.getSegmentsNuked());
        nextTasks.addAll(status.getNextTasks());

        if(status.isSuccess()) {
          nSuccesses ++;
        } else if(status.isFailure()) {
          nFailures ++;
        }
      }
    }

    final Optional<TaskStatus> status;

    if(nTotal == 0) {
      status = Optional.absent();
    } else if(nSuccesses == nTotal) {
      status = Optional.of(TaskStatus.success(taskid)
                         .withSegments(segments)
                         .withSegmentsNuked(segmentsNuked)
                         .withNextTasks(nextTasks));
    } else if(nFailures > 0) {
      status = Optional.of(TaskStatus.failure(taskid));
    } else {
      status = Optional.of(TaskStatus.running(taskid));
    }

    return status;
  }

  /**
   * Returns running tasks along with their preferred versions. If no version is present for a task, the
   * version field of the returned {@link VersionedTaskWrapper} will be null.
   */
  public List<VersionedTaskWrapper> getRunningTaskVersions()
  {
    return Lists.transform(
        storage.getRunningTasks(),
        new Function<Task, VersionedTaskWrapper>()
        {
          @Override
          public VersionedTaskWrapper apply(Task task)
          {
            return new VersionedTaskWrapper(task, storage.getVersion(task.getId()).orNull());
          }
        }
    );
  }
}
