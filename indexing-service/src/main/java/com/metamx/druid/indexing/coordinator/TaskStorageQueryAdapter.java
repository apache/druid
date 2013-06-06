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

package com.metamx.druid.indexing.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.actions.SegmentInsertAction;
import com.metamx.druid.indexing.common.actions.SpawnTasksAction;
import com.metamx.druid.indexing.common.actions.TaskAction;
import com.metamx.druid.indexing.common.task.Task;

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
   * Returns all recursive task statuses for a particular task, staying within the same task group. Includes that
   * task, plus any tasks it spawned, and so on. Does not include spawned tasks that ended up in a different task
   * group. Does not include this task's parents or siblings.
   */
  public Map<String, Optional<TaskStatus>> getSameGroupChildStatuses(final String taskid)
  {
    final Optional<Task> taskOptional = storage.getTask(taskid);
    final Optional<TaskStatus> statusOptional = storage.getStatus(taskid);
    final ImmutableMap.Builder<String, Optional<TaskStatus>> resultBuilder = ImmutableMap.builder();

    resultBuilder.put(taskid, statusOptional);

    final Iterable<Task> nextTasks = FunctionalIterable
        .create(storage.getAuditLogs(taskid)).filter(
            new Predicate<TaskAction>()
            {
              @Override
              public boolean apply(TaskAction taskAction)
              {
                return taskAction instanceof SpawnTasksAction;
              }
            }
        ).transformCat(
            new Function<TaskAction, Iterable<Task>>()
            {
              @Override
              public Iterable<Task> apply(TaskAction taskAction)
              {
                return ((SpawnTasksAction) taskAction).getNewTasks();
              }
            }
        );

    if(taskOptional.isPresent() && statusOptional.isPresent()) {
      for(final Task nextTask : nextTasks) {
        if(nextTask.getGroupId().equals(taskOptional.get().getGroupId())) {
          resultBuilder.putAll(getSameGroupChildStatuses(nextTask.getId()));
        }
      }
    }

    return resultBuilder.build();
  }

  /**
   * Like {@link #getSameGroupChildStatuses}, but flattens the recursive statuses into a single, merged status.
   */
  public Optional<TaskStatus> getSameGroupMergedStatus(final String taskid)
  {
    final Map<String, Optional<TaskStatus>> statuses = getSameGroupChildStatuses(taskid);

    int nSuccesses = 0;
    int nFailures = 0;
    int nTotal = 0;
    int nPresent = 0;

    for(final Optional<TaskStatus> statusOption : statuses.values()) {
      nTotal ++;

      if(statusOption.isPresent()) {
        nPresent ++;

        final TaskStatus status = statusOption.get();

        if(status.isSuccess()) {
          nSuccesses ++;
        } else if(status.isFailure()) {
          nFailures ++;
        }
      }
    }

    final Optional<TaskStatus> status;

    if(nPresent == 0) {
      status = Optional.absent();
    } else if(nSuccesses == nTotal) {
      status = Optional.of(TaskStatus.success(taskid));
    } else if(nFailures > 0) {
      status = Optional.of(TaskStatus.failure(taskid));
    } else {
      status = Optional.of(TaskStatus.running(taskid));
    }

    return status;
  }

  /**
   * Returns all segments created by descendants for a particular task that stayed within the same task group. Includes
   * that task, plus any tasks it spawned, and so on. Does not include spawned tasks that ended up in a different task
   * group. Does not include this task's parents or siblings.
   *
   * This method is useful when you want to figure out all of the things a single task spawned.  It does pose issues
   * with the result set perhaps growing boundlessly and we do not do anything to protect against that.  Use at your
   * own risk and know that at some point, we might adjust this to actually enforce some sort of limits.
   */
  public Set<DataSegment> getSameGroupNewSegments(final String taskid)
  {
    final Optional<Task> taskOptional = storage.getTask(taskid);
    final Set<DataSegment> segments = Sets.newHashSet();
    final List<Task> nextTasks = Lists.newArrayList();

    for(final TaskAction action : storage.getAuditLogs(taskid)) {
      if(action instanceof SpawnTasksAction) {
        nextTasks.addAll(((SpawnTasksAction) action).getNewTasks());
      }

      if(action instanceof SegmentInsertAction) {
        segments.addAll(((SegmentInsertAction) action).getSegments());
      }
    }

    if(taskOptional.isPresent()) {
      for(final Task nextTask : nextTasks) {
        if(nextTask.getGroupId().equals(taskOptional.get().getGroupId())) {
          segments.addAll(getSameGroupNewSegments(nextTask.getId()));
        }
      }
    }

    return segments;
  }
}
