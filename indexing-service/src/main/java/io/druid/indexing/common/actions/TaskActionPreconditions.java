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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.java.util.common.ISE;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

public class TaskActionPreconditions
{
  static void checkLockCoversSegments(
      final Task task,
      final TaskLockbox taskLockbox,
      final Set<DataSegment> segments
  )
  {
    if (!isLockCoversSegments(task, taskLockbox, segments)) {
      throw new ISE("Segments not covered by locks for task: %s", task.getId());
    }
  }

  @VisibleForTesting
  static boolean isLockCoversSegments(
      final Task task,
      final TaskLockbox taskLockbox,
      final Set<DataSegment> segments
  )
  {
    // Verify that each of these segments falls under some lock

    // NOTE: It is possible for our lock to be revoked (if the task has failed and given up its locks) after we check
    // NOTE: it and before we perform the segment insert, but, that should be OK since the worst that happens is we
    // NOTE: insert some segments from the task but not others.

    final NavigableMap<DateTime, TaskLock> taskLockMap = getTaskLockMap(taskLockbox, task);
    if (taskLockMap.isEmpty()) {
      return false;
    }

    return segments.stream().allMatch(
        segment -> {
          final Entry<DateTime, TaskLock> entry = taskLockMap.floorEntry(segment.getInterval().getStart());
          if (entry == null) {
            return false;
          }

          final TaskLock taskLock = entry.getValue();
          return taskLock.getInterval().contains(segment.getInterval()) &&
                 taskLock.getDataSource().equals(segment.getDataSource()) &&
                 taskLock.getVersion().compareTo(segment.getVersion()) >= 0;
        }
    );
  }

  static void checkLockCoversInterval(
      Task task,
      TaskLockbox taskLockbox,
      String dataSource,
      Interval interval
  )
  {
    checkLockCoversIntervals(task, taskLockbox, dataSource, ImmutableList.of(interval));
  }

  static void checkLockCoversIntervals(
      Task task,
      TaskLockbox taskLockbox,
      String dataSource,
      List<Interval> intervals
  )
  {
    if (!isLockCoversIntervals(task, taskLockbox, dataSource, intervals)) {
      throw new ISE("Intervals not covered by locks for task: %s", task.getId());
    }
  }

  @VisibleForTesting
  static boolean isLockCoversIntervals(
      Task task,
      TaskLockbox taskLockbox,
      String dataSource,
      List<Interval> intervals
  )
  {
    final NavigableMap<DateTime, TaskLock> taskLockMap = getTaskLockMap(taskLockbox, task);
    if (taskLockMap.isEmpty()) {
      return false;
    }

    return intervals.stream().allMatch(
        interval -> {
          final Entry<DateTime, TaskLock> entry = taskLockMap.floorEntry(interval.getStart());
          if (entry == null) {
            return false;
          }

          final TaskLock taskLock = entry.getValue();
          return taskLock.getDataSource().equals(dataSource) &&
                 taskLock.getInterval().contains(interval);
        }
    );
  }

  private static NavigableMap<DateTime, TaskLock> getTaskLockMap(TaskLockbox taskLockbox, Task task)
  {
    final List<TaskLock> taskLocks = taskLockbox.findLocksForTask(task);
    final NavigableMap<DateTime, TaskLock> taskLockMap = new TreeMap<>();
    taskLocks.forEach(taskLock -> taskLockMap.put(taskLock.getInterval().getStart(), taskLock));
    return taskLockMap;
  }
}
