/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.actions;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class TaskLocks
{
  static void checkLockCoversSegments(
      final Task task,
      final TaskLockbox taskLockbox,
      final Collection<DataSegment> segments
  )
  {
    if (!isLockCoversSegments(task, taskLockbox, segments)) {
      throw new ISE(
          "Segments[%s] are not covered by locks[%s] for task[%s]",
          segments,
          taskLockbox.findLocksForTask(task),
          task.getId()
      );
    }
  }

  @VisibleForTesting
  static boolean isLockCoversSegments(
      final Task task,
      final TaskLockbox taskLockbox,
      final Collection<DataSegment> segments
  )
  {
    // Verify that each of these segments falls under some lock

    // NOTE: It is possible for our lock to be revoked (if the task has failed and given up its locks) after we check
    // NOTE: it and before we perform the segment insert, but, that should be OK since the worst that happens is we
    // NOTE: insert some segments from the task but not others.

    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = getTaskLockMap(taskLockbox, task);
    if (taskLockMap.isEmpty()) {
      return false;
    }

    return isLockCoversSegments(taskLockMap, segments);
  }

  public static String defaultLockVersion(TaskLockType lockType)
  {
    return lockType == TaskLockType.APPEND
           ? DateTimes.EPOCH.toString()
           : DateTimes.nowUtc().toString();
  }

  /**
   * Checks if the segments are covered by a non revoked lock
   * @param taskLockMap task locks for a task
   * @param segments segments to be checked
   * @return true if each of the segments is covered by a non-revoked lock
   */
  public static boolean isLockCoversSegments(
      NavigableMap<DateTime, List<TaskLock>> taskLockMap,
      Collection<DataSegment> segments
  )
  {
    return segments.stream().allMatch(
        segment -> {
          final Entry<DateTime, List<TaskLock>> entry = taskLockMap.floorEntry(segment.getInterval().getStart());
          if (entry == null) {
            return false;
          }

          // taskLockMap may contain revoked locks which need to be filtered
          final List<TaskLock> locks = entry.getValue()
                                            .stream()
                                            .filter(lock -> !lock.isRevoked())
                                            .collect(Collectors.toList());
          return locks.stream().anyMatch(
              lock -> {
                if (lock.getGranularity() == LockGranularity.TIME_CHUNK) {
                  final TimeChunkLock timeChunkLock = (TimeChunkLock) lock;
                  return timeChunkLock.getInterval().contains(segment.getInterval())
                         && timeChunkLock.getDataSource().equals(segment.getDataSource())
                         && (timeChunkLock.getVersion().compareTo(segment.getVersion()) >= 0
                             || TaskLockType.APPEND.equals(timeChunkLock.getType()));
                  // APPEND locks always have the version DateTimes.EPOCH (1970-01-01)
                  // and cover the segments irrespective of the segment version
                } else {
                  final SegmentLock segmentLock = (SegmentLock) lock;
                  return segmentLock.getInterval().contains(segment.getInterval())
                         && segmentLock.getDataSource().equals(segment.getDataSource())
                         && segmentLock.getVersion().compareTo(segment.getVersion()) >= 0
                         && segmentLock.getPartitionId() == segment.getShardSpec().getPartitionNum();
                }
              }
          );
        }
    );
  }

  /**
   * Determines the type of time chunk lock to use for appending segments.
   * <p>
   * This method should be de-duplicated with {@link AbstractBatchIndexTask#determineLockType}
   * by passing the ParallelIndexSupervisorTask instance into the
   * SinglePhaseParallelIndexTaskRunner.
   */
  public static TaskLockType determineLockTypeForAppend(
      Map<String, Object> taskContext
  )
  {
    final boolean useConcurrentLocks = (boolean) taskContext.getOrDefault(
        Tasks.USE_CONCURRENT_LOCKS,
        Tasks.DEFAULT_USE_CONCURRENT_LOCKS
    );
    if (useConcurrentLocks) {
      return TaskLockType.APPEND;
    }
    final Object lockType = taskContext.get(Tasks.TASK_LOCK_TYPE);
    if (lockType == null) {
      final boolean useSharedLock = (boolean) taskContext.getOrDefault(Tasks.USE_SHARED_LOCK, false);
      return useSharedLock ? TaskLockType.SHARED : TaskLockType.EXCLUSIVE;
    } else {
      return TaskLockType.valueOf(lockType.toString());
    }
  }

  /**
   * Finds locks of type {@link TaskLockType#REPLACE} for each of the given segments
   * that have an interval completely covering the interval of the respective segments.
   *
   * @return Map from segment to REPLACE lock that completely covers it. The map
   * does not contain an entry for segments that have no covering REPLACE lock.
   */
  public static Map<DataSegment, ReplaceTaskLock> findReplaceLocksCoveringSegments(
      final String datasource,
      final TaskLockbox taskLockbox,
      final Set<DataSegment> segments
  )
  {
    // Identify unique segment intervals
    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    segments.forEach(
        segment -> intervalToSegments.computeIfAbsent(
            segment.getInterval(), interval -> new ArrayList<>()
        ).add(segment)
    );

    final Set<ReplaceTaskLock> replaceLocks = taskLockbox.getAllReplaceLocksForDatasource(datasource);
    final Map<DataSegment, ReplaceTaskLock> segmentToReplaceLock = new HashMap<>();

    intervalToSegments.forEach((interval, segmentsInInterval) -> {
      // For each interval, find the lock that covers it, if any
      for (ReplaceTaskLock lock : replaceLocks) {
        if (lock.getInterval().contains(interval)) {
          segmentsInInterval.forEach(s -> segmentToReplaceLock.put(s, lock));
          return;
        }
      }
    });

    return segmentToReplaceLock;
  }

  public static List<TaskLock> findLocksForSegments(
      final Task task,
      final TaskLockbox taskLockbox,
      final Collection<DataSegment> segments
  )
  {
    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = getTaskLockMap(taskLockbox, task);
    if (taskLockMap.isEmpty()) {
      return Collections.emptyList();
    }

    final List<TaskLock> found = new ArrayList<>();
    segments.forEach(segment -> {
      final Entry<DateTime, List<TaskLock>> entry = taskLockMap.floorEntry(segment.getInterval().getStart());
      if (entry == null) {
        throw new ISE("Can't find lock for the interval of segment[%s]", segment.getId());
      }

      final List<TaskLock> locks = entry.getValue();
      locks.forEach(lock -> {
        if (lock.getGranularity() == LockGranularity.TIME_CHUNK) {
          final TimeChunkLock timeChunkLock = (TimeChunkLock) lock;
          if (timeChunkLock.getInterval().contains(segment.getInterval())
              && timeChunkLock.getDataSource().equals(segment.getDataSource())
              && timeChunkLock.getVersion().compareTo(segment.getVersion()) >= 0) {
            found.add(lock);
          }
        } else {
          final SegmentLock segmentLock = (SegmentLock) lock;
          if (segmentLock.getInterval().contains(segment.getInterval())
              && segmentLock.getDataSource().equals(segment.getDataSource())
              && segmentLock.getVersion().compareTo(segment.getVersion()) >= 0
              && segmentLock.getPartitionId() == segment.getShardSpec().getPartitionNum()) {
            found.add(lock);
          }
        }
      });
    });
    return found;
  }

  private static NavigableMap<DateTime, List<TaskLock>> getTaskLockMap(TaskLockbox taskLockbox, Task task)
  {
    final List<TaskLock> taskLocks = taskLockbox.findLocksForTask(task);
    final NavigableMap<DateTime, List<TaskLock>> taskLockMap = new TreeMap<>();
    taskLocks.forEach(taskLock -> taskLockMap.computeIfAbsent(taskLock.getInterval().getStart(), k -> new ArrayList<>())
                                             .add(taskLock));
    return taskLockMap;
  }
}
