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

package org.apache.druid.indexing.overlord;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.SegmentLock;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateRequest;
import org.apache.druid.indexing.common.actions.SegmentAllocateResult;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Remembers which activeTasks have locked which intervals or which segments. Tasks are permitted to lock an interval
 * or a segment if no other task outside their group has locked an overlapping interval for the same datasource or
 * the same segments. Note that TaskLockbox is also responsible for allocating segmentIds when a task requests to lock
 * a new segment. Task lock might involve version assignment.
 *
 * - When a task locks an interval or a new segment, it is assigned a new version string that it can use to publish
 *   segments.
 * - When a task locks a existing segment, it doesn't need to be assigned a new version.
 *
 * Note that tasks of higher priorities can revoke locks of tasks of lower priorities.
 */
public class TaskLockbox
{
  // Datasource -> startTime -> Interval -> list of (Tasks + TaskLock)
  // Multiple shared locks can be acquired for the same dataSource and interval.
  // Note that revoked locks are also maintained in this map to notify that those locks are revoked to the callers when
  // they acquire the same locks again.
  // Also, the key of the second inner map is the start time to find all intervals properly starting with the same
  // startTime.
  private final Map<String, NavigableMap<DateTime, SortedMap<Interval, List<TaskLockPosse>>>> running = new HashMap<>();

  private final TaskStorage taskStorage;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final ReentrantLock giant = new ReentrantLock(true);
  private final Condition lockReleaseCondition = giant.newCondition();

  private static final EmittingLogger log = new EmittingLogger(TaskLockbox.class);

  // Stores List of Active Tasks. TaskLockbox will only grant locks to active activeTasks.
  // this set should be accessed under the giant lock.
  private final Set<String> activeTasks = new HashSet<>();

  @Inject
  public TaskLockbox(
      TaskStorage taskStorage,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator
  )
  {
    this.taskStorage = taskStorage;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
  }

  /**
   * Wipe out our current in-memory state and resync it from our bundled {@link TaskStorage}.
   *
   * @return SyncResult which needs to be processed by the caller
   */
  public TaskLockboxSyncResult syncFromStorage()
  {
    giant.lock();

    try {
      // Load stuff from taskStorage first. If this fails, we don't want to lose all our locks.
      final Set<Task> storedActiveTasks = new HashSet<>();
      final List<Pair<Task, TaskLock>> storedLocks = new ArrayList<>();
      for (final Task task : taskStorage.getActiveTasks()) {
        storedActiveTasks.add(task);
        for (final TaskLock taskLock : taskStorage.getLocks(task.getId())) {
          storedLocks.add(Pair.of(task, taskLock));
        }
      }
      // Sort locks by version, so we add them back in the order they were acquired.
      final Ordering<Pair<Task, TaskLock>> byVersionOrdering = new Ordering<Pair<Task, TaskLock>>()
      {
        @Override
        public int compare(Pair<Task, TaskLock> left, Pair<Task, TaskLock> right)
        {
          // The second compare shouldn't be necessary, but, whatever.
          return ComparisonChain.start()
                                .compare(left.rhs.getVersion(), right.rhs.getVersion())
                                .compare(left.lhs.getId(), right.lhs.getId())
                                .result();
        }
      };
      running.clear();
      activeTasks.clear();
      activeTasks.addAll(storedActiveTasks.stream()
                                          .map(Task::getId)
                                          .collect(Collectors.toSet())
      );
      // Set of task groups in which at least one task failed to re-acquire a lock
      final Set<String> failedToReacquireLockTaskGroups = new HashSet<>();
      // Bookkeeping for a log message at the end
      int taskLockCount = 0;
      for (final Pair<Task, TaskLock> taskAndLock : byVersionOrdering.sortedCopy(storedLocks)) {
        final Task task = Preconditions.checkNotNull(taskAndLock.lhs, "task");
        final TaskLock savedTaskLock = Preconditions.checkNotNull(taskAndLock.rhs, "savedTaskLock");
        if (savedTaskLock.getInterval().toDurationMillis() <= 0) {
          // "Impossible", but you never know what crazy stuff can be restored from storage.
          log.warn("Ignoring lock[%s] with empty interval for task: %s", savedTaskLock, task.getId());
          continue;
        }

        // Create a new taskLock if it doesn't have a proper priority,
        // so that every taskLock in memory has the priority.
        final TaskLock savedTaskLockWithPriority = savedTaskLock.getPriority() == null
                                      ? savedTaskLock.withPriority(task.getPriority())
                                      : savedTaskLock;

        final TaskLockPosse taskLockPosse = verifyAndCreateOrFindLockPosse(
            task,
            savedTaskLockWithPriority
        );
        if (taskLockPosse != null) {
          final TaskLock taskLock = taskLockPosse.getTaskLock();

          if (savedTaskLockWithPriority.getVersion().equals(taskLock.getVersion())) {
            taskLockCount++;
            log.info(
                "Reacquired lock[%s] for task: %s",
                taskLock,
                task.getId()
            );
          } else {
            taskLockCount++;
            log.info(
                "Could not reacquire lock on interval[%s] version[%s] (got version[%s] instead) for task: %s",
                savedTaskLockWithPriority.getInterval(),
                savedTaskLockWithPriority.getVersion(),
                taskLock.getVersion(),
                task.getId()
            );
          }
        } else {
          failedToReacquireLockTaskGroups.add(task.getGroupId());
          log.error(
              "Could not reacquire lock on interval[%s] version[%s] for task: %s from group %s.",
              savedTaskLockWithPriority.getInterval(),
              savedTaskLockWithPriority.getVersion(),
              task.getId(),
              task.getGroupId()
          );
        }
      }

      Set<Task> tasksToFail = new HashSet<>();
      for (Task task : storedActiveTasks) {
        if (failedToReacquireLockTaskGroups.contains(task.getGroupId())) {
          tasksToFail.add(task);
          activeTasks.remove(task.getId());
        }
      }

      log.info(
          "Synced %,d locks for %,d activeTasks from storage (%,d locks ignored).",
          taskLockCount,
          activeTasks.size(),
          storedLocks.size() - taskLockCount
      );

      if (!failedToReacquireLockTaskGroups.isEmpty()) {
        log.warn("Marking all tasks from task groups[%s] to be failed "
                 + "as they failed to reacquire at least one lock.", failedToReacquireLockTaskGroups);
      }

      return new TaskLockboxSyncResult(tasksToFail);
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * This method is called only in {@link #syncFromStorage()} and verifies the given task and the taskLock have the same
   * groupId, dataSource, and priority.
   */
  @VisibleForTesting
  @Nullable
  protected TaskLockPosse verifyAndCreateOrFindLockPosse(Task task, TaskLock taskLock)
  {
    giant.lock();

    try {
      Preconditions.checkArgument(
          task.getGroupId().equals(taskLock.getGroupId()),
          "lock groupId[%s] is different from task groupId[%s]",
          taskLock.getGroupId(),
          task.getGroupId()
      );
      Preconditions.checkArgument(
          task.getDataSource().equals(taskLock.getDataSource()),
          "lock dataSource[%s] is different from task dataSource[%s]",
          taskLock.getDataSource(),
          task.getDataSource()
      );
      final int taskPriority = task.getPriority();
      final int lockPriority = taskLock.getNonNullPriority();

      Preconditions.checkArgument(
          lockPriority == taskPriority,
          "lock priority[%s] is different from task priority[%s]",
          lockPriority,
          taskPriority
      );

      final LockRequest request;
      switch (taskLock.getGranularity()) {
        case SEGMENT:
          final SegmentLock segmentLock = (SegmentLock) taskLock;
          request = new SpecificSegmentLockRequest(
              segmentLock.getType(),
              segmentLock.getGroupId(),
              segmentLock.getDataSource(),
              segmentLock.getInterval(),
              segmentLock.getVersion(),
              segmentLock.getPartitionId(),
              taskPriority,
              segmentLock.isRevoked()
          );
          break;
        case TIME_CHUNK:
          final TimeChunkLock timeChunkLock = (TimeChunkLock) taskLock;
          request = new TimeChunkLockRequest(
              timeChunkLock.getType(),
              timeChunkLock.getGroupId(),
              timeChunkLock.getDataSource(),
              timeChunkLock.getInterval(),
              timeChunkLock.getVersion(),
              taskPriority,
              timeChunkLock.isRevoked()
          );
          break;
        default:
          throw new ISE("Unknown lockGranularity[%s]", taskLock.getGranularity());
      }

      return createOrFindLockPosse(request, task, false);
    }
    catch (Exception e) {
      log.error(e,
                "Could not reacquire lock for task: %s from metadata store", task.getId()
      );
      return null;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Acquires a lock on behalf of a task.  Blocks until the lock is acquired.
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public LockResult lock(final Task task, final LockRequest request) throws InterruptedException
  {
    giant.lockInterruptibly();
    try {
      LockResult lockResult;
      while (!(lockResult = tryLock(task, request)).isOk()) {
        if (lockResult.isRevoked()) {
          return lockResult;
        }
        lockReleaseCondition.await();
      }
      return lockResult;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Acquires a lock on behalf of a task, waiting up to the specified wait time if necessary.
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   *
   * @throws InterruptedException if the current thread is interrupted
   */
  public LockResult lock(final Task task, final LockRequest request, long timeoutMs) throws InterruptedException
  {
    long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    giant.lockInterruptibly();
    try {
      LockResult lockResult;
      while (!(lockResult = tryLock(task, request)).isOk()) {
        if (nanos <= 0 || lockResult.isRevoked()) {
          return lockResult;
        }
        nanos = lockReleaseCondition.awaitNanos(nanos);
      }
      return lockResult;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Attempt to acquire a lock for a task, without removing it from the queue. Can safely be called multiple times on
   * the same task until the lock is preempted.
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   *
   * @throws IllegalStateException if the task is not a valid active task
   */
  public LockResult tryLock(final Task task, final LockRequest request)
  {
    giant.lock();

    try {
      if (!activeTasks.contains(task.getId())) {
        throw new ISE("Unable to grant lock to inactive Task [%s]", task.getId());
      }
      Preconditions.checkArgument(request.getInterval().toDurationMillis() > 0, "interval empty");

      SegmentIdWithShardSpec newSegmentId = null;
      final LockRequest convertedRequest;
      if (request instanceof LockRequestForNewSegment) {
        final LockRequestForNewSegment lockRequestForNewSegment = (LockRequestForNewSegment) request;
        if (lockRequestForNewSegment.getGranularity() == LockGranularity.SEGMENT) {
          newSegmentId = allocateSegmentId(lockRequestForNewSegment, request.getVersion());
          if (newSegmentId == null) {
            return LockResult.fail();
          }
          convertedRequest = new SpecificSegmentLockRequest(lockRequestForNewSegment, newSegmentId);
        } else {
          convertedRequest = new TimeChunkLockRequest(lockRequestForNewSegment);
        }
      } else {
        convertedRequest = request;
      }

      final TaskLockPosse posseToUse = createOrFindLockPosse(convertedRequest, task, true);
      if (posseToUse != null && !posseToUse.getTaskLock().isRevoked()) {
        if (request instanceof LockRequestForNewSegment) {
          final LockRequestForNewSegment lockRequestForNewSegment = (LockRequestForNewSegment) request;
          if (lockRequestForNewSegment.getGranularity() == LockGranularity.TIME_CHUNK) {
            if (newSegmentId != null) {
              throw new ISE(
                  "SegmentId must be allocated after getting a timeChunk lock,"
                  + " but we already have [%s] before getting the lock?",
                  newSegmentId
              );
            }
            newSegmentId = allocateSegmentId(lockRequestForNewSegment, posseToUse.getTaskLock().getVersion());
          }
        }
        return LockResult.ok(posseToUse.getTaskLock(), newSegmentId);
      } else {
        final boolean lockRevoked = posseToUse != null && posseToUse.getTaskLock().isRevoked();
        if (lockRevoked) {
          return LockResult.revoked(posseToUse.getTaskLock());
        }
        return LockResult.fail();
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Attempts to allocate segments for the given requests. Each request contains
   * a {@link Task} and a {@link SegmentAllocateAction}. This method tries to
   * acquire the task locks on the required intervals/segments and then performs
   * a batch allocation of segments. It is possible that some requests succeed
   * successfully and others failed. In that case, only the failed ones should be
   * retried.
   *
   * @param requests                List of allocation requests
   * @param dataSource              Datasource for which segment is to be allocated.
   * @param interval                Interval for which segment is to be allocated.
   * @param skipSegmentLineageCheck Whether lineage check is to be skipped
   *                                (this is true for streaming ingestion)
   * @param lockGranularity         Granularity of task lock
   * @return List of allocation results in the same order as the requests.
   */
  public List<SegmentAllocateResult> allocateSegments(
      List<SegmentAllocateRequest> requests,
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      LockGranularity lockGranularity
  )
  {
    log.info("Allocating [%d] segments for datasource [%s], interval [%s]", requests.size(), dataSource, interval);
    final boolean isTimeChunkLock = lockGranularity == LockGranularity.TIME_CHUNK;

    final AllocationHolderList holderList = new AllocationHolderList(requests, interval);
    holderList.getPending().forEach(this::verifyTaskIsActive);

    giant.lock();
    try {
      if (isTimeChunkLock) {
        // For time-chunk locking, segment must be allocated only after acquiring the lock
        holderList.getPending().forEach(holder -> acquireTaskLock(holder, true));
        allocateSegmentIds(dataSource, interval, skipSegmentLineageCheck, holderList.getPending());
      } else {
        allocateSegmentIds(dataSource, interval, skipSegmentLineageCheck, holderList.getPending());
        holderList.getPending().forEach(holder -> acquireTaskLock(holder, false));
      }
      holderList.getPending().forEach(SegmentAllocationHolder::markSucceeded);
    }
    finally {
      giant.unlock();
    }

    return holderList.getResults();
  }

  /**
   * Marks the segment allocation as failed if the underlying task is not active.
   */
  private void verifyTaskIsActive(SegmentAllocationHolder holder)
  {
    final String taskId = holder.task.getId();
    if (!activeTasks.contains(taskId)) {
      holder.markFailed("Unable to grant lock to inactive Task [%s]", taskId);
    }
  }

  /**
   * Creates a task lock request and creates or finds the lock for that request.
   * Marks the segment allocation as failed if the lock could not be acquired or
   * was revoked.
   */
  private void acquireTaskLock(SegmentAllocationHolder holder, boolean isTimeChunkLock)
  {
    final LockRequest lockRequest;
    if (isTimeChunkLock) {
      lockRequest = new TimeChunkLockRequest(holder.lockRequest);
    } else {
      lockRequest = new SpecificSegmentLockRequest(holder.lockRequest, holder.allocatedSegment);
    }

    // Create or find the task lock for the created lock request
    final TaskLockPosse posseToUse = createOrFindLockPosse(lockRequest, holder.task, true);
    final TaskLock acquiredLock = posseToUse == null ? null : posseToUse.getTaskLock();
    if (posseToUse == null) {
      holder.markFailed("Could not find or create lock posse.");
    } else if (acquiredLock.isRevoked()) {
      holder.markFailed("Lock was revoked.");
    } else {
      holder.setAcquiredLock(posseToUse, lockRequest.getInterval());
    }
  }

  private TaskLockPosse createOrFindLockPosse(LockRequest request, Task task, boolean persist)
  {
    Preconditions.checkState(!(request instanceof LockRequestForNewSegment), "Can't handle LockRequestForNewSegment");

    giant.lock();

    try {
      final TaskLockPosse posseToUse;
      final List<TaskLockPosse> foundPosses = findLockPossesOverlapsInterval(
          request.getDataSource(),
          request.getInterval()
      );

      final List<TaskLockPosse> conflictPosses = foundPosses
          .stream()
          .filter(taskLockPosse -> taskLockPosse.getTaskLock().conflict(request))
          .collect(Collectors.toList());

      if (!conflictPosses.isEmpty()) {
        // If we have some locks for dataSource and interval, check they can be reused.
        // If they can't be reused, check lock priority and revoke existing locks if possible.
        final List<TaskLockPosse> reusablePosses = foundPosses
            .stream()
            .filter(posse -> posse.reusableFor(request))
            .collect(Collectors.toList());

        if (reusablePosses.isEmpty()) {
          // case 1) this task doesn't have any lock, but others do

          if ((request.getType().equals(TaskLockType.APPEND) || request.getType().equals(TaskLockType.REPLACE))
              && !request.getGranularity().equals(LockGranularity.TIME_CHUNK)) {
            // APPEND and REPLACE locks are specific to time chunk locks
            return null;
          }

          // First, check if the lock can coexist with its conflicting posses
          // If not, revoke all lower priority locks of different types if the request has a greater priority
          if (canLockCoexist(conflictPosses, request)
              || revokeAllIncompatibleActiveLocksIfPossible(conflictPosses, request)) {
            posseToUse = createNewTaskLockPosse(request);
          } else {
            // During a rolling update, tasks of mixed versions can be run at the same time. Old tasks would request
            // timeChunkLocks while new tasks would ask segmentLocks. The below check is to allow for old and new tasks
            // to get locks of different granularities if they have the same groupId.
            final boolean allDifferentGranularity = conflictPosses
                .stream()
                .allMatch(
                    conflictPosse -> conflictPosse.taskLock.getGranularity() != request.getGranularity()
                                     && conflictPosse.getTaskLock().getGroupId().equals(request.getGroupId())
                                     && conflictPosse.getTaskLock().getInterval().equals(request.getInterval())
                );
            if (allDifferentGranularity) {
              // Lock collision was because of the different granularity in the same group.
              // We can add a new taskLockPosse.
              posseToUse = createNewTaskLockPosse(request);
            } else {
              log.info(
                  "Cannot create a new taskLockPosse for request[%s] because existing locks[%s] have same or higher priorities",
                  request,
                  conflictPosses
              );
              return null;
            }
          }
        } else if (reusablePosses.size() == 1) {
          // case 2) we found a lock posse for the given request
          posseToUse = reusablePosses.get(0);
        } else {
          // case 3) we found multiple lock posses for the given task
          throw new ISE(
              "Task group[%s] has multiple locks for the same interval[%s]?",
              request.getGroupId(),
              request.getInterval()
          );
        }
      } else {
        // We don't have any locks for dataSource and interval.
        // Let's make a new one.
        posseToUse = createNewTaskLockPosse(request);
      }
      if (posseToUse == null || posseToUse.getTaskLock() == null) {
        return null;
      }
      // Add to existing TaskLockPosse
      if (posseToUse.addTask(task)) {
        log.info("Added task[%s] to TaskLock[%s]", task.getId(), posseToUse.getTaskLock());

        // If the task lock can be used instead of the conflicing posses, their locks can be released
        for (TaskLockPosse conflictPosse : conflictPosses) {
          if (conflictPosse.containsTask(task) && posseToUse.supersedes(conflictPosse)) {
            unlock(task, conflictPosse.getTaskLock().getInterval());
          }
        }

        if (persist) {
          // Update task storage facility. If it fails, unlock it
          try {
            taskStorage.addLock(task.getId(), posseToUse.getTaskLock());
          }
          catch (Exception e) {
            log.makeAlert("Failed to persist lock in storage")
               .addData("task", task.getId())
               .addData("dataSource", posseToUse.getTaskLock().getDataSource())
               .addData("interval", posseToUse.getTaskLock().getInterval())
               .addData("version", posseToUse.getTaskLock().getVersion())
               .emit();
            unlock(
                task,
                posseToUse.getTaskLock().getInterval(),
                posseToUse.getTaskLock().getGranularity() == LockGranularity.SEGMENT
                ? ((SegmentLock) posseToUse.taskLock).getPartitionId()
                : null
            );
            return null;
          }
        }

      } else {
        log.info("Task[%s] already present in TaskLock[%s]", task.getId(), posseToUse.getTaskLock().getGroupId());
      }
      return posseToUse;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Create a new {@link TaskLockPosse} for a new {@link TaskLock}. This method will attempt to assign version strings
   * that obey the invariant that every version string is lexicographically greater than any other version string
   * previously assigned to the same interval. This invariant is only mostly guaranteed, however; we assume clock
   * monotonicity and that callers specifying {@code preferredVersion} are doing the right thing.
   *
   * @param request request to lock
   * @return a new {@link TaskLockPosse}
   */
  private TaskLockPosse createNewTaskLockPosse(LockRequest request)
  {
    giant.lock();
    try {
      final TaskLockPosse posseToUse = new TaskLockPosse(request.toLock());
      running.computeIfAbsent(request.getDataSource(), k -> new TreeMap<>())
             .computeIfAbsent(
                 request.getInterval().getStart(),
                 k -> new TreeMap<>(Comparators.intervalsByStartThenEnd())
             )
             .computeIfAbsent(request.getInterval(), k -> new ArrayList<>())
             .add(posseToUse);

      return posseToUse;
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Makes a call to the {@link #metadataStorageCoordinator} to allocate segments
   * for the given requests. Updates the holder with the allocated segment if
   * the allocation succeeds, otherwise marks it as failed.
   */
  @VisibleForTesting
  void allocateSegmentIds(
      String dataSource,
      Interval interval,
      boolean skipSegmentLineageCheck,
      Collection<SegmentAllocationHolder> holders
  )
  {
    if (holders.isEmpty()) {
      return;
    }

    final List<SegmentCreateRequest> createRequests =
        holders.stream()
               .map(SegmentAllocationHolder::getSegmentRequest)
               .collect(Collectors.toList());

    Map<SegmentCreateRequest, SegmentIdWithShardSpec> allocatedSegments =
        metadataStorageCoordinator.allocatePendingSegments(
            dataSource,
            interval,
            skipSegmentLineageCheck,
            createRequests
        );

    for (SegmentAllocationHolder holder : holders) {
      SegmentIdWithShardSpec segmentId = allocatedSegments.get(holder.getSegmentRequest());
      if (segmentId == null) {
        holder.markFailed("Storage coordinator could not allocate segment.");
      } else {
        holder.setAllocatedSegment(segmentId);
      }
    }
  }

  private SegmentIdWithShardSpec allocateSegmentId(LockRequestForNewSegment request, String version)
  {
    return metadataStorageCoordinator.allocatePendingSegment(
        request.getDataSource(),
        request.getSequenceName(),
        request.getPreviousSegmentId(),
        request.getInterval(),
        request.getPartialShardSpec(),
        version,
        request.isSkipSegmentLineageCheck()
    );
  }

  /**
   * Perform the given action with a guarantee that the locks of the task are not revoked in the middle of action.  This
   * method first checks that all locks for the given task and intervals are valid and perform the right action.
   * <p>
   * The given action should be finished as soon as possible because all other methods in this class are blocked until
   * this method is finished.
   *
   * @param task      task performing a critical action
   * @param intervals intervals
   * @param action    action to be performed inside of the critical section
   */
  public <T> T doInCriticalSection(Task task, Set<Interval> intervals, CriticalAction<T> action) throws Exception
  {
    giant.lock();

    try {
      return action.perform(isTaskLocksValid(task, intervals));
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Check all locks task acquired are still valid.
   * It doesn't check other semantics like acquired locks are enough to overwrite existing segments.
   * This kind of semantic should be checked in each caller of {@link #doInCriticalSection}.
   */
  private boolean isTaskLocksValid(Task task, Set<Interval> intervals)
  {
    giant.lock();
    try {
      return intervals
          .stream()
          .allMatch(interval -> {
            final List<TaskLockPosse> lockPosses = getOnlyTaskLockPosseContainingInterval(task, interval);
            return lockPosses.stream().map(TaskLockPosse::getTaskLock).noneMatch(
                TaskLock::isRevoked
            );
          });
    }
    finally {
      giant.unlock();
    }
  }

  private void revokeLock(TaskLockPosse lockPosse)
  {
    giant.lock();

    try {
      lockPosse.forEachTask(taskId -> revokeLock(taskId, lockPosse.getTaskLock()));
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Mark the lock as revoked. Note that revoked locks are NOT removed. Instead, they are maintained in {@link #running}
   * and {@link #taskStorage} as the normal locks do. This is to check locks are revoked when they are requested to be
   * acquired and notify to the callers if revoked. Revoked locks are removed by calling
   * {@link #unlock(Task, Interval)}.
   *
   * @param taskId an id of the task holding the lock
   * @param lock   lock to be revoked
   */
  @VisibleForTesting
  public void revokeLock(String taskId, TaskLock lock)
  {
    giant.lock();

    try {
      if (!activeTasks.contains(taskId)) {
        throw new ISE("Cannot revoke lock for inactive task[%s]", taskId);
      }

      final Task task = taskStorage.getTask(taskId).orNull();
      if (task == null) {
        throw new ISE("Cannot revoke lock for unknown task[%s]", taskId);
      }

      log.info("Revoking task lock[%s] for task[%s]", lock, taskId);

      if (lock.isRevoked()) {
        log.warn("TaskLock[%s] is already revoked", lock);
      } else {
        final TaskLock revokedLock = lock.revokedCopy();
        taskStorage.replaceLock(taskId, lock, revokedLock);

        final List<TaskLockPosse> possesHolder = running.get(task.getDataSource())
                                                        .get(lock.getInterval().getStart())
                                                        .get(lock.getInterval());
        final TaskLockPosse foundPosse = possesHolder.stream()
                                                     .filter(posse -> posse.getTaskLock().equals(lock))
                                                     .findFirst()
                                                     .orElseThrow(
                                                         () -> new ISE("Failed to find lock posse for lock[%s]", lock)
                                                     );
        possesHolder.remove(foundPosse);
        possesHolder.add(foundPosse.withTaskLock(revokedLock));
        log.info("Revoked taskLock[%s]", lock);
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return the currently-active locks for some task.
   *
   * @param task task for which to locate locks
   * @return currently-active locks for the given task
   */
  public List<TaskLock> findLocksForTask(final Task task)
  {
    giant.lock();

    try {
      return Lists.transform(findLockPossesForTask(task), TaskLockPosse::getTaskLock);
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Finds the active non-revoked REPLACE locks held by the given task.
   */
  public Set<ReplaceTaskLock> findReplaceLocksForTask(Task task)
  {
    giant.lock();
    try {
      return getNonRevokedReplaceLocks(findLockPossesForTask(task), task.getDataSource());
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Finds all the active non-revoked REPLACE locks for the given datasource.
   */
  public Set<ReplaceTaskLock> getAllReplaceLocksForDatasource(String datasource)
  {
    giant.lock();
    try {
      final NavigableMap<DateTime, SortedMap<Interval, List<TaskLockPosse>>> activeLocks = running.get(datasource);
      if (activeLocks == null) {
        return ImmutableSet.of();
      }

      List<TaskLockPosse> lockPosses
          = activeLocks.values()
                       .stream()
                       .flatMap(map -> map.values().stream())
                       .flatMap(Collection::stream)
                       .collect(Collectors.toList());
      return getNonRevokedReplaceLocks(lockPosses, datasource);
    }
    finally {
      giant.unlock();
    }
  }

  private Set<ReplaceTaskLock> getNonRevokedReplaceLocks(List<TaskLockPosse> posses, String datasource)
  {
    final Set<ReplaceTaskLock> replaceLocks = new HashSet<>();
    for (TaskLockPosse posse : posses) {
      final TaskLock lock = posse.getTaskLock();
      if (lock.isRevoked() || !TaskLockType.REPLACE.equals(posse.getTaskLock().getType())) {
        continue;
      }

      // Replace locks are always held by the supervisor task
      if (posse.taskIds.size() > 1) {
        throw DruidException.defensive(
            "Replace lock[%s] for datasource[%s] is held by multiple tasks[%s]",
            lock, datasource, posse.taskIds
        );
      }

      String supervisorTaskId = posse.taskIds.iterator().next();
      replaceLocks.add(
          new ReplaceTaskLock(supervisorTaskId, lock.getInterval(), lock.getVersion())
      );
    }

    return replaceLocks;
  }

  /**
   * @param lockFilterPolicies Lock filters for the given datasources
   * @return Map from datasource to intervals locked by tasks satisfying the lock filter condititions
   */
  public Map<String, List<Interval>> getLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
  {
    final Map<String, Set<Interval>> datasourceToIntervals = new HashMap<>();

    // Take a lock and populate the maps
    giant.lock();

    try {
      lockFilterPolicies.forEach(
          lockFilter -> {
            final String datasource = lockFilter.getDatasource();
            if (!running.containsKey(datasource)) {
              return;
            }

            final int priority = lockFilter.getPriority();
            final boolean ignoreAppendLocks =
                TaskLockType.REPLACE.name().equals(lockFilter.getContext().get(Tasks.TASK_LOCK_TYPE));

            running.get(datasource).forEach(
                (startTime, startTimeLocks) -> startTimeLocks.forEach(
                    (interval, taskLockPosses) -> taskLockPosses.forEach(
                        taskLockPosse -> {
                          if (taskLockPosse.getTaskLock().isRevoked()) {
                            // do nothing
                          } else if (ignoreAppendLocks
                                     && TaskLockType.APPEND.equals(taskLockPosse.getTaskLock().getType())) {
                            // do nothing
                          } else if (taskLockPosse.getTaskLock().getPriority() == null
                                     || taskLockPosse.getTaskLock().getPriority() < priority) {
                            // do nothing
                          } else {
                            datasourceToIntervals.computeIfAbsent(datasource, k -> new HashSet<>())
                                                 .add(interval);
                          }
                        }
                    )
                )
            );
          }
      );
    }
    finally {
      giant.unlock();
    }

    return datasourceToIntervals.entrySet().stream()
                                .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> new ArrayList<>(entry.getValue())
                                ));
  }

  /**
   * Gets a List of Intervals locked by higher priority tasks for each datasource.
   * Here, Segment Locks are being treated the same as Time Chunk Locks i.e.
   * a Task with a Segment Lock is assumed to lock a whole Interval and not just
   * the corresponding Segment.
   *
   * @param minTaskPriority Minimum task priority for each datasource. Only the
   *                        Intervals that are locked by Tasks with equal or
   *                        higher priority than this are returned. Locked intervals
   *                        for datasources that are not present in this Map are
   *                        not returned.
   * @return Map from Datasource to List of Intervals locked by Tasks that have
   * priority greater than or equal to the {@code minTaskPriority} for that datasource.
   */
  public Map<String, List<Interval>> getLockedIntervals(Map<String, Integer> minTaskPriority)
  {
    final Map<String, Set<Interval>> datasourceToIntervals = new HashMap<>();

    // Take a lock and populate the maps
    giant.lock();
    try {
      running.forEach(
          (datasource, datasourceLocks) -> {
            // If this datasource is not requested, do not proceed
            if (!minTaskPriority.containsKey(datasource)) {
              return;
            }

            datasourceLocks.forEach(
                (startTime, startTimeLocks) -> startTimeLocks.forEach(
                    (interval, taskLockPosses) -> taskLockPosses.forEach(
                        taskLockPosse -> {
                          if (taskLockPosse.getTaskLock().isRevoked()) {
                            // Do not proceed if the lock is revoked
                            return;
                          } else if (taskLockPosse.getTaskLock().getPriority() == null
                                     || taskLockPosse.getTaskLock().getPriority() < minTaskPriority.get(datasource)) {
                            // Do not proceed if the lock has a priority strictly less than the minimum
                            return;
                          }

                          datasourceToIntervals
                              .computeIfAbsent(datasource, k -> new HashSet<>())
                              .add(interval);
                        })
                )
            );
          }
      );
    }
    finally {
      giant.unlock();
    }

    return datasourceToIntervals.entrySet().stream()
                                .collect(Collectors.toMap(
                                    Map.Entry::getKey,
                                    entry -> new ArrayList<>(entry.getValue())
                                ));
  }

  public void unlock(final Task task, final Interval interval)
  {
    unlock(task, interval, null);
  }

  /**
   * Release lock held for a task on a particular interval. Does nothing if the task does not currently
   * hold the mentioned lock.
   *
   * @param task     task to unlock
   * @param interval interval to unlock
   */
  public void unlock(final Task task, final Interval interval, @Nullable Integer partitionId)
  {
    giant.lock();

    try {
      final String dataSource = task.getDataSource();
      final NavigableMap<DateTime, SortedMap<Interval, List<TaskLockPosse>>> dsRunning = running.get(
          task.getDataSource()
      );

      if (dsRunning == null || dsRunning.isEmpty()) {
        return;
      }

      final SortedMap<Interval, List<TaskLockPosse>> intervalToPosses = dsRunning.get(interval.getStart());

      if (intervalToPosses == null || intervalToPosses.isEmpty()) {
        return;
      }

      final List<TaskLockPosse> possesHolder = intervalToPosses.get(interval);
      if (possesHolder == null || possesHolder.isEmpty()) {
        return;
      }

      final List<TaskLockPosse> posses = possesHolder.stream()
                                                     .filter(posse -> posse.containsTask(task))
                                                     .collect(Collectors.toList());

      for (TaskLockPosse taskLockPosse : posses) {
        final TaskLock taskLock = taskLockPosse.getTaskLock();

        final boolean match = (partitionId == null && taskLock.getGranularity() == LockGranularity.TIME_CHUNK)
                              || (partitionId != null
                                  && taskLock.getGranularity() == LockGranularity.SEGMENT
                                  && ((SegmentLock) taskLock).getPartitionId() == partitionId);

        if (match) {
          // Remove task from live list
          log.info("Removing task[%s] from TaskLock[%s]", task.getId(), taskLock);
          final boolean removed = taskLockPosse.removeTask(task);

          if (taskLockPosse.isTasksEmpty()) {
            log.info("TaskLock is now empty: %s", taskLock);
            possesHolder.remove(taskLockPosse);
          }

          if (possesHolder.isEmpty()) {
            intervalToPosses.remove(interval);
          }

          if (intervalToPosses.isEmpty()) {
            dsRunning.remove(interval.getStart());
          }

          if (running.get(dataSource).isEmpty()) {
            running.remove(dataSource);
          }

          // Wake up blocking-lock waiters
          lockReleaseCondition.signalAll();

          // Remove lock from storage. If it cannot be removed, just ignore the failure.
          try {
            taskStorage.removeLock(task.getId(), taskLock);
          }
          catch (Exception e) {
            log.makeAlert(e, "Failed to clean up lock from storage")
               .addData("task", task.getId())
               .addData("dataSource", taskLock.getDataSource())
               .addData("interval", taskLock.getInterval())
               .addData("version", taskLock.getVersion())
               .emit();
          }

          if (!removed) {
            log.makeAlert("Lock release without acquire")
               .addData("task", task.getId())
               .addData("interval", interval)
               .emit();
          }
        }
      }
    }
    finally {
      giant.unlock();
    }
  }

  public void unlockAll(Task task)
  {
    giant.lock();
    try {
      for (final TaskLockPosse taskLockPosse : findLockPossesForTask(task)) {
        unlock(
            task,
            taskLockPosse.getTaskLock().getInterval(),
            taskLockPosse.getTaskLock().getGranularity() == LockGranularity.SEGMENT
            ? ((SegmentLock) taskLockPosse.taskLock).getPartitionId()
            : null
        );
      }
    }
    finally {
      giant.unlock();
    }
  }

  public void add(Task task)
  {
    giant.lock();
    try {
      log.info("Adding task[%s] to activeTasks", task.getId());
      activeTasks.add(task.getId());
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Release all locks for a task and remove task from set of active tasks. Does nothing if the task is not currently locked or not an active task.
   *
   * @param task task to unlock
   */
  public void remove(final Task task)
  {
    giant.lock();
    try {
      try {
        log.info("Removing task[%s] from activeTasks", task.getId());
        if (findLocksForTask(task).stream().anyMatch(lock -> lock.getType() == TaskLockType.REPLACE)) {
          final int upgradeSegmentsDeleted = metadataStorageCoordinator.deleteUpgradeSegmentsForTask(task.getId());
          log.info(
              "Deleted [%d] entries from upgradeSegments table for task[%s] with REPLACE locks.",
              upgradeSegmentsDeleted,
              task.getId()
          );
        }
        unlockAll(task);
      }
      finally {
        activeTasks.remove(task.getId());
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return the currently-active lock posses for some task.
   *
   * @param task task for which to locate locks
   */
  private List<TaskLockPosse> findLockPossesForTask(final Task task)
  {
    giant.lock();

    try {
      // Scan through all locks for this datasource
      final NavigableMap<DateTime, SortedMap<Interval, List<TaskLockPosse>>> dsRunning = running.get(task.getDataSource());
      if (dsRunning == null) {
        return ImmutableList.of();
      } else {
        return dsRunning.values().stream()
                        .flatMap(map -> map.values().stream())
                        .flatMap(Collection::stream)
                        .filter(taskLockPosse -> taskLockPosse.containsTask(task))
                        .collect(Collectors.toList());
      }
    }
    finally {
      giant.unlock();
    }
  }

  private List<TaskLockPosse> findLockPossesContainingInterval(final String dataSource, final Interval interval)
  {
    giant.lock();

    try {
      final List<TaskLockPosse> intervalOverlapsPosses = findLockPossesOverlapsInterval(dataSource, interval);
      return intervalOverlapsPosses.stream()
                                   .filter(taskLockPosse -> taskLockPosse.taskLock.getInterval().contains(interval))
                                   .collect(Collectors.toList());
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Return all locks that overlap some search interval.
   */
  private List<TaskLockPosse> findLockPossesOverlapsInterval(final String dataSource, final Interval interval)
  {
    giant.lock();

    try {
      final NavigableMap<DateTime, SortedMap<Interval, List<TaskLockPosse>>> dsRunning = running.get(dataSource);
      if (dsRunning == null) {
        // No locks at all
        return Collections.emptyList();
      } else {
        return dsRunning.navigableKeySet().stream()
                            .filter(java.util.Objects::nonNull)
                            .map(dsRunning::get)
                            .filter(java.util.Objects::nonNull)
                            .flatMap(sortedMap -> sortedMap.entrySet().stream())
                            .filter(entry -> entry.getKey().overlaps(interval))
                            .flatMap(entry -> entry.getValue().stream())
                            .collect(Collectors.toList());
      }
    }
    finally {
      giant.unlock();
    }
  }

  @VisibleForTesting
  List<TaskLockPosse> getOnlyTaskLockPosseContainingInterval(Task task, Interval interval)
  {
    giant.lock();
    try {
      return getOnlyTaskLockPosseContainingInterval(task, interval, Collections.emptySet());
    }
    finally {
      giant.unlock();
    }
  }

  @VisibleForTesting
  List<TaskLockPosse> getOnlyTaskLockPosseContainingInterval(Task task, Interval interval, Set<Integer> partitionIds)
  {
    giant.lock();
    try {
      final List<TaskLockPosse> filteredPosses = findLockPossesContainingInterval(task.getDataSource(), interval)
          .stream()
          .filter(lockPosse -> lockPosse.containsTask(task))
          .collect(Collectors.toList());

      if (filteredPosses.isEmpty()) {
        throw new ISE("Cannot find locks for task[%s] and interval[%s]", task.getId(), interval);
      } else if (filteredPosses.size() > 1) {
        if (filteredPosses.stream()
                          .anyMatch(posse -> posse.getTaskLock().getGranularity() == LockGranularity.TIME_CHUNK)) {
          throw new ISE(
              "There are multiple timeChunk lockPosses for task[%s] and interval[%s]?",
              task.getId(),
              interval
          );
        } else {
          final Map<Integer, TaskLockPosse> partitionIdsOfLocks = new HashMap<>();
          for (TaskLockPosse posse : filteredPosses) {
            final SegmentLock segmentLock = (SegmentLock) posse.getTaskLock();
            partitionIdsOfLocks.put(segmentLock.getPartitionId(), posse);
          }

          if (partitionIds.stream().allMatch(partitionIdsOfLocks::containsKey)) {
            return partitionIds.stream().map(partitionIdsOfLocks::get).collect(Collectors.toList());
          } else {
            throw new ISE(
                "Task[%s] doesn't have locks for interval[%s] partitions[%]",
                task.getId(),
                interval,
                partitionIds.stream().filter(pid -> !partitionIdsOfLocks.containsKey(pid)).collect(Collectors.toList())
            );
          }
        }
      } else {
        return filteredPosses;
      }
    }
    finally {
      giant.unlock();
    }
  }

  @VisibleForTesting
  Set<String> getActiveTasks()
  {
    return activeTasks;
  }

  @VisibleForTesting
  Map<String, NavigableMap<DateTime, SortedMap<Interval, List<TaskLockPosse>>>> getAllLocks()
  {
    return running;
  }

  /**
   * Check if the lock for a given request can coexist with a given set of conflicting posses without any revocation.
   * @param conflictPosses conflict lock posses
   * @param request lock request
   * @return true iff the lock can coexist with all its conflicting locks
   */
  private boolean canLockCoexist(List<TaskLockPosse> conflictPosses, LockRequest request)
  {
    switch (request.getType()) {
      case APPEND:
        return canAppendLockCoexist(conflictPosses, request);
      case REPLACE:
        return canReplaceLockCoexist(conflictPosses, request);
      case SHARED:
        return canSharedLockCoexist(conflictPosses);
      case EXCLUSIVE:
        return canExclusiveLockCoexist(conflictPosses);
      default:
        throw new UOE("Unsupported lock type: " + request.getType());
    }
  }

  /**
   * Check if an APPEND lock can coexist with a given set of conflicting posses.
   * An APPEND lock can coexist with any number of other APPEND locks
   *    OR with at most one REPLACE lock over an interval which encloes this request.
   * @param conflictPosses conflicting lock posses
   * @param appendRequest append lock request
   * @return true iff append lock can coexist with all its conflicting locks
   */
  private boolean canAppendLockCoexist(List<TaskLockPosse> conflictPosses, LockRequest appendRequest)
  {
    TaskLock replaceLock = null;
    for (TaskLockPosse posse : conflictPosses) {
      if (posse.getTaskLock().isRevoked()) {
        continue;
      }
      if (posse.getTaskLock().getType().equals(TaskLockType.EXCLUSIVE)
          || posse.getTaskLock().getType().equals(TaskLockType.SHARED)) {
        return false;
      }
      if (posse.getTaskLock().getType().equals(TaskLockType.REPLACE)) {
        if (replaceLock != null) {
          return false;
        }
        replaceLock = posse.getTaskLock();
        if (!replaceLock.getInterval().contains(appendRequest.getInterval())) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Check if a REPLACE lock can coexist with a given set of conflicting posses.
   * A REPLACE lock can coexist with any number of other APPEND locks and revoked locks
   * @param conflictPosses conflicting lock posses
   * @param replaceLock replace lock request
   * @return true iff replace lock can coexist with all its conflicting locks
   */
  private boolean canReplaceLockCoexist(List<TaskLockPosse> conflictPosses, LockRequest replaceLock)
  {
    for (TaskLockPosse posse : conflictPosses) {
      if (posse.getTaskLock().isRevoked()) {
        continue;
      }
      if (posse.getTaskLock().getType().equals(TaskLockType.EXCLUSIVE)
          || posse.getTaskLock().getType().equals(TaskLockType.SHARED)
          || posse.getTaskLock().getType().equals(TaskLockType.REPLACE)) {
        return false;
      }
      if (posse.getTaskLock().getType().equals(TaskLockType.APPEND)
          && !replaceLock.getInterval().contains(posse.getTaskLock().getInterval())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if a SHARED lock can coexist with a given set of conflicting posses.
   * A SHARED lock can coexist with any number of other active SHARED locks
   * @param conflictPosses conflicting lock posses
   * @return true iff shared lock can coexist with all its conflicting locks
   */
  private boolean canSharedLockCoexist(List<TaskLockPosse> conflictPosses)
  {
    for (TaskLockPosse posse : conflictPosses) {
      if (posse.getTaskLock().isRevoked()) {
        continue;
      }
      if (posse.getTaskLock().getType().equals(TaskLockType.EXCLUSIVE)
          || posse.getTaskLock().getType().equals(TaskLockType.APPEND)
          || posse.getTaskLock().getType().equals(TaskLockType.REPLACE)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Check if an EXCLUSIVE lock can coexist with a given set of conflicting posses.
   * An EXCLUSIVE lock cannot coexist with any other overlapping active locks
   * @param conflictPosses conflicting lock posses
   * @return true iff the exclusive lock can coexist with all its conflicting locks
   */
  private boolean canExclusiveLockCoexist(List<TaskLockPosse> conflictPosses)
  {
    for (TaskLockPosse posse : conflictPosses) {
      if (posse.getTaskLock().isRevoked()) {
        continue;
      }
      return false;
    }
    return true;
  }


  /**
   * Verify if every incompatible active lock is revokable. If yes, revoke all of them.
   * - EXCLUSIVE locks are incompatible with every other conflicting lock
   * - SHARED locks are incompatible with conflicting locks of every other type
   * - REPLACE locks are incompatible with every conflicting lock which is not (APPEND and enclosed) within its interval
   * - APPEND locks are incompatible with every EXCLUSIVE and SHARED lock.
   *   Conflicting REPLACE locks which don't enclose its interval are also incompatible.
   * @param conflictPosses conflicting lock posses
   * @param request lock request
   * @return true iff every incompatible lock is revocable.
   */
  private boolean revokeAllIncompatibleActiveLocksIfPossible(
      List<TaskLockPosse> conflictPosses,
      LockRequest request
  )
  {
    final int priority = request.getPriority();
    final TaskLockType type = request.getType();
    final List<TaskLockPosse> possesToRevoke = new ArrayList<>();

    for (TaskLockPosse posse : conflictPosses) {
      if (posse.getTaskLock().isRevoked()) {
        continue;
      }
      switch (type) {
        case EXCLUSIVE:
          if (posse.getTaskLock().getNonNullPriority() >= priority) {
            return false;
          }
          possesToRevoke.add(posse);
          break;
        case SHARED:
          if (!posse.getTaskLock().getType().equals(TaskLockType.SHARED)) {
            if (posse.getTaskLock().getNonNullPriority() >= priority) {
              return false;
            }
            possesToRevoke.add(posse);
          }
          break;
        case REPLACE:
          if (!(posse.getTaskLock().getType().equals(TaskLockType.APPEND)
                && request.getInterval().contains(posse.getTaskLock().getInterval()))) {
            if (posse.getTaskLock().getNonNullPriority() >= priority) {
              return false;
            }
            possesToRevoke.add(posse);
          }
          break;
        case APPEND:
          if (!(posse.getTaskLock().getType().equals(TaskLockType.APPEND)
                || (posse.getTaskLock().getType().equals(TaskLockType.REPLACE)
                    && posse.getTaskLock().getInterval().contains(request.getInterval())))) {
            if (posse.getTaskLock().getNonNullPriority() >= priority) {
              return false;
            }
            possesToRevoke.add(posse);
          }
          break;
        default:
          throw new UOE("Unsupported lock type: " + type);
      }
    }
    for (TaskLockPosse revokablePosse : possesToRevoke) {
      revokeLock(revokablePosse);
    }
    return true;
  }

  /**
   * Task locks for tasks of the same groupId
   */
  static class TaskLockPosse
  {
    private final TaskLock taskLock;
    private final Set<String> taskIds;

    TaskLockPosse(TaskLock taskLock)
    {
      this.taskLock = taskLock;
      this.taskIds = new HashSet<>();
    }

    private TaskLockPosse(TaskLock taskLock, Set<String> taskIds)
    {
      this.taskLock = taskLock;
      this.taskIds = new HashSet<>(taskIds);
    }

    TaskLockPosse withTaskLock(TaskLock taskLock)
    {
      return new TaskLockPosse(taskLock, taskIds);
    }

    TaskLock getTaskLock()
    {
      return taskLock;
    }

    boolean addTask(Task task)
    {
      if (taskLock.getType() == TaskLockType.EXCLUSIVE) {
        Preconditions.checkArgument(
            taskLock.getGroupId().equals(task.getGroupId()),
            "groupId[%s] of task[%s] is different from the existing lockPosse's groupId[%s]",
            task.getGroupId(),
            task.getId(),
            taskLock.getGroupId()
        );
      }
      Preconditions.checkArgument(
          taskLock.getNonNullPriority() == task.getPriority(),
          "priority[%s] of task[%s] is different from the existing lockPosse's priority[%s]",
          task.getPriority(),
          task.getId(),
          taskLock.getNonNullPriority()
      );
      return taskIds.add(task.getId());
    }

    boolean containsTask(Task task)
    {
      Preconditions.checkNotNull(task, "task");
      return taskIds.contains(task.getId());
    }

    boolean removeTask(Task task)
    {
      Preconditions.checkNotNull(task, "task");
      return taskIds.remove(task.getId());
    }

    boolean isTasksEmpty()
    {
      return taskIds.isEmpty();
    }

    /**
     * Checks if an APPEND time chunk lock can be reused for another append time chunk lock that already exists
     * and has an interval that strictly contains the other's interval
     * We do not expect multiple locks to exist with the same interval as the existing lock would be reused.
     * A new append lock with a strictly encompassing interval can be created when a concurrent replace
     * with a coarser granularity commits its segments and the appending task makes subsequent allocations
     * @param other the conflicting lockPosse that already exists
     * @return true if the task can be unlocked from the other posse after it has been added to the newly created posse.
     */
    boolean supersedes(TaskLockPosse other)
    {
      final TaskLock otherLock = other.taskLock;
      return !taskLock.isRevoked()
             && taskLock.getGranularity() == LockGranularity.TIME_CHUNK
             && taskLock.getGranularity() == otherLock.getGranularity()
             && taskLock.getType() == TaskLockType.APPEND
             && taskLock.getType() == otherLock.getType()
             && taskLock.getVersion().compareTo(otherLock.getVersion()) >= 0
             && !taskLock.getInterval().equals(otherLock.getInterval())
             && taskLock.getInterval().contains(otherLock.getInterval())
             && taskLock.getGroupId().equals(otherLock.getGroupId());
    }

    boolean reusableFor(LockRequest request)
    {
      if (taskLock.getType() == request.getType() && taskLock.getGranularity() == request.getGranularity()) {
        switch (taskLock.getType()) {
          case REPLACE:
          case APPEND:
          case SHARED:
            if (request instanceof TimeChunkLockRequest) {
              return taskLock.getInterval().contains(request.getInterval())
                     && taskLock.getGroupId().equals(request.getGroupId());
            }
            return false;
          case EXCLUSIVE:
            if (request instanceof TimeChunkLockRequest) {
              return taskLock.getInterval().contains(request.getInterval())
                     && taskLock.getGroupId().equals(request.getGroupId());
            } else if (request instanceof SpecificSegmentLockRequest) {
              final SegmentLock segmentLock = (SegmentLock) taskLock;
              final SpecificSegmentLockRequest specificSegmentLockRequest = (SpecificSegmentLockRequest) request;
              return segmentLock.getInterval().contains(specificSegmentLockRequest.getInterval())
                     && segmentLock.getGroupId().equals(specificSegmentLockRequest.getGroupId())
                     && specificSegmentLockRequest.getPartitionId() == segmentLock.getPartitionId();
            } else {
              throw new ISE("Unknown request type[%s]", request);
            }
            //noinspection SuspiciousIndentAfterControlStatement
          default:
            throw new ISE("Unknown lock type[%s]", taskLock.getType());
        }
      }

      return false;
    }

    void forEachTask(Consumer<String> action)
    {
      Preconditions.checkNotNull(action, "action");
      taskIds.forEach(action);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }

      if (o == null || !getClass().equals(o.getClass())) {
        return false;
      }

      TaskLockPosse that = (TaskLockPosse) o;
      return java.util.Objects.equals(taskLock, that.taskLock) &&
             java.util.Objects.equals(taskIds, that.taskIds);
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(taskLock, taskIds);
    }

    @Override
    public String toString()
    {
      return MoreObjects.toStringHelper(this)
                        .add("taskLock", taskLock)
                        .add("taskIds", taskIds)
                        .toString();
    }
  }

  /**
   * Maintains a list of pending allocation holders.
   */
  private static class AllocationHolderList
  {
    final List<SegmentAllocationHolder> all = new ArrayList<>();
    final Set<SegmentAllocationHolder> pending = new HashSet<>();
    final Set<SegmentAllocationHolder> recentlyCompleted = new HashSet<>();

    AllocationHolderList(List<SegmentAllocateRequest> requests, Interval interval)
    {
      for (SegmentAllocateRequest request : requests) {
        SegmentAllocationHolder holder = new SegmentAllocationHolder(request, interval, this);
        all.add(holder);
        pending.add(holder);
      }
    }

    void markCompleted(SegmentAllocationHolder holder)
    {
      recentlyCompleted.add(holder);
    }

    Set<SegmentAllocationHolder> getPending()
    {
      pending.removeAll(recentlyCompleted);
      recentlyCompleted.clear();
      return pending;
    }

    List<SegmentAllocateResult> getResults()
    {
      return all.stream().map(holder -> holder.result).collect(Collectors.toList());
    }
  }

  /**
   * Contains the task, request, lock and final result for a segment allocation.
   */
  @VisibleForTesting
  static class SegmentAllocationHolder
  {
    final AllocationHolderList list;

    final Task task;
    final Interval allocateInterval;
    final SegmentAllocateAction action;
    final LockRequestForNewSegment lockRequest;
    SegmentCreateRequest segmentRequest;

    TaskLock acquiredLock;
    TaskLockPosse taskLockPosse;
    Interval lockRequestInterval;
    SegmentIdWithShardSpec allocatedSegment;
    SegmentAllocateResult result;

    SegmentAllocationHolder(SegmentAllocateRequest request, Interval allocateInterval, AllocationHolderList list)
    {
      this.list = list;
      this.allocateInterval = allocateInterval;
      this.task = request.getTask();
      this.action = request.getAction();

      this.lockRequest = new LockRequestForNewSegment(
          action.getLockGranularity(),
          action.getTaskLockType(),
          task.getGroupId(),
          action.getDataSource(),
          allocateInterval,
          action.getPartialShardSpec(),
          task.getPriority(),
          action.getSequenceName(),
          action.getPreviousSegmentId(),
          action.isSkipSegmentLineageCheck()
      );
    }

    SegmentCreateRequest getSegmentRequest()
    {
      // Initialize the first time this is requested
      if (segmentRequest == null) {
        segmentRequest = new SegmentCreateRequest(
            action.getSequenceName(),
            action.getPreviousSegmentId(),
            acquiredLock == null ? lockRequest.getVersion() : acquiredLock.getVersion(),
            action.getPartialShardSpec()
        );
      }

      return segmentRequest;
    }

    void markFailed(String msgFormat, Object... args)
    {
      list.markCompleted(this);
      result = new SegmentAllocateResult(null, StringUtils.format(msgFormat, args));
    }

    void markSucceeded()
    {
      list.markCompleted(this);
      result = new SegmentAllocateResult(allocatedSegment, null);
    }

    void setAllocatedSegment(SegmentIdWithShardSpec segmentId)
    {
      this.allocatedSegment = segmentId;
    }

    void setAcquiredLock(TaskLockPosse lockPosse, Interval lockRequestInterval)
    {
      this.taskLockPosse = lockPosse;
      this.acquiredLock = lockPosse == null ? null : lockPosse.getTaskLock();
      this.lockRequestInterval = lockRequestInterval;
    }
  }
}
