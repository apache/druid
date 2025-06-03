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
import com.google.inject.Inject;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.actions.SegmentAllocateAction;
import org.apache.druid.indexing.common.actions.SegmentAllocateRequest;
import org.apache.druid.indexing.common.actions.SegmentAllocateResult;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.metadata.ReplaceTaskLock;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Maintains a {@link TaskLockbox} for each datasource.
 */
public class GlobalTaskLockbox
{
  private static final Logger log = new Logger(GlobalTaskLockbox.class);

  private final TaskStorage taskStorage;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final ConcurrentHashMap<String, TaskLockbox> datasourceToLockbox = new ConcurrentHashMap<>();

  private final AtomicBoolean syncComplete = new AtomicBoolean(false);

  @Inject
  public GlobalTaskLockbox(
      TaskStorage taskStorage,
      IndexerMetadataStorageCoordinator metadataStorageCoordinator
  )
  {
    this.taskStorage = taskStorage;
    this.metadataStorageCoordinator = metadataStorageCoordinator;
  }

  private TaskLockbox getDatasourceLockbox(Task task)
  {
    return getDatasourceLockbox(task.getDataSource());
  }

  private TaskLockbox getDatasourceLockbox(String datasource)
  {
    // Verify that sync is complete
    if (!syncComplete.get()) {
      throw new ISE("Cannot get TaskLockbox for datasource[%s]. Sync with storage is not complete yet.", datasource);
    }

    return datasourceToLockbox.computeIfAbsent(
        datasource,
        ds -> new TaskLockbox(ds, taskStorage, metadataStorageCoordinator)
    );
  }

  /**
   * Result of metadata store sync for a single datasource.
   */
  private static class DatasourceSync
  {
    final Set<Task> storedActiveTasks = new HashSet<>();
    final List<Pair<Task, TaskLock>> storedLocks = new ArrayList<>();
  }

  /**
   * Syncs the current in-memory state with the {@link TaskStorage}.
   * This method should be called only once when the {@link TaskQueue#start()}
   * is invoked.
   *
   * @return SyncResult which needs to be processed by the caller
   */
  public TaskLockboxSyncResult syncFromStorage()
  {
    // Load stuff from taskStorage first. If this fails, we don't want to lose all our locks.
    final Map<String, DatasourceSync> datasourceSyncs = new HashMap<>();
    int activeTaskCount = 0;
    int totalLockCount = 0;
    for (Task task : taskStorage.getActiveTasks()) {
      ++activeTaskCount;
      final DatasourceSync sync = datasourceSyncs.computeIfAbsent(
          task.getDataSource(),
          ds -> new DatasourceSync()
      );
      sync.storedActiveTasks.add(task);
      for (TaskLock taskLock : taskStorage.getLocks(task.getId())) {
        ++totalLockCount;
        sync.storedLocks.add(Pair.of(task, taskLock));
      }
    }

    // Set of task groups in which at least one task failed to re-acquire a lock
    final Set<Task> tasksToFail = new HashSet<>();
    int taskLockCount = 0;

    datasourceToLockbox.clear();
    for (String dataSource : datasourceSyncs.keySet()) {
      final DatasourceSync sync = datasourceSyncs.get(dataSource);
      final TaskLockboxSyncResult result = getDatasourceLockbox(dataSource)
          .resetState(sync.storedActiveTasks, sync.storedLocks);
      tasksToFail.addAll(result.getTasksToFail());
      taskLockCount += result.getTaskLockCount();
    }

    log.info(
        "Synced [%,d] locks for [%,d] activeTasks from storage ([%,d] locks ignored).",
        taskLockCount, activeTaskCount, totalLockCount - taskLockCount
    );

    syncComplete.set(true);
    return new TaskLockboxSyncResult(tasksToFail, taskLockCount);
  }

  /**
   * Acquires a lock on behalf of a task.  Blocks until the lock is acquired.
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   * @throws InterruptedException if the current thread is interrupted
   */
  public LockResult lock(final Task task, final LockRequest request) throws InterruptedException
  {
    return getDatasourceLockbox(task).lock(task, request);
  }

  /**
   * Acquires a lock on behalf of a task, waiting up to the specified wait time if necessary.
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   * @throws InterruptedException if the current thread is interrupted
   */
  public LockResult lock(final Task task, final LockRequest request, long timeoutMs) throws InterruptedException
  {
    return getDatasourceLockbox(task).lock(task, request, timeoutMs);
  }

  /**
   * Attempt to acquire a lock for a task, without removing it from the queue. Can safely be called multiple times on
   * the same task until the lock is preempted.
   *
   * @return {@link LockResult} containing a new or an existing lock if succeeded. Otherwise, {@link LockResult} with a
   * {@link LockResult#revoked} flag.
   * @throws IllegalStateException if the task is not a valid active task
   */
  public LockResult tryLock(final Task task, final LockRequest request)
  {
    return getDatasourceLockbox(task).tryLock(task, request);
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
      LockGranularity lockGranularity,
      boolean reduceMetadataIO
  )
  {
    return getDatasourceLockbox(dataSource).allocateSegments(
        requests,
        dataSource,
        interval,
        skipSegmentLineageCheck,
        lockGranularity,
        reduceMetadataIO
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
   * @param action    action to be performed inside the critical section
   */
  public <T> T doInCriticalSection(Task task, Set<Interval> intervals, CriticalAction<T> action) throws Exception
  {
    return getDatasourceLockbox(task).doInCriticalSection(task, intervals, action);
  }

  /**
   * Mark the lock as revoked. Note that revoked locks are NOT removed. Instead, they are kept in memory
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
    getDatasourceLockbox(lock.getDataSource()).revokeLock(taskId, lock);
  }

  /**
   * Return the currently-active locks for some task.
   *
   * @param task task for which to locate locks
   * @return currently-active locks for the given task
   */
  public List<TaskLock> findLocksForTask(final Task task)
  {
    return getDatasourceLockbox(task).findLocksForTask(task);
  }

  /**
   * Finds the active non-revoked REPLACE locks held by the given task.
   */
  public Set<ReplaceTaskLock> findReplaceLocksForTask(Task task)
  {
    return getDatasourceLockbox(task).findReplaceLocksForTask(task);
  }

  /**
   * Finds all the active non-revoked REPLACE locks for the given datasource.
   */
  public Set<ReplaceTaskLock> getAllReplaceLocksForDatasource(String datasource)
  {
    return getDatasourceLockbox(datasource).getAllReplaceLocksForDatasource(datasource);
  }

  /**
   * @param lockFilterPolicies Lock filters for the given datasources
   * @return Map from datasource to intervals locked by tasks satisfying the lock filter condititions
   */
  public Map<String, List<Interval>> getLockedIntervals(List<LockFilterPolicy> lockFilterPolicies)
  {
    final Map<String, List<LockFilterPolicy>> datasourceToFilterPolicies = new HashMap<>();
    for (LockFilterPolicy policy : lockFilterPolicies) {
      datasourceToFilterPolicies.computeIfAbsent(policy.getDatasource(), ds -> new ArrayList<>())
                                .add(policy);
    }

    final Map<String, List<Interval>> datasourceToLockedIntervals = new HashMap<>();
    datasourceToFilterPolicies.forEach((datasource, policies) -> {
      final List<Interval> lockedIntervals = getDatasourceLockbox(datasource).getLockedIntervals(policies);
      if (!lockedIntervals.isEmpty()) {
        datasourceToLockedIntervals.put(datasource, lockedIntervals);
      }
    });

    return datasourceToLockedIntervals;
  }

  /**
   * @param lockFilterPolicies Lock filters for the given datasources
   * @return Map from datasource to list of non-revoked locks with at least as much priority and an overlapping interval
   */
  public Map<String, List<TaskLock>> getActiveLocks(List<LockFilterPolicy> lockFilterPolicies)
  {
    final Map<String, List<LockFilterPolicy>> datasourceToFilterPolicies = new HashMap<>();
    for (LockFilterPolicy policy : lockFilterPolicies) {
      datasourceToFilterPolicies.computeIfAbsent(policy.getDatasource(), ds -> new ArrayList<>())
                                .add(policy);
    }

    final Map<String, List<TaskLock>> datasourceToActiveLocks = new HashMap<>();
    datasourceToFilterPolicies.forEach((datasource, policies) -> {
      final List<TaskLock> datasourceLocks = getDatasourceLockbox(datasource).getActiveLocks(policies);
      if (!datasourceLocks.isEmpty()) {
        datasourceToActiveLocks.put(datasource, datasourceLocks);
      }
    });

    return datasourceToActiveLocks;
  }

  public void unlock(final Task task, final Interval interval)
  {
    getDatasourceLockbox(task).unlock(task, interval);
  }

  public void unlockAll(Task task)
  {
    getDatasourceLockbox(task).unlockAll(task);
  }

  public void add(Task task)
  {
    getDatasourceLockbox(task).add(task);
  }

  /**
   * Release all locks for a task and remove task from set of active tasks.
   * Does nothing if the task is not currently locked or not an active task.
   *
   * @param task task to unlock
   */
  public void remove(final Task task)
  {
    getDatasourceLockbox(task).remove(task);
  }

  @VisibleForTesting
  Optional<TaskLockbox.TaskLockPosse> getOnlyTaskLockPosseContainingInterval(Task task, Interval interval)
  {
    return getDatasourceLockbox(task).getOnlyTaskLockPosseContainingInterval(task, interval);
  }

  @VisibleForTesting
  Set<String> getActiveTasks()
  {
    final Set<String> allActiveTasks = new HashSet<>();
    datasourceToLockbox.values().forEach(lockbox -> allActiveTasks.addAll(lockbox.getActiveTasks()));

    return allActiveTasks;
  }

  @VisibleForTesting
  Map<String, NavigableMap<DateTime, SortedMap<Interval, List<TaskLockbox.TaskLockPosse>>>> getAllLocks()
  {
    final Map<String, NavigableMap<DateTime, SortedMap<Interval, List<TaskLockbox.TaskLockPosse>>>>
        allLocks = new HashMap<>();

    datasourceToLockbox.forEach((datasource, lockbox) -> {
      final NavigableMap<DateTime, SortedMap<Interval, List<TaskLockbox.TaskLockPosse>>>
          locks = lockbox.getAllLocks();
      if (!locks.isEmpty()) {
        allLocks.put(datasource, locks);
      }
    });

    return allLocks;
  }

}
