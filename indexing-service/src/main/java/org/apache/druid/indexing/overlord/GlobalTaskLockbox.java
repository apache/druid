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
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

/**
 * Maintains a {@link TaskLockbox} for each datasource.
 * A {@link TaskLockbox} is used to maintain locks over intervals of a datasource
 * to ensure data consistency while various types of ingestion tasks append,
 * overwrite or delete data.
 */
public class GlobalTaskLockbox
{
  private static final Logger log = new Logger(GlobalTaskLockbox.class);

  private final TaskStorage taskStorage;
  private final IndexerMetadataStorageCoordinator metadataStorageCoordinator;
  private final ConcurrentHashMap<String, DatasourceLockboxResource> datasourceToLockbox = new ConcurrentHashMap<>();

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

  /**
   * Syncs the current in-memory state with the {@link TaskStorage}.
   * This method should be called only from {@link TaskQueue#start()}.
   * If the sync fails, no other operation can be performed on this lockbox.
   *
   * @return SyncResult which needs to be processed by the caller
   */
  public TaskLockboxSyncResult syncFromStorage()
  {
    // Shutdown to reset the state and ensure that no other operation is
    // in progress during the sync
    shutdown();

    // Retrieve all active tasks and locks associated with them
    final Map<String, DatasourceSyncResult> datasourceToSyncResult = new HashMap<>();
    int activeTaskCount = 0;
    int totalLockCount = 0;
    for (Task task : taskStorage.getActiveTasks()) {
      ++activeTaskCount;
      final DatasourceSyncResult result = datasourceToSyncResult.computeIfAbsent(
          task.getDataSource(),
          ds -> new DatasourceSyncResult()
      );
      result.storedActiveTasks.add(task);
      for (TaskLock taskLock : taskStorage.getLocks(task.getId())) {
        ++totalLockCount;
        result.storedLocks.add(Pair.of(task, taskLock));
      }
    }

    // Identify task groups in which at least one task failed to re-acquire a lock
    final Set<Task> tasksToFail = new HashSet<>();
    final AtomicInteger taskLockCount = new AtomicInteger(0);

    datasourceToSyncResult.forEach((dataSource, syncResult) -> {
      try (final DatasourceLockboxResource lockboxResource = getLockboxResource(dataSource)) {
        final TaskLockboxSyncResult lockboxSyncResult = lockboxResource.delegate.resetState(
            syncResult.storedActiveTasks,
            syncResult.storedLocks
        );
        tasksToFail.addAll(lockboxSyncResult.getTasksToFail());
        taskLockCount.addAndGet(lockboxSyncResult.getTaskLockCount());
      }
    });

    log.info(
        "Synced [%,d] locks for [%,d] active tasks from storage ([%,d] locks ignored).",
        taskLockCount.get(), activeTaskCount, totalLockCount - taskLockCount.get()
    );

    syncComplete.set(true);
    return new TaskLockboxSyncResult(tasksToFail, taskLockCount.get());
  }

  /**
   * Clears up the state of the lockbox. Should be called when leadership is lost.
   */
  public void shutdown()
  {
    // Mark sync as incomplete so that no more lockboxes are created
    syncComplete.set(false);

    // Clean up all existing lockboxes
    final Set<String> datasourceNames = Set.copyOf(datasourceToLockbox.keySet());
    for (String dataSource : datasourceNames) {
      cleanupLockboxResourceIf(dataSource, resource -> true);
    }

    log.info(
        "Removed lockboxes for [%d] datasources, [%d] remaining.",
        datasourceNames.size(), datasourceToLockbox.size()
    );
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
    return computeForTask(
        task,
        lockbox -> lockbox.lock(task, request)
    );
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
    return computeForTask(
        task,
        lockbox -> lockbox.lock(task, request, timeoutMs)
    );
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
    return computeForTask(
        task,
        lockbox -> lockbox.tryLock(task, request)
    );
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
    return computeForDatasource(
        dataSource,
        lockbox -> lockbox.allocateSegments(
            requests,
            dataSource,
            interval,
            skipSegmentLineageCheck,
            lockGranularity,
            reduceMetadataIO
        )
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
    return computeForTask(
        task,
        lockbox -> lockbox.doInCriticalSection(task, intervals, action)
    );
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
    computeForDatasource(
        lock.getDataSource(),
        lockbox -> {
          lockbox.revokeLock(taskId, lock);
          return 0;
        }
    );
  }

  /**
   * Cleans up pending segments associated with the given task, if any.
   */
  @VisibleForTesting
  protected void cleanupPendingSegments(Task task)
  {
    executeForTask(
        task,
        lockbox -> lockbox.cleanupPendingSegments(task)
    );
  }

  /**
   * Return the currently-active locks for some task.
   *
   * @param task task for which to locate locks
   * @return currently-active locks for the given task
   */
  public List<TaskLock> findLocksForTask(final Task task)
  {
    return computeForTask(
        task,
        lockbox -> lockbox.findLocksForTask(task)
    );
  }

  /**
   * Finds the active non-revoked REPLACE locks held by the given task.
   */
  public Set<ReplaceTaskLock> findReplaceLocksForTask(Task task)
  {
    return computeForTask(
        task,
        lockbox -> lockbox.findReplaceLocksForTask(task)
    );
  }

  /**
   * Finds all the active non-revoked REPLACE locks for the given datasource.
   */
  public Set<ReplaceTaskLock> getAllReplaceLocksForDatasource(String datasource)
  {
    return computeForDatasource(
        datasource,
        lockbox -> lockbox.getAllReplaceLocksForDatasource(datasource)
    );
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
      final List<Interval> lockedIntervals = computeForDatasource(
          datasource,
          lockbox -> lockbox.getLockedIntervals(policies)
      );
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
      final List<TaskLock> datasourceLocks = computeForDatasource(
          datasource,
          lockbox -> lockbox.getActiveLocks(policies)
      );
      if (!datasourceLocks.isEmpty()) {
        datasourceToActiveLocks.put(datasource, datasourceLocks);
      }
    });

    return datasourceToActiveLocks;
  }

  public void unlock(final Task task, final Interval interval)
  {
    executeForTask(
        task,
        lockbox -> lockbox.unlock(task, interval)
    );
  }

  public void unlockAll(Task task)
  {
    executeForTask(
        task,
        lockbox -> lockbox.unlockAll(task)
    );
  }

  public void add(Task task)
  {
    executeForTask(
        task,
        lockbox -> lockbox.add(task)
    );
  }

  /**
   * Release all locks for a task and remove task from set of active tasks.
   * Does nothing if the task is not currently locked or not an active task.
   *
   * @param task task to unlock
   */
  public void remove(final Task task)
  {
    final boolean isEmpty = computeForTask(
        task,
        lockbox -> {
          lockbox.remove(task);
          return lockbox.isEmpty();
        }
    );

    if (isEmpty) {
      cleanupLockboxResourceIf(task.getDataSource(), resource -> resource.references.get() <= 0);
    }
  }

  @VisibleForTesting
  Optional<TaskLockbox.TaskLockPosse> getOnlyTaskLockPosseContainingInterval(Task task, Interval interval)
  {
    return computeForTask(
        task,
        lockbox -> lockbox.getOnlyTaskLockPosseContainingInterval(task, interval)
    );
  }

  @VisibleForTesting
  Set<String> getActiveTasks()
  {
    final Set<String> allActiveTasks = new HashSet<>();

    final Set<String> datasourceNames = Set.copyOf(datasourceToLockbox.keySet());
    for (String datasource : datasourceNames) {
      allActiveTasks.addAll(
          computeForDatasource(datasource, TaskLockbox::getActiveTasks)
      );
    }

    return allActiveTasks;
  }

  @VisibleForTesting
  Map<String, NavigableMap<DateTime, SortedMap<Interval, List<TaskLockbox.TaskLockPosse>>>> getAllLocks()
  {
    final Map<String, NavigableMap<DateTime, SortedMap<Interval, List<TaskLockbox.TaskLockPosse>>>>
        allLocks = new HashMap<>();

    final Set<String> datasourceNames = Set.copyOf(datasourceToLockbox.keySet());
    for (String datasource : datasourceNames) {
      final NavigableMap<DateTime, SortedMap<Interval, List<TaskLockbox.TaskLockPosse>>>
          datasourceLocks = computeForDatasource(datasource, TaskLockbox::getAllLocks);
      if (!datasourceLocks.isEmpty()) {
        allLocks.put(datasource, datasourceLocks);
      }
    }

    return allLocks;
  }

  /**
   * Gets the {@link DatasourceLockboxResource} for the given datasource and
   * increments the number of references currently in use.
   * This resource must be closed once it is not needed anymore.
   */
  private DatasourceLockboxResource getLockboxResource(String datasource)
  {
    return datasourceToLockbox.compute(
        datasource,
        (ds, existingResource) -> {
          final DatasourceLockboxResource resource = Objects.requireNonNullElseGet(
              existingResource,
              () -> new DatasourceLockboxResource(
                  new TaskLockbox(ds, taskStorage, metadataStorageCoordinator)
              )
          );
          resource.acquireReference();
          return resource;
        }
    );
  }

  /**
   * Cleans up the lockbox for the given datasource if the {@link DatasourceLockboxResource}
   * meets the given criteria.
   */
  private void cleanupLockboxResourceIf(
      String dataSource,
      Predicate<DatasourceLockboxResource> resourcePredicate
  )
  {
    datasourceToLockbox.compute(
        dataSource,
        (ds, resource) -> {
          if (resource != null && resourcePredicate.test(resource)) {
            // TODO: what if a bad runaway operation is holding the TaskLockbox.giant?
            resource.delegate.clear();
            return null;
          } else {
            return resource;
          }
        }
    );
  }

  /**
   * Performs a computation using the {@link TaskLockbox} corresponding to the
   * given datasource and returns the result.
   */
  private <R, T extends Throwable> R computeForDatasource(
      String datasource,
      LockComputation<R, T> computation
  ) throws T
  {
    // Verify that sync is complete
    if (!syncComplete.get()) {
      throw new ISE(
          "Cannot get TaskLockbox for datasource[%s] as sync with storage has not happened yet.",
          datasource
      );
    }

    try (final DatasourceLockboxResource lockbox = getLockboxResource(datasource)) {
      return computation.perform(lockbox.delegate);
    }
  }

  /**
   * Performs a computation using the {@link TaskLockbox} corresponding to the
   * given task and returns the result.
   */
  private <R, T extends Throwable> R computeForTask(
      Task task,
      LockComputation<R, T> computation
  ) throws T
  {
    return computeForDatasource(task.getDataSource(), computation);
  }

  /**
   * Executes an operation on the {@link TaskLockbox} corresponding to the given
   * task.
   */
  private <T extends Throwable> void executeForTask(
      Task task,
      LockOperation<T> operation
  ) throws T
  {
    computeForDatasource(
        task.getDataSource(),
        lockbox -> {
          operation.perform(lockbox);
          return 0;
        }
    );
  }

  @FunctionalInterface
  private interface LockComputation<R, T extends Throwable>
  {
    R perform(TaskLockbox lockbox) throws T;
  }

  @FunctionalInterface
  private interface LockOperation<T extends Throwable>
  {
    void perform(TaskLockbox lockbox) throws T;
  }

  /**
   * Result of metadata store sync for a single datasource.
   */
  private static class DatasourceSyncResult
  {
    final Set<Task> storedActiveTasks = new HashSet<>();
    final List<Pair<Task, TaskLock>> storedLocks = new ArrayList<>();
  }

  /**
   * Wrapper around a {@link TaskLockbox} for a specific datasource which keeps
   * track of the number of active references of this resource that are currently
   * in use.
   */
  private static class DatasourceLockboxResource implements AutoCloseable
  {
    final AtomicInteger references;
    final TaskLockbox delegate;

    DatasourceLockboxResource(TaskLockbox delegate)
    {
      this.delegate = delegate;
      this.references = new AtomicInteger(0);
    }

    void acquireReference()
    {
      references.incrementAndGet();
    }

    @Override
    public void close()
    {
      references.decrementAndGet();
    }
  }

}
