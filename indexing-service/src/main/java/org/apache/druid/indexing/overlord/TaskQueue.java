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
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.thisptr.jackson.jq.internal.misc.Lists;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.MaxAllowedLocksExceededException;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseParallelIndexTaskRunner;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.EntryExistsException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Interface between task producers and the task runner.
 * <p/>
 * This object accepts tasks from producers using {@link #add} and manages delivery of these tasks to a
 * {@link TaskRunner}. Tasks will run in a mostly-FIFO order, with deviations when the natural next task is not ready
 * in time (based on its {@link Task#isReady} method).
 * <p/>
 * For persistence, we save all new tasks and task status changes using a {@link TaskStorage} object.
 */
public class TaskQueue
{
  private static final long MANAGEMENT_WAIT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);
  private static final long MIN_WAIT_TIME_MILLIS = 100;

  private static final EmittingLogger log = new EmittingLogger(TaskQueue.class);

  /**
   * Map from Task ID to active task (submitted, running, recently finished).
   * This must be a ConcurrentHashMap so that operations for any task id happen
   * atomically.
   */
  private final ConcurrentHashMap<String, Task> tasks = new ConcurrentHashMap<>();

  /**
   * Queue of all active Task IDs. Updates to this queue must happen within
   * {@code tasks.compute()} or {@code tasks.computeIfAbsent()} to ensure that
   * this queue is always in sync with the {@code tasks} map.
   */
  private final BlockingDeque<String> activeTaskIdQueue = new LinkedBlockingDeque<>();

  /**
   * Tasks that have already been submitted to the TaskRunner.
   */
  private final Set<String> submittedTaskIds = Sets.newConcurrentHashSet();

  /**
   * Tasks that have recently completed and are being cleaned up. These tasks
   * should not be relaunched by task management.
   */
  private final Set<String> recentlyCompletedTaskIds = Sets.newConcurrentHashSet();

  private final TaskLockConfig lockConfig;
  private final TaskQueueConfig config;
  private final DefaultTaskConfig defaultTaskConfig;
  private final TaskStorage taskStorage;
  private final TaskRunner taskRunner;
  private final TaskActionClientFactory taskActionClientFactory;
  private final TaskLockbox taskLockbox;
  private final ServiceEmitter emitter;

  private final BlockingQueue<String> managementRequestQueue = new ArrayBlockingQueue<>(1);

  private final ExecutorService managerExec = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(false)
          .setNameFormat("TaskQueue-Manager").build()
  );
  private final ScheduledExecutorService storageSyncExec = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder()
          .setDaemon(false)
          .setNameFormat("TaskQueue-StorageSync").build()
  );

  private volatile boolean active = false;

  private final ConcurrentHashMap<String, Long> datasourceToSuccessfulTaskCount = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Long> datasourceToFailedTaskCount = new ConcurrentHashMap<>();

  public TaskQueue(
      TaskLockConfig lockConfig,
      TaskQueueConfig config,
      DefaultTaskConfig defaultTaskConfig,
      TaskStorage taskStorage,
      TaskRunner taskRunner,
      TaskActionClientFactory taskActionClientFactory,
      TaskLockbox taskLockbox,
      ServiceEmitter emitter
  )
  {
    this.lockConfig = Preconditions.checkNotNull(lockConfig, "lockConfig");
    this.config = Preconditions.checkNotNull(config, "config");
    this.defaultTaskConfig = Preconditions.checkNotNull(defaultTaskConfig, "defaultTaskContextConfig");
    this.taskStorage = Preconditions.checkNotNull(taskStorage, "taskStorage");
    this.taskRunner = Preconditions.checkNotNull(taskRunner, "taskRunner");
    this.taskActionClientFactory = Preconditions.checkNotNull(taskActionClientFactory, "taskActionClientFactory");
    this.taskLockbox = Preconditions.checkNotNull(taskLockbox, "taskLockbox");
    this.emitter = Preconditions.checkNotNull(emitter, "emitter");
  }

  /**
   * Starts this task queue. Allows {@link #add(Task)} to accept new tasks.
   */
  @LifecycleStart
  public synchronized void start()
  {
    Preconditions.checkState(!active, "queue must be stopped");

    // Mark queue as active only after first sync is complete
    syncFromStorage();
    active = true;

    // Mark these tasks as failed as they could not reacquire locks
    Set<Task> tasksToFail = taskLockbox.syncFromStorage().getTasksToFail();
    for (Task task : tasksToFail) {
      shutdown(
          task.getId(),
          "Shutting down forcefully as task failed to reacquire lock while becoming leader"
      );
    }
    requestManagement("Starting TaskQueue");
    // Remove any unacquired locks from storage (shutdown only clears entries for which a TaskLockPosse was acquired)
    // This is called after requesting management as locks need to be cleared after notifyStatus is processed
    for (Task task : tasksToFail) {
      for (TaskLock lock : taskStorage.getLocks(task.getId())) {
        taskStorage.removeLock(task.getId(), lock);
      }
    }
    log.info("Cleaned up [%d] tasks which failed to reacquire locks.", tasksToFail.size());

    // Submit task management job
    managerExec.submit(
        () -> {
          log.info("Beginning task management in [%s].", config.getStartDelay());
          long startDelayMillis = config.getStartDelay().getMillis();
          while (active) {
            try {
              Thread.sleep(startDelayMillis);
              runTaskManagement();
            }
            catch (InterruptedException e) {
              log.info("Interrupted, stopping task management.");
              break;
            }
            catch (Exception e) {
              startDelayMillis = config.getRestartDelay().getMillis();
              log.makeAlert(e, "Failed to manage").addData("restartDelay", startDelayMillis).emit();
            }
          }
        }
    );

    // Schedule storage sync job
    ScheduledExecutors.scheduleAtFixedRate(
        storageSyncExec,
        config.getStorageSyncRate(),
        () -> {
          if (!active) {
            log.info("Stopping storage sync as TaskQueue has been stopped");
            return ScheduledExecutors.Signal.STOP;
          }

          try {
            syncFromStorage();
          }
          catch (Exception e) {
            if (active) {
              log.makeAlert(e, "Failed to sync with storage").emit();
            }
          }

          return active ? ScheduledExecutors.Signal.REPEAT : ScheduledExecutors.Signal.STOP;
        }
    );
  }

  /**
   * Shuts down the queue.
   */
  @LifecycleStop
  public synchronized void stop()
  {
    active = false;
    tasks.clear();
    submittedTaskIds.clear();
    recentlyCompletedTaskIds.clear();
    managerExec.shutdownNow();
    storageSyncExec.shutdownNow();
    requestManagement("Stopping TaskQueue");
  }

  /**
   * Request management from the management thread. Non-blocking.
   * <p>
   * Callers (such as notifyStatus) can trigger task management by calling
   * this method.
   */
  private void requestManagement(String reason)
  {
    // do not care if the item fits into the queue:
    // if the queue is already full, request has been triggered anyway
    managementRequestQueue.offer(reason);
  }

  /**
   * Waits for a management request to be triggered by another thread.
   *
   * @throws InterruptedException if the thread is interrupted while waiting.
   */
  private void awaitManagementRequest() throws InterruptedException
  {
    // mitigate a busy loop, it can get pretty busy when there are a lot of start/stops
    try {
      Thread.sleep(MIN_WAIT_TIME_MILLIS);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // Wait for management to be requested
    String reason = managementRequestQueue.poll(
        MANAGEMENT_WAIT_TIMEOUT_MILLIS - MIN_WAIT_TIME_MILLIS,
        TimeUnit.MILLISECONDS
    );
    log.debug("Received management request [%s]", reason);
  }

  /**
   * Main task runner management loop. Meant to run forever, or, at least until we're stopped.
   */
  private void runTaskManagement() throws InterruptedException
  {
    // Ignore return value- we'll get the IDs and futures from getKnownTasks later.
    taskRunner.restore();

    while (active) {
      manageTasks();
      awaitManagementRequest();
    }
  }

  @VisibleForTesting
  void manageTasks()
  {
    runReadyTasks();
    killUnknownTasks();
  }


  /**
   * Submits ready tasks to the TaskRunner.
   * <p>
   * This method should be called only by the management thread.
   */
  private synchronized void runReadyTasks()
  {
    // Task futures available from the taskRunner
    final Map<String, ListenableFuture<TaskStatus>> runnerTaskFutures = new HashMap<>();
    for (final TaskRunnerWorkItem workItem : taskRunner.getKnownTasks()) {
      if (!recentlyCompletedTaskIds.contains(workItem.getTaskId())) {
        // Don't do anything with recently completed tasks; notifyStatus will handle it.
        runnerTaskFutures.put(workItem.getTaskId(), workItem.getResult());
      }
    }

    // Attain futures for all active tasks (assuming they are ready to run).
    for (final String taskId : Lists.newArrayList(activeTaskIdQueue)) {
      final Task task = tasks.get(taskId);
      if (task == null || recentlyCompletedTaskIds.contains(taskId)) {
        // Don't do anything for unknown tasks or recently completed tasks
      } else if (submittedTaskIds.contains(taskId)) {
        // Re-trigger execution of pending task to avoid unnecessary delays
        // see https://github.com/apache/druid/pull/6991
        if (isTaskPending(task)) {
          taskRunner.run(task);
        }
      } else if (runnerTaskFutures.containsKey(taskId)) {
        attachCallbacks(task, runnerTaskFutures.get(taskId));
        submittedTaskIds.add(taskId);
      } else if (isTaskReady(task)) {
        log.info("Asking taskRunner to run ready task [%s].", taskId);
        attachCallbacks(task, taskRunner.run(task));
        submittedTaskIds.add(taskId);
      } else {
        // Release all locks (possibly acquired by task.isReady()) if task is not ready
        taskLockbox.unlockAll(task);
      }
    }
  }

  private boolean isTaskReady(Task task)
  {
    try {
      return task.isReady(taskActionClientFactory.create(task));
    }
    catch (Exception e) {
      log.warn(e, "Error while checking if task [%s] is ready to run.", task.getId());
      final String errorMessage;
      if (e instanceof MaxAllowedLocksExceededException) {
        errorMessage = e.getMessage();
      } else {
        errorMessage = "Failed while waiting for the task to be ready to run. "
                       + "See overlord logs for more details.";
      }
      notifyStatus(task, TaskStatus.failure(task.getId(), errorMessage), errorMessage);
      return false;
    }
  }

  /**
   * Kills tasks not present in the set of known tasks.
   */
  private void killUnknownTasks()
  {
    final Set<String> knownTaskIds = tasks.keySet();
    final Set<String> runnerTaskIds = new HashSet<>(submittedTaskIds);
    taskRunner.getKnownTasks().forEach(item -> runnerTaskIds.add(item.getTaskId()));

    final Set<String> tasksToKill = Sets.difference(runnerTaskIds, knownTaskIds);
    tasksToKill.removeAll(recentlyCompletedTaskIds);
    if (tasksToKill.isEmpty()) {
      return;
    }

    log.info("Asking TaskRunner to clean up [%,d] unknown tasks.", tasksToKill.size());
    log.debug("Killing all tasks not present in known task ids [%s].", knownTaskIds);

    for (final String taskId : tasksToKill) {
      try {
        taskRunner.shutdown(taskId, "Killing unknown task");
      }
      catch (Exception e) {
        log.warn(e, "TaskRunner failed to shutdown unknown task [%s]", taskId);
      }
    }
  }

  private boolean isTaskPending(Task task)
  {
    return taskRunner.getPendingTasks().stream()
                     .anyMatch(workItem -> workItem.getTaskId().equals(task.getId()));
  }

  /**
   * Adds some work to the queue and the underlying task storage facility with a generic "running" status.
   *
   * @param task task to add
   *
   * @return true
   *
   * @throws EntryExistsException if the task already exists
   */
  public boolean add(final Task task) throws EntryExistsException
  {
    Preconditions.checkState(active, "Queue is not active!");
    Preconditions.checkNotNull(task, "task");
    Preconditions.checkState(
        tasks.size() < config.getMaxSize(),
        "Task queue is already full, max size is [%,d].", config.getMaxSize()
    );

    // Before adding the task, validate the ID, so it can be safely used in file paths, znodes, etc.
    IdUtils.validateId("Task ID", task.getId());

    if (taskStorage.getTask(task.getId()).isPresent()) {
      throw new EntryExistsException(StringUtils.format("Task %s already exists", task.getId()));
    }

    // Set forceTimeChunkLock before adding task spec to taskStorage, so that we can see always consistent task spec.
    task.addToContextIfAbsent(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockConfig.isForceTimeChunkLock());
    defaultTaskConfig.getContext().forEach(task::addToContextIfAbsent);
    // Every task should use the lineage-based segment allocation protocol unless it is explicitly set to
    // using the legacy protocol.
    task.addToContextIfAbsent(
        SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
        SinglePhaseParallelIndexTaskRunner.DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION
    );

    // Do not add the task to queue if insert into metadata fails for any reason
    taskStorage.insert(task, TaskStatus.running(task.getId()));
    addTaskInternal(task);
    requestManagement("Adding new task");
    return true;
  }

  /**
   * Atomically adds this task to the TaskQueue.
   */
  private void addTaskInternal(final Task task)
  {
    tasks.computeIfAbsent(
        task.getId(),
        taskId -> {
          activeTaskIdQueue.add(taskId);
          taskLockbox.add(task);
          return task;
        }
    );
  }

  /**
   * Atomically removes this task from the TaskQueue.
   */
  private void removeTaskInternal(final Task task)
  {
    tasks.compute(
        task.getId(),
        (taskId, existingTask) -> {
          if (existingTask != null) {
            taskLockbox.remove(existingTask);
            activeTaskIdQueue.remove(taskId);
          } else {
            log.warn("Removing unknown task [%s]", taskId);
          }

          submittedTaskIds.remove(taskId);
          recentlyCompletedTaskIds.remove(taskId);

          return null;
        }
    );
  }

  /**
   * Shuts down a task if it has not yet finished.
   *
   * @param taskId task to kill
   * @param reasonFormat A format string indicating the shutdown reason
   * @param args arguments for reasonFormat
   */
  public void shutdown(final String taskId, String reasonFormat, Object... args)
  {
    final Task task = tasks.get(Preconditions.checkNotNull(taskId, "taskId"));
    if (task != null) {
      notifyStatus(task, TaskStatus.failure(taskId, StringUtils.format(reasonFormat, args)), reasonFormat, args);
    }
  }

  /**
   * Shuts down a task, but records the task status as a success, unike {@link #shutdown(String, String, Object...)}
   *
   * @param taskId task to shutdown
   * @param reasonFormat A format string indicating the shutdown reason
   * @param args arguments for reasonFormat
   */
  public void shutdownWithSuccess(final String taskId, String reasonFormat, Object... args)
  {
    final Task task = tasks.get(Preconditions.checkNotNull(taskId, "taskId"));
    if (task != null) {
      notifyStatus(task, TaskStatus.success(taskId), reasonFormat, args);
    }
  }

  /**
   * Notify this queue that some task has an updated status. If this update is valid, the status will be persisted in
   * the task storage facility. If the status is a completed status, the task will be unlocked and no further
   * updates will be accepted.
   *
   * @param task       task to update
   * @param taskStatus new task status
   *
   * @throws NullPointerException     if task or status is null
   * @throws IllegalArgumentException if the task ID does not match the status ID
   * @throws IllegalStateException    if this queue is currently shut down
   */
  private void notifyStatus(final Task task, final TaskStatus taskStatus, String reasonFormat, Object... args)
  {
    Preconditions.checkNotNull(task, "task");
    Preconditions.checkNotNull(taskStatus, "status");
    Preconditions.checkState(active, "Queue is not active!");
    Preconditions.checkArgument(
        task.getId().equals(taskStatus.getId()),
        "Mismatching task ids[%s/%s]",
        task.getId(),
        taskStatus.getId()
    );

    if (!taskStatus.isComplete()) {
      // Nothing to do for incomplete statuses.
      return;
    }

    // Add this task to recentlyCompletedTasks, so it isn't managed while being cleaned up.
    recentlyCompletedTaskIds.add(task.getId());

    final TaskLocation taskLocation = taskRunner.getTaskLocation(task.getId());

    // Save status to metadata store first, so if we crash while doing the rest of the shutdown, our successor
    // remembers that this task has completed.
    try {
      final Optional<TaskStatus> previousStatus = taskStorage.getStatus(task.getId());
      if (!previousStatus.isPresent() || !previousStatus.get().isRunnable()) {
        log.makeAlert("Ignoring notification for already-complete task")
           .addData("task", task.getId()).emit();
      } else {
        taskStorage.setStatus(taskStatus.withLocation(taskLocation));
      }
    }
    catch (Throwable e) {
      // If persist fails, even after the retries performed in taskStorage, then metadata store and actual cluster
      // state have diverged. Send out an alert and continue with the task shutdown routine.
      log.makeAlert(e, "Failed to persist status for task")
         .addData("task", task.getId())
         .addData("statusCode", taskStatus.getStatusCode())
         .emit();
    }

    // Inform taskRunner that this task can be shut down.
    try {
      taskRunner.shutdown(task.getId(), reasonFormat, args);
    }
    catch (Throwable e) {
      // If task runner shutdown fails, continue with the task shutdown routine. We'll come back and try to
      // shut it down again later in manageInternalPostCritical, once it's removed from the "tasks" map.
      log.warn(e, "TaskRunner failed to cleanup task [%s] after completion", task.getId());
    }

    // Cleanup internal state
    removeTaskInternal(task);
    requestManagement("Completing task");
  }

  /**
   * Attach success and failure handlers to a task status future, such that when it completes, we perform the
   * appropriate updates.
   *
   * @param statusFuture a task status future
   */
  private void attachCallbacks(final Task task, final ListenableFuture<TaskStatus> statusFuture)
  {
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);

    Futures.addCallback(
        statusFuture,
        new FutureCallback<TaskStatus>()
        {
          @Override
          public void onSuccess(final TaskStatus status)
          {
            log.info("Received status [%s] for task [%s]", status.getStatusCode(), status.getId());
            handleStatus(status);
          }

          @Override
          public void onFailure(final Throwable t)
          {
            log.makeAlert(t, "Failed to run task")
               .addData("task", task.getId())
               .addData("type", task.getType())
               .addData("dataSource", task.getDataSource())
               .emit();
            handleStatus(
                TaskStatus.failure(task.getId(), "Failed to run task. See overlord logs for more details.")
            );
          }

          private void handleStatus(final TaskStatus status)
          {
            try {
              // If we're not supposed to be running anymore, don't do anything. Somewhat racey if the flag gets set
              // after we check and before we commit the database transaction, but better than nothing.
              if (!active) {
                log.info("Not handling status of task [%s] as TaskQueue is stopped.", task.getId());
                return;
              }

              notifyStatus(task, status, "notified status change from task");

              // Emit event and log, if the task is done
              if (status.isComplete()) {
                IndexTaskUtils.setTaskStatusDimensions(metricBuilder, status);
                emitter.emit(metricBuilder.build("task/run/time", status.getDuration()));

                log.info(
                    "Completed task [%s] with status [%s] in [%d] ms.",
                    task.getId(), status.getStatusCode(), status.getDuration()
                );

                final String datasource = task.getDataSource();
                if (status.isSuccess()) {
                  datasourceToSuccessfulTaskCount.compute(datasource, (ds, c) -> c == null ? 1L : c + 1);
                } else {
                  datasourceToFailedTaskCount.compute(datasource, (ds, c) -> c == null ? 1L : c + 1);
                }
              }
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to handle task status")
                 .addData("task", task.getId())
                 .addData("statusCode", status.getStatusCode())
                 .emit();
            }
          }
        }
    );
  }

  /**
   * Resync the contents of this task queue with our storage facility.
   * Useful to make sure our in-memory state corresponds to the storage facility
   * even if the latter is manually modified.
   * <p>
   * This method must be called only when queue is {@link #active}, except when
   * starting up.
   */
  private synchronized void syncFromStorage()
  {
    final Map<String, Task> newTasks = taskStorage.getActiveTasks().stream().collect(
        Collectors.toMap(Task::getId, Function.identity())
    );
    final Map<String, Task> oldTasks = new HashMap<>(tasks);

    // Calculate differences on IDs instead of Task Objects.
    Set<String> commonIds = Sets.intersection(newTasks.keySet(), oldTasks.keySet());
    for (String taskId : commonIds) {
      newTasks.remove(taskId);
      oldTasks.remove(taskId);
    }
    Collection<Task> addedTasks = newTasks.values();
    Collection<Task> removedTasks = oldTasks.values();

    // Add new tasks and clean up removed tasks
    addedTasks.forEach(this::addTaskInternal);
    removedTasks.forEach(this::removeTaskInternal);
    log.info(
        "Synced [%d] tasks from storage. Added [%d] tasks, removed [%d] tasks.",
        newTasks.size(), addedTasks.size(), removedTasks.size()
    );

    requestManagement("Syncing storage");
  }

  public Map<String, Long> getAndResetSuccessfulTaskCounts()
  {
    Set<String> datasources = new HashSet<>(datasourceToSuccessfulTaskCount.keySet());

    Map<String, Long> total = new HashMap<>();
    datasources.forEach(ds -> total.put(ds, datasourceToSuccessfulTaskCount.remove(ds)));
    return total;
  }

  public Map<String, Long> getAndResetFailedTaskCounts()
  {
    Set<String> datasources = new HashSet<>(datasourceToFailedTaskCount.keySet());

    Map<String, Long> total = new HashMap<>();
    datasources.forEach(ds -> total.put(ds, datasourceToFailedTaskCount.remove(ds)));
    return total;
  }

  public Map<String, Long> getRunningTaskCount()
  {
    return taskRunner.getRunningTasks().stream().collect(
        Collectors.toMap(TaskRunnerWorkItem::getDataSource, task -> 1L, Long::sum)
    );
  }

  public Map<String, Long> getPendingTaskCount()
  {
    return taskRunner.getPendingTasks().stream().collect(
        Collectors.toMap(TaskRunnerWorkItem::getDataSource, task -> 1L, Long::sum)
    );
  }

  public Map<String, Long> getWaitingTaskCount()
  {
    Set<String> runnerKnownTaskIds = taskRunner.getKnownTasks()
                                               .stream()
                                               .map(TaskRunnerWorkItem::getTaskId)
                                               .collect(Collectors.toSet());

    return tasks.values().stream()
                .filter(task -> !runnerKnownTaskIds.contains(task.getId()))
                .collect(Collectors.toMap(Task::getDataSource, task -> 1L, Long::sum));
  }

  @VisibleForTesting
  List<Task> getTasks()
  {
    return new ArrayList<>(tasks.values());
  }
}
