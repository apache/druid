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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.Counters;
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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
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
  private static final long MANAGEMENT_WAIT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);
  private static final long MIN_WAIT_TIME_MS = 100;

  // Task ID -> Task, for tasks that are active in some way (submitted, running, or finished and to-be-cleaned-up).
  @GuardedBy("giant")
  private final LinkedHashMap<String, Task> tasks = new LinkedHashMap<>();

  // Task ID -> Future from the TaskRunner
  @GuardedBy("giant")
  private final Map<String, ListenableFuture<TaskStatus>> taskFutures = new HashMap<>();

  /**
   * Tasks that have recently completed and are being cleaned up. These tasks
   * should not be relaunched by task management.
   */
  @GuardedBy("giant")
  private final Set<String> recentlyCompletedTasks = new HashSet<>();

  private final TaskLockConfig lockConfig;
  private final TaskQueueConfig config;
  private final DefaultTaskConfig defaultTaskConfig;
  private final TaskStorage taskStorage;
  private final TaskRunner taskRunner;
  private final TaskActionClientFactory taskActionClientFactory;
  private final TaskLockbox taskLockbox;
  private final ServiceEmitter emitter;

  private final ReentrantLock giant = new ReentrantLock(true);
  @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
  private final BlockingQueue<Object> managementMayBeNecessary = new ArrayBlockingQueue<>(8);
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

  /**
   * Dedicated executor for task completion callbacks, to ensure that task runner
   * and worker sync operations are not blocked.
   */
  private final ExecutorService taskCompleteCallbackExecutor;

  private volatile boolean active = false;

  private static final EmittingLogger log = new EmittingLogger(TaskQueue.class);

  private final ConcurrentHashMap<String, AtomicLong> totalSuccessfulTaskCount = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, AtomicLong> totalFailedTaskCount = new ConcurrentHashMap<>();
  @GuardedBy("totalSuccessfulTaskCount")
  private Map<String, Long> prevTotalSuccessfulTaskCount = new HashMap<>();
  @GuardedBy("totalFailedTaskCount")
  private Map<String, Long> prevTotalFailedTaskCount = new HashMap<>();

  private final AtomicInteger statusUpdatesInQueue = new AtomicInteger();
  private final AtomicInteger handledStatusUpdates = new AtomicInteger();

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
    this.taskCompleteCallbackExecutor = Execs.multiThreaded(
        config.getTaskCompleteHandlerNumThreads(),
        "TaskQueue-OnComplete-%d"
    );
  }

  @VisibleForTesting
  void setActive(boolean active)
  {
    this.active = active;
  }

  /**
   * Starts this task queue. Allows {@link #add(Task)} to accept new tasks.
   */
  @LifecycleStart
  public void start()
  {
    giant.lock();

    try {
      Preconditions.checkState(!active, "queue must be stopped");
      active = true;
      syncFromStorage();
      // Mark these tasks as failed as they could not reacuire the lock
      // Clean up needs to happen after tasks have been synced from storage
      Set<Task> tasksToFail = taskLockbox.syncFromStorage().getTasksToFail();
      for (Task task : tasksToFail) {
        shutdown(task.getId(),
                 "Shutting down forcefully as task failed to reacquire lock while becoming leader");
      }
      managerExec.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              while (true) {
                try {
                  manage();
                  break;
                }
                catch (InterruptedException e) {
                  log.info("Interrupted, exiting!");
                  break;
                }
                catch (Exception e) {
                  final long restartDelay = config.getRestartDelay().getMillis();
                  log.makeAlert(e, "Failed to manage").addData("restartDelay", restartDelay).emit();
                  try {
                    Thread.sleep(restartDelay);
                  }
                  catch (InterruptedException e2) {
                    log.info("Interrupted, exiting!");
                    break;
                  }
                }
              }
            }
          }
      );
      ScheduledExecutors.scheduleAtFixedRate(
          storageSyncExec,
          config.getStorageSyncRate(),
          new Callable<ScheduledExecutors.Signal>()
          {
            @Override
            public ScheduledExecutors.Signal call()
            {
              try {
                syncFromStorage();
              }
              catch (Exception e) {
                if (active) {
                  log.makeAlert(e, "Failed to sync with storage").emit();
                }
              }
              if (active) {
                return ScheduledExecutors.Signal.REPEAT;
              } else {
                return ScheduledExecutors.Signal.STOP;
              }
            }
          }
      );
      requestManagement();
      // Remove any unacquired locks from storage (shutdown only clears entries for which a TaskLockPosse was acquired)
      // This is called after requesting management as locks need to be cleared after notifyStatus is processed
      for (Task task : tasksToFail) {
        for (TaskLock lock : taskStorage.getLocks(task.getId())) {
          taskStorage.removeLock(task.getId(), lock);
        }
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Shuts down the queue.
   */
  @LifecycleStop
  public void stop()
  {
    giant.lock();

    try {
      tasks.clear();
      taskFutures.clear();
      active = false;
      managerExec.shutdownNow();
      storageSyncExec.shutdownNow();
      requestManagement();
    }
    finally {
      giant.unlock();
    }
  }

  public boolean isActive()
  {
    return active;
  }

  /**
   * Request management from the management thread. Non-blocking.
   *
   * Other callers (such as notifyStatus) should trigger activity on the
   * TaskQueue thread by requesting management here.
   */
  void requestManagement()
  {
    // use a BlockingQueue since the offer/poll/wait behaviour is simple
    // and very easy to reason about

    // the request has to be offer (non blocking), since someone might request
    // while already holding giant lock

    // do not care if the item fits into the queue:
    // if the queue is already full, request has been triggered anyway
    managementMayBeNecessary.offer(this);
  }

  /**
   * Await for an event to manage.
   *
   * This should only be called from the management thread to wait for activity.
   *
   * @param nanos
   * @throws InterruptedException
   */
  @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED", justification = "using queue as notification mechanism, result has no value")
  void awaitManagementNanos(long nanos) throws InterruptedException
  {
    // mitigate a busy loop, it can get pretty busy when there are a lot of start/stops
    try {
      Thread.sleep(MIN_WAIT_TIME_MS);
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // wait for an item, if an item arrives (or is already available), complete immediately
    // (does not actually matter what the item is)
    managementMayBeNecessary.poll(nanos - (TimeUnit.MILLISECONDS.toNanos(MIN_WAIT_TIME_MS)), TimeUnit.NANOSECONDS);

    // there may have been multiple requests, clear them all
    managementMayBeNecessary.clear();
  }

  /**
   * Main task runner management loop. Meant to run forever, or, at least until we're stopped.
   */
  private void manage() throws InterruptedException
  {
    log.info("Beginning management in %s.", config.getStartDelay());
    Thread.sleep(config.getStartDelay().getMillis());

    // Ignore return value- we'll get the IDs and futures from getKnownTasks later.
    taskRunner.restore();

    while (active) {
      manageInternal();

      // awaitNanos because management may become necessary without this condition signalling,
      // due to e.g. tasks becoming ready when other folks mess with the TaskLockbox.
      awaitManagementNanos(MANAGEMENT_WAIT_TIMEOUT_NANOS);
    }
  }

  @VisibleForTesting
  void manageInternal()
  {
    Set<String> knownTaskIds = new HashSet<>();
    Map<String, ListenableFuture<TaskStatus>> runnerTaskFutures = new HashMap<>();

    giant.lock();

    try {
      manageInternalCritical(knownTaskIds, runnerTaskFutures);
    }
    finally {
      giant.unlock();
    }

    manageInternalPostCritical(knownTaskIds, runnerTaskFutures);
  }


  /**
   * Management loop critical section tasks.
   *
   * @param knownTaskIds will be modified - filled with known task IDs
   * @param runnerTaskFutures will be modified - filled with futures related to getting the running tasks
   */
  @GuardedBy("giant")
  private void manageInternalCritical(
      final Set<String> knownTaskIds,
      final Map<String, ListenableFuture<TaskStatus>> runnerTaskFutures
  )
  {
    // Task futures available from the taskRunner
    for (final TaskRunnerWorkItem workItem : taskRunner.getKnownTasks()) {
      if (!recentlyCompletedTasks.contains(workItem.getTaskId())) {
        // Don't do anything with tasks that have recently finished; notifyStatus will handle it.
        runnerTaskFutures.put(workItem.getTaskId(), workItem.getResult());
      }
    }
    // Attain futures for all active tasks (assuming they are ready to run).
    // Copy tasks list, as notifyStatus may modify it.
    for (final Task task : ImmutableList.copyOf(tasks.values())) {
      if (recentlyCompletedTasks.contains(task.getId())) {
        // Don't do anything with tasks that have recently finished; notifyStatus will handle it.
        continue;
      }

      knownTaskIds.add(task.getId());

      if (!taskFutures.containsKey(task.getId())) {
        final ListenableFuture<TaskStatus> runnerTaskFuture;
        if (runnerTaskFutures.containsKey(task.getId())) {
          runnerTaskFuture = runnerTaskFutures.get(task.getId());
        } else {
          // Task should be running, so run it.
          final boolean taskIsReady;
          try {
            taskIsReady = task.isReady(taskActionClientFactory.create(task));
          }
          catch (Exception e) {
            log.warn(e, "Exception thrown during isReady for task: %s", task.getId());
            final String errorMessage;
            if (e instanceof MaxAllowedLocksExceededException) {
              errorMessage = e.getMessage();
            } else {
              errorMessage = "Failed while waiting for the task to be ready to run. "
                                          + "See overlord logs for more details.";
            }
            notifyStatus(task, TaskStatus.failure(task.getId(), errorMessage), errorMessage);
            continue;
          }
          if (taskIsReady) {
            log.info("Asking taskRunner to run: %s", task.getId());
            runnerTaskFuture = taskRunner.run(task);
          } else {
            // Task.isReady() can internally lock intervals or segments.
            // We should release them if the task is not ready.
            taskLockbox.unlockAll(task);
            continue;
          }
        }
        attachCallbacks(task, runnerTaskFuture);
        taskFutures.put(task.getId(), runnerTaskFuture);
      } else if (isTaskPending(task)) {
        // if the taskFutures contain this task and this task is pending, also let the taskRunner
        // to run it to guarantee it will be assigned to run
        // see https://github.com/apache/druid/pull/6991
        taskRunner.run(task);
      }
    }
  }

  @VisibleForTesting
  private void manageInternalPostCritical(
      final Set<String> knownTaskIds,
      final Map<String, ListenableFuture<TaskStatus>> runnerTaskFutures
  )
  {
    // Kill tasks that shouldn't be running
    final Set<String> tasksToKill = Sets.difference(runnerTaskFutures.keySet(), knownTaskIds);
    if (!tasksToKill.isEmpty()) {
      log.info("Asking taskRunner to clean up %,d tasks.", tasksToKill.size());

      // On large installations running several thousands of tasks,
      // concatenating the list of known task ids can be compupationally expensive.
      final boolean logKnownTaskIds = log.isDebugEnabled();
      final String reason = logKnownTaskIds
              ? StringUtils.format("Task is not in knownTaskIds[%s]", knownTaskIds)
              : "Task is not in knownTaskIds";

      for (final String taskId : tasksToKill) {
        try {
          taskRunner.shutdown(taskId, reason);
        }
        catch (Exception e) {
          log.warn(e, "TaskRunner failed to clean up task: %s", taskId);
        }
      }
    }
  }

  private boolean isTaskPending(Task task)
  {
    return taskRunner.getPendingTasks()
                     .stream()
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
    // Before adding the task, validate the ID, so it can be safely used in file paths, znodes, etc.
    IdUtils.validateId("Task ID", task.getId());

    if (taskStorage.getTask(task.getId()).isPresent()) {
      throw new EntryExistsException("Task", task.getId());
    }

    // Set forceTimeChunkLock before adding task spec to taskStorage, so that we can see always consistent task spec.
    task.addToContextIfAbsent(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockConfig.isForceTimeChunkLock());
    defaultTaskConfig.getContext().forEach(task::addToContextIfAbsent);
    // Every task shuold use the lineage-based segment allocation protocol unless it is explicitly set to
    // using the legacy protocol.
    task.addToContextIfAbsent(
        SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
        SinglePhaseParallelIndexTaskRunner.DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION
    );

    giant.lock();

    try {
      Preconditions.checkState(active, "Queue is not active!");
      Preconditions.checkNotNull(task, "task");
      if (tasks.size() >= config.getMaxSize()) {
        throw DruidException.forPersona(DruidException.Persona.ADMIN)
                .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                .build(
                        StringUtils.format(
                                "Too many tasks are in the queue (Limit = %d), " +
                                        "(Current active tasks = %d). Retry later or increase the druid.indexer.queue.maxSize",
                                config.getMaxSize(),
                                tasks.size()
                        )
                );
      }

      // If this throws with any sort of exception, including TaskExistsException, we don't want to
      // insert the task into our queue. So don't catch it.
      taskStorage.insert(task, TaskStatus.running(task.getId()));
      addTaskInternal(task);
      requestManagement();
      return true;
    }
    finally {
      giant.unlock();
    }
  }

  @GuardedBy("giant")
  private void addTaskInternal(final Task task)
  {
    final Task existingTask = tasks.putIfAbsent(task.getId(), task);

    if (existingTask == null) {
      taskLockbox.add(task);
    } else if (!existingTask.equals(task)) {
      throw new ISE("Cannot add task ID [%s] with same ID as task that has already been added", task.getId());
    }
  }

  /**
   * Removes a task from {@link #tasks} and {@link #taskLockbox}, if it exists. Returns whether the task was
   * removed or not.
   */
  @GuardedBy("giant")
  private boolean removeTaskInternal(final String taskId)
  {
    final Task task = tasks.remove(taskId);
    if (task != null) {
      taskLockbox.remove(task);
      return true;
    } else {
      return false;
    }
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
    giant.lock();

    try {
      final Task task = tasks.get(Preconditions.checkNotNull(taskId, "taskId"));
      if (task != null) {
        notifyStatus(task, TaskStatus.failure(taskId, StringUtils.format(reasonFormat, args)), reasonFormat, args);
      }
    }
    finally {
      giant.unlock();
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
    giant.lock();

    try {
      final Task task = tasks.get(Preconditions.checkNotNull(taskId, "taskId"));
      if (task != null) {
        notifyStatus(task, TaskStatus.success(taskId), reasonFormat, args);
      }
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Notifies this queue that the given task has an updated status. If this update
   * is valid and task is now complete, the following operations are performed:
   * <ul>
   * <li>Add task to {@link #recentlyCompletedTasks} to prevent re-launching them</li>
   * <li>Persist new status in the metadata storage to safeguard against crashes
   * and leader re-elections</li>
   * <li>Request {@link #taskRunner} to shutdown task (synchronously)</li>
   * <li>Remove all locks for task from metadata storage</li>
   * <li>Remove task entry from {@link #tasks} and {@link #taskFutures}</li>
   * <li>Remove task from {@link #recentlyCompletedTasks}</li>
   * <li>Request task management</li>
   * </ul>
   * <p>
   * Since this operation involves DB updates and synchronous remote calls, it
   * must be invoked on a dedicated executor so that task runner and worker sync
   * is not blocked.
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

    // Critical section: add this task to recentlyCompletedTasks, so it isn't managed while being cleaned up.
    giant.lock();
    try {
      recentlyCompletedTasks.add(task.getId());
    }
    finally {
      giant.unlock();
    }

    final TaskLocation taskLocation = taskRunner.getTaskLocation(task.getId());

    // Save status to metadata store first, so if we crash while doing the rest of the shutdown, our successor
    // remembers that this task has completed.
    try {
      //The code block is only called when a task completes,
      //and we need to check to make sure the metadata store has the correct status stored.
      final Optional<TaskStatus> previousStatus = taskStorage.getStatus(task.getId());
      if (!previousStatus.isPresent() || !previousStatus.get().isRunnable()) {
        log.makeAlert("Ignoring notification for already-complete task").addData("task", task.getId()).emit();
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
      log.warn(e, "TaskRunner failed to cleanup task after completion: %s", task.getId());
    }

    // Critical section: remove this task from all of our tracking data structures.
    giant.lock();
    try {
      if (removeTaskInternal(task.getId())) {
        taskFutures.remove(task.getId());
      } else {
        log.warn("Unknown task[%s] completed", task.getId());
      }

      recentlyCompletedTasks.remove(task.getId());
      requestManagement();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Attaches callbacks to the task status future to update application state when
   * the task completes. Submits a job to handle the status on the dedicated
   * {@link #taskCompleteCallbackExecutor}.
   * <p>
   * The {@code onSuccess} and {@code onFailure} methods will however run on the
   * executor providing the {@code statusFuture} itself (typically the worker sync executor).
   * This has been done in order to track metrics for in-flight status updates
   * immediately. Thus, care must be taken to ensure that the success/failure
   * methods remain lightweight enough to keep the sync executor unblocked.
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
            log.info("Received status[%s] for task[%s].", status.getStatusCode(), status.getId());
            statusUpdatesInQueue.incrementAndGet();
            taskCompleteCallbackExecutor.execute(() -> handleStatus(status));
          }

          @Override
          public void onFailure(final Throwable t)
          {
            log.makeAlert(t, "Failed to run task")
               .addData("task", task.getId())
               .addData("type", task.getType())
               .addData("dataSource", task.getDataSource())
               .emit();
            statusUpdatesInQueue.incrementAndGet();
            TaskStatus status = TaskStatus.failure(
                task.getId(),
                "Failed to run task. See overlord logs for more details."
            );
            taskCompleteCallbackExecutor.execute(() -> handleStatus(status));
          }

          private void handleStatus(final TaskStatus status)
          {
            try {
              // If we're not supposed to be running anymore, don't do anything. Somewhat racey if the flag gets set
              // after we check and before we commit the database transaction, but better than nothing.
              if (!active) {
                log.info("Abandoning task [%s] due to shutdown.", task.getId());
                return;
              }

              notifyStatus(task, status, "notified status change from task");

              // Emit event and log, if the task is done
              if (status.isComplete()) {
                IndexTaskUtils.setTaskStatusDimensions(metricBuilder, status);
                emitter.emit(metricBuilder.setMetric("task/run/time", status.getDuration()));

                log.info(
                    "Completed task[%s] with status[%s] in [%d]ms.",
                    task.getId(), status.getStatusCode(), status.getDuration()
                );

                if (status.isSuccess()) {
                  Counters.incrementAndGetLong(totalSuccessfulTaskCount, task.getDataSource());
                } else {
                  Counters.incrementAndGetLong(totalFailedTaskCount, task.getDataSource());
                }
              }
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to handle task status")
                 .addData("task", task.getId())
                 .addData("statusCode", status.getStatusCode())
                 .emit();
            }
            finally {
              statusUpdatesInQueue.decrementAndGet();
              handledStatusUpdates.incrementAndGet();
            }
          }
        },
        // Use direct executor to track metrics for in-flight updates immediately
        Execs.directExecutor()
    );
  }

  /**
   * Resync the contents of this task queue with our storage facility. Useful to make sure our in-memory state
   * corresponds to the storage facility even if the latter is manually modified.
   */
  private void syncFromStorage()
  {
    giant.lock();

    try {
      if (active) {
        final Map<String, Task> newTasks = toTaskIDMap(taskStorage.getActiveTasks());
        final int tasksSynced = newTasks.size();
        final Map<String, Task> oldTasks = new HashMap<>(tasks);

        // Calculate differences on IDs instead of Task Objects.
        Set<String> commonIds = Sets.newHashSet(Sets.intersection(newTasks.keySet(), oldTasks.keySet()));
        for (String taskID : commonIds) {
          newTasks.remove(taskID);
          oldTasks.remove(taskID);
        }
        Collection<Task> addedTasks = newTasks.values();
        Collection<Task> removedTasks = oldTasks.values();

        // Clean up removed Tasks
        for (Task task : removedTasks) {
          removeTaskInternal(task.getId());
        }

        // Add newly Added tasks to the queue
        for (Task task : addedTasks) {
          addTaskInternal(task);
        }

        log.info(
            "Synced %d tasks from storage (%d tasks added, %d tasks removed).",
            tasksSynced,
            addedTasks.size(),
            removedTasks.size()
        );
        requestManagement();
      } else {
        log.info("Not active. Skipping storage sync.");
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to sync tasks from storage!");
      throw new RuntimeException(e);
    }
    finally {
      giant.unlock();
    }
  }

  private static Map<String, Task> toTaskIDMap(List<Task> taskList)
  {
    Map<String, Task> rv = new HashMap<>();
    for (Task task : taskList) {
      rv.put(task.getId(), task);
    }
    return rv;
  }

  private Map<String, Long> getDeltaValues(Map<String, Long> total, Map<String, Long> prev)
  {
    return total.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue() - prev.getOrDefault(e.getKey(), 0L)));
  }

  public Map<String, Long> getSuccessfulTaskCount()
  {
    Map<String, Long> total = CollectionUtils.mapValues(totalSuccessfulTaskCount, AtomicLong::get);
    synchronized (totalSuccessfulTaskCount) {
      Map<String, Long> delta = getDeltaValues(total, prevTotalSuccessfulTaskCount);
      prevTotalSuccessfulTaskCount = total;
      return delta;
    }
  }

  public Map<String, Long> getFailedTaskCount()
  {
    Map<String, Long> total = CollectionUtils.mapValues(totalFailedTaskCount, AtomicLong::get);
    synchronized (totalFailedTaskCount) {
      Map<String, Long> delta = getDeltaValues(total, prevTotalFailedTaskCount);
      prevTotalFailedTaskCount = total;
      return delta;
    }
  }

  Map<String, String> getCurrentTaskDatasources()
  {
    giant.lock();
    try {
      return tasks.values().stream().collect(Collectors.toMap(Task::getId, Task::getDataSource));
    }
    finally {
      giant.unlock();
    }
  }

  public Map<String, Long> getRunningTaskCount()
  {
    Map<String, String> taskDatasources = getCurrentTaskDatasources();
    return taskRunner.getRunningTasks()
                     .stream()
                     .collect(Collectors.toMap(
                         e -> taskDatasources.getOrDefault(e.getTaskId(), ""),
                         e -> 1L,
                         Long::sum
                     ));
  }

  public Map<String, Long> getPendingTaskCount()
  {
    Map<String, String> taskDatasources = getCurrentTaskDatasources();
    return taskRunner.getPendingTasks()
                     .stream()
                     .collect(Collectors.toMap(
                         e -> taskDatasources.getOrDefault(e.getTaskId(), ""),
                         e -> 1L,
                         Long::sum
                     ));
  }

  public Map<String, Long> getWaitingTaskCount()
  {
    Set<String> runnerKnownTaskIds = taskRunner.getKnownTasks()
                                               .stream()
                                               .map(TaskRunnerWorkItem::getTaskId)
                                               .collect(Collectors.toSet());

    giant.lock();
    try {
      return tasks.values().stream().filter(task -> !runnerKnownTaskIds.contains(task.getId()))
                  .collect(Collectors.toMap(Task::getDataSource, task -> 1L, Long::sum));
    }
    finally {
      giant.unlock();
    }
  }

  public Optional<TaskStatus> getTaskStatus(final String taskId)
  {
    RunnerTaskState runnerTaskState = taskRunner.getRunnerTaskState(taskId);
    if (runnerTaskState != null && runnerTaskState != RunnerTaskState.NONE) {
      return Optional.of(TaskStatus.running(taskId).withLocation(taskRunner.getTaskLocation(taskId)));
    } else {
      return taskStorage.getStatus(taskId);
    }
  }

  public CoordinatorRunStats getQueueStats()
  {
    final int queuedUpdates = statusUpdatesInQueue.get();
    final int handledUpdates = handledStatusUpdates.getAndSet(0);
    if (queuedUpdates > 0) {
      log.info("There are [%d] task status updates in queue, handled [%d]", queuedUpdates, handledUpdates);
    }

    final CoordinatorRunStats stats = new CoordinatorRunStats();
    stats.add(Stats.TaskQueue.STATUS_UPDATES_IN_QUEUE, queuedUpdates);
    stats.add(Stats.TaskQueue.HANDLED_STATUS_UPDATES, handledUpdates);
    return stats;
  }

  public Optional<Task> getActiveTask(String id)
  {
    giant.lock();
    try {
      return Optional.fromNullable(tasks.get(id));
    }
    finally {
      giant.unlock();
    }
  }

  @VisibleForTesting
  List<Task> getTasks()
  {
    giant.lock();
    try {
      return new ArrayList<>(tasks.values());
    }
    finally {
      giant.unlock();
    }
  }
}
