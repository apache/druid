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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.EntryAlreadyExists;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.Counters;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskContextEnricher;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.MaxAllowedLocksExceededException;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseParallelIndexTaskRunner;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.metadata.PasswordProviderRedactionMixIn;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.utils.CollectionUtils;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
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
  public static final String FAILED_TO_RUN_TASK_SEE_OVERLORD_MSG = "Failed to run task. See overlord logs for more details.";
  private static final long MANAGEMENT_WAIT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(60);
  private static final long MIN_WAIT_TIME_MS = 100;

  // 60 MB warning threshold since 64 MB is the default max_allowed_packet size in MySQL 8+
  private static final long TASK_SIZE_WARNING_THRESHOLD = 1024 * 1024 * 60;

  /**
   * Tasks being managed by this queue. This includes tasks that are currently
   * active i.e. submitted, running or recently completed (pending clean up).
   */
  private final ConcurrentHashMap<String, TaskEntry> activeTasks = new ConcurrentHashMap<>();

  private final TaskLockConfig lockConfig;
  private final TaskQueueConfig config;
  private final DefaultTaskConfig defaultTaskConfig;
  private final TaskStorage taskStorage;
  private final TaskRunner taskRunner;
  private final TaskActionClientFactory taskActionClientFactory;
  private final GlobalTaskLockbox taskLockbox;
  private final ServiceEmitter emitter;
  private final ObjectMapper passwordRedactingMapper;
  private final TaskContextEnricher taskContextEnricher;

  /**
   * Lock used to ensure that {@link #start} and {@link #stop} do not run while
   * queue management is in progress. {@link #start} and {@link #stop} methods
   * acquire a WRITE lock whereas all other operations acquire a READ lock.
   */
  private final ReentrantReadWriteLock startStopLock = new ReentrantReadWriteLock(true);

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

  private final ConcurrentHashMap<RowKey, AtomicLong> totalSuccessfulTaskCount = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<RowKey, AtomicLong> totalFailedTaskCount = new ConcurrentHashMap<>();
  @GuardedBy("totalSuccessfulTaskCount")
  private Map<RowKey, Long> prevTotalSuccessfulTaskCount = new HashMap<>();
  @GuardedBy("totalFailedTaskCount")
  private Map<RowKey, Long> prevTotalFailedTaskCount = new HashMap<>();

  private final AtomicInteger statusUpdatesInQueue = new AtomicInteger();
  private final AtomicInteger handledStatusUpdates = new AtomicInteger();

  public TaskQueue(
      TaskLockConfig lockConfig,
      TaskQueueConfig config,
      DefaultTaskConfig defaultTaskConfig,
      TaskStorage taskStorage,
      TaskRunner taskRunner,
      TaskActionClientFactory taskActionClientFactory,
      GlobalTaskLockbox taskLockbox,
      ServiceEmitter emitter,
      ObjectMapper mapper,
      TaskContextEnricher taskContextEnricher
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
    this.passwordRedactingMapper = mapper.copy()
                                         .addMixIn(PasswordProvider.class, PasswordProviderRedactionMixIn.class);
    this.taskContextEnricher = Preconditions.checkNotNull(taskContextEnricher, "taskContextEnricher");
  }

  @VisibleForTesting
  void setActive(boolean active)
  {
    startStopLock.writeLock().lock();
    try {
      this.active = active;
    }
    finally {
      startStopLock.writeLock().unlock();
    }
  }

  /**
   * Starts this task queue. Allows {@link #add(Task)} to accept new tasks.
   */
  @LifecycleStart
  public void start()
  {
    startStopLock.writeLock().lock();

    try {
      Preconditions.checkState(!active, "queue must be stopped");
      setActive(true);
      // Mark these tasks as failed as they could not reacuire the lock
      // Clean up needs to happen after tasks have been synced from storage
      Set<Task> tasksToFail = taskLockbox.syncFromStorage().getTasksToFail();
      syncFromStorage();
      for (Task task : tasksToFail) {
        shutdown(task.getId(),
                 "Shutting down forcefully as task failed to reacquire lock while becoming leader");
      }
      managerExec.submit(
          () -> {
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
      );
      ScheduledExecutors.scheduleAtFixedRate(
          storageSyncExec,
          config.getStorageSyncRate(),
          () -> {
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
      startStopLock.writeLock().unlock();
    }
  }

  /**
   * Shuts down the queue.
   */
  @LifecycleStop
  public void stop()
  {
    startStopLock.writeLock().lock();

    try {
      setActive(false);
      activeTasks.clear();
      taskLockbox.shutdown();
      managerExec.shutdownNow();
      storageSyncExec.shutdownNow();
      requestManagement();
    }
    finally {
      startStopLock.writeLock().unlock();
    }
  }

  public boolean isActive()
  {
    return active;
  }

  /**
   * Request management from the management thread. Non-blocking.
   * <p>
   * Other callers (such as notifyStatus) should trigger activity on the
   * TaskQueue thread by requesting management here.
   */
  private void requestManagement()
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
   * <p>
   * This should only be called from the management thread to wait for activity.
   */
  @SuppressFBWarnings(value = "RV_RETURN_VALUE_IGNORED", justification = "using queue as notification mechanism, result has no value")
  private void awaitManagement() throws InterruptedException
  {
    // mitigate a busy loop, it can get pretty busy when there are a lot of start/stops
    Thread.sleep(MIN_WAIT_TIME_MS);

    // wait for an item, if an item arrives (or is already available), complete immediately
    // (does not actually matter what the item is)
    managementMayBeNecessary.poll(
        TaskQueue.MANAGEMENT_WAIT_TIMEOUT_NANOS - TimeUnit.MILLISECONDS.toNanos(MIN_WAIT_TIME_MS),
        TimeUnit.NANOSECONDS
    );

    // there may have been multiple requests, clear them all
    managementMayBeNecessary.clear();
  }

  /**
   * Main task runner management loop. Meant to run forever, or, at least until we're stopped.
   */
  private void manage() throws InterruptedException
  {
    log.info("Beginning management in [%s].", config.getStartDelay());
    Thread.sleep(config.getStartDelay().getMillis());

    // Ignore return value - we'll get the IDs and futures from getKnownTasks later.
    taskRunner.restore();

    while (active) {
      manageQueuedTasks();

      // wait because management may become necessary without this condition signalling,
      // e.g., due to tasks becoming ready when other folks mess with the TaskLockbox.
      awaitManagement();
    }
  }

  @VisibleForTesting
  void manageQueuedTasks()
  {
    startStopLock.readLock().lock();
    try {
      startPendingTasksOnRunner();
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }


  /**
   * Starts pending tasks on the {@link #taskRunner}.
   * Cleans up tasks present on the {@link #taskRunner} but not present in
   * {@link #activeTasks} anymore.
   */
  @GuardedBy("startStopLock")
  private void startPendingTasksOnRunner()
  {
    final Set<String> unknownTaskIds = new HashSet<>();

    // Get all task futures from the taskRunner and shutdown tasks not in queue anymore
    final Map<String, ListenableFuture<TaskStatus>> runnerTaskFutures = new HashMap<>();
    for (final TaskRunnerWorkItem workItem : List.copyOf(taskRunner.getKnownTasks())) {
      final String taskId = workItem.getTaskId();
      updateTaskEntry(taskId, entry -> {
        if (entry == null) {
          unknownTaskIds.add(taskId);
          shutdownUnknownTaskOnRunner(taskId);
        } else {
          runnerTaskFutures.put(taskId, workItem.getResult());
        }
      });
    }
    log.info("Cleaned up [%,d] tasks on task runner with IDs[%s].", unknownTaskIds.size(), unknownTaskIds);

    // Attain futures for all active tasks (assuming they are ready to run).
    // Copy tasks list, as notifyStatus may modify it.
    for (final String queuedTaskId : List.copyOf(activeTasks.keySet())) {
      updateTaskEntry(
          queuedTaskId,
          entry -> startPendingTaskOnRunner(entry, runnerTaskFutures.get(queuedTaskId))
      );
    }
  }

  @GuardedBy("startStopLock")
  private void startPendingTaskOnRunner(TaskEntry entry, ListenableFuture<TaskStatus> runnerTaskFuture)
  {
    // Don't do anything with tasks that have recently finished; notifyStatus will handle it.
    if (entry != null && !entry.isComplete) {
      final Task task = entry.taskInfo.getTask();

      if (entry.future == null) {
        if (runnerTaskFuture == null) {
          // Task should be running, so run it.
          final boolean taskIsReady;
          try {
            taskIsReady = task.isReady(taskActionClientFactory.create(task));
          }
          catch (Exception e) {
            log.warn(e, "Exception thrown during isReady for task: %s", task.getId());
            final String errorMessage;
            if (e instanceof MaxAllowedLocksExceededException || e instanceof DruidException) {
              errorMessage = e.getMessage();
            } else {
              errorMessage = StringUtils.format(
                  "Encountered error[%s] while waiting for task to be ready. See Overlord logs for more details.",
                  e.getMessage()
              );
            }
            final TaskStatus taskStatus = TaskStatus.failure(task.getId(), errorMessage);
            notifyStatus(entry, taskStatus, taskStatus.getErrorMsg());
            emitTaskCompletionLogsAndMetrics(task, taskStatus);
            return;
          }
          if (taskIsReady) {
            log.info("Asking taskRunner to run task[%s]", task.getId());
            runnerTaskFuture = taskRunner.run(task);
          } else {
            // Task.isReady() can internally lock intervals or segments.
            // We should release them if the task is not ready.
            taskLockbox.unlockAll(task);
            return;
          }
        }
        attachCallbacks(task, runnerTaskFuture);
        entry.future = runnerTaskFuture;
      } else if (isTaskPending(task)) {
        // if the taskFutures contain this task and this task is pending, also let the taskRunner
        // to run it to guarantee it will be assigned to run
        // see https://github.com/apache/druid/pull/6991
        taskRunner.run(task);
      }
    }
  }

  private void shutdownUnknownTaskOnRunner(String taskId)
  {
    try {
      taskRunner.shutdown(taskId, "Task is not present in queue anymore.");
    }
    catch (Exception e) {
      log.warn(e, "TaskRunner failed to clean up task[%s].", taskId);
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
   */
  public boolean add(final Task task)
  {
    // Before adding the task, validate the ID, so it can be safely used in file paths, znodes, etc.
    IdUtils.validateId("Task ID", task.getId());

    if (taskStorage.getTask(task.getId()).isPresent()) {
      throw EntryAlreadyExists.exception("Task[%s] already exists", task.getId());
    }
    validateTaskPayload(task);

    // Set forceTimeChunkLock before adding task spec to taskStorage, so that we can see always consistent task spec.
    task.addToContextIfAbsent(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, lockConfig.isForceTimeChunkLock());
    defaultTaskConfig.getContext().forEach(task::addToContextIfAbsent);
    // Every task shuold use the lineage-based segment allocation protocol unless it is explicitly set to
    // using the legacy protocol.
    task.addToContextIfAbsent(
        SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
        SinglePhaseParallelIndexTaskRunner.DEFAULT_USE_LINEAGE_BASED_SEGMENT_ALLOCATION
    );

    taskContextEnricher.enrichContext(task);

    startStopLock.readLock().lock();

    try {
      Preconditions.checkState(active, "Queue is not active!");
      Preconditions.checkNotNull(task, "task");
      if (activeTasks.size() >= config.getMaxSize()) {
        throw DruidException.forPersona(DruidException.Persona.ADMIN)
                            .ofCategory(DruidException.Category.CAPACITY_EXCEEDED)
                            .build(
                                "Task queue already contains [%d] tasks."
                                + " Retry later or increase 'druid.indexer.queue.maxSize'[%d].",
                                activeTasks.size(), config.getMaxSize()
                            );
      }

      // If this throws with any sort of exception, including TaskExistsException, we don't want to
      // insert the task into our queue. So don't catch it.
      final DateTime insertTime = DateTimes.nowUtc();
      final TaskInfo<Task, TaskStatus> taskInfo = taskStorage.insert(task, TaskStatus.running(task.getId()));
      // Note: the TaskEntry created for this task doesn't actually use the `insertTime` timestamp, it uses a new
      // timestamp created in the ctor. This prevents races from occurring while syncFromStorage() is happening.
      addTaskInternal(taskInfo, insertTime);
      requestManagement();
      return true;
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }

  @GuardedBy("startStopLock")
  private void addTaskInternal(final TaskInfo<Task, TaskStatus> taskInfo, final DateTime updateTime)
  {
    final AtomicBoolean added = new AtomicBoolean(false);
    final TaskEntry entry = addOrUpdateTaskEntry(
        taskInfo.getId(),
        prevEntry -> {
          if (prevEntry == null) {
            added.set(true);
            return new TaskEntry(taskInfo);
          } else if (prevEntry.lastUpdatedTime.isBefore(updateTime)) {
            // Ensure we keep the current status up-to-date
            prevEntry.updateStatus(taskInfo.getStatus(), updateTime);
          }

          return prevEntry;
        }
    );

    if (added.get()) {
      taskLockbox.add(taskInfo.getTask());
    } else if (!entry.taskInfo.getTask().equals(taskInfo.getTask())) {
      throw new ISE("Cannot add task[%s] as a different task for the same ID has already been added.", taskInfo.getTask().getId());
    }
  }

  /**
   * Removes a task from {@link #activeTasks} and {@link #taskLockbox}, if required.
   * <p>
   * This method must be called only from {@link #syncFromStorage()} to avoid
   * race conditions. For example, if a task finishes and is removed from
   * {@link #activeTasks} while a sync is in progress, the polled results from
   * the DB would still have the task as RUNNING and {@link #syncFromStorage()}
   * might add it back to the queue, thus causing a duplicate run of the task.
   */
  @GuardedBy("startStopLock")
  private boolean removeTaskInternal(final String taskId, final DateTime deleteTime)
  {
    final AtomicReference<TaskInfo<Task, TaskStatus>> removedTask = new AtomicReference<>();

    addOrUpdateTaskEntry(
        taskId,
        prevEntry -> {
          // Remove the task only if it is complete OR it doesn't have a more recent update
          if (prevEntry != null && (prevEntry.isComplete || prevEntry.lastUpdatedTime.isBefore(deleteTime))) {
            removedTask.set(prevEntry.taskInfo);
            // Remove this taskId from activeTasks by mapping it to null
            return null;
          }
          // Preserve this taskId by returning the same reference
          return prevEntry;
        }
    );

    if (removedTask.get() != null) {
      removeTaskLock(removedTask.get().getTask());
      return true;
    }
    return false;
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
    Preconditions.checkNotNull(taskId, "taskId");
    startStopLock.readLock().lock();
    try {
      updateTaskEntry(
          taskId,
          entry -> notifyStatus(
              entry,
              TaskStatus.failure(taskId, StringUtils.format(reasonFormat, args)),
              reasonFormat,
              args
          )
      );
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }

  /**
   * Shuts down a task, but records the task status as a success, unike {@link #shutdown(String, String, Object...)}
   *
   * @param taskId task to shut down
   * @param reasonFormat A format string indicating the shutdown reason
   * @param args arguments for reasonFormat
   */
  public void shutdownWithSuccess(final String taskId, String reasonFormat, Object... args)
  {
    Preconditions.checkNotNull(taskId, "taskId");
    startStopLock.readLock().lock();
    try {
      updateTaskEntry(
          taskId,
          entry -> notifyStatus(entry, TaskStatus.success(taskId), reasonFormat, args)
      );
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }

  /**
   * Notifies this queue that the given task has an updated status. If this update
   * is valid and task is now complete, the following operations are performed:
   * <ul>
   * <li>Mark task as completed to prevent re-launching it</li>
   * <li>Persist new status in the metadata storage to safeguard against crashes
   * and leader re-elections</li>
   * <li>Request {@link #taskRunner} to shutdown task (synchronously)</li>
   * <li>Remove all locks for task from metadata storage</li>
   * <li>Request task management</li>
   * </ul>
   * This method does not remove the task from {@link #activeTasks} to avoid
   * race conditions with {@link #syncFromStorage()}.
   * <p>
   * Since this operation is intended to be performed under one of activeTasks hash segment locks, involves DB updates
   * and synchronous remote calls, it must be invoked on a dedicated executor so that task runner and worker sync
   * are not blocked.
   *
   * @throws NullPointerException     if task or status is null
   * @throws IllegalArgumentException if the task ID does not match the status ID
   * @throws IllegalStateException    if this queue is currently shut down
   * @see #removeTaskInternal
   */
  private void notifyStatus(final TaskEntry entry, final TaskStatus taskStatus, String reasonFormat, Object... args)
  {
    // Don't do anything if the task has no entry in activeTasks
    if (entry == null) {
      return;
    }

    final Task task = entry.taskInfo.getTask();
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
    } else if (entry.isComplete) {
      // A callback() or shutdown() beat us to updating the status and has already cleaned up this task
      log.info("Ignoring notification with status[%s] for already completed task[%s]", taskStatus, task.getId());
      return;
    }

    // Mark this task as complete, so it isn't managed while being cleaned up.
    entry.isComplete = true;
    // Update the task status associated with this entry
    entry.taskInfo = entry.taskInfo.withStatus(taskStatus);

    final TaskLocation taskLocation = taskRunner.getTaskLocation(task.getId());

    // Save status to metadata store first, so if we crash while doing the rest of the shutdown, our successor
    // remembers that this task has completed.
    try {
      // The code block is only called when a task completes,
      // and we need to check to make sure the metadata store has the correct status stored.
      final Optional<TaskStatus> previousStatus = taskStorage.getStatus(task.getId());
      if (!previousStatus.isPresent() || previousStatus.get().isComplete()) {
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
      // If task runner shutdown fails, continue with the task shutdown routine.
      log.warn(e, "TaskRunner failed to cleanup task after completion: %s", task.getId());
    }

    removeTaskLock(task);
    requestManagement();

    log.info("Completed notifyStatus for task[%s] with status[%s]", task.getId(), taskStatus);
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
    Futures.addCallback(
        statusFuture,
        new FutureCallback<>()
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
                FAILED_TO_RUN_TASK_SEE_OVERLORD_MSG
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

              updateTaskEntry(
                  task.getId(),
                  entry -> notifyStatus(entry, status, "notified status change from task")
              );

              // Emit event and log, if the task is done
              emitTaskCompletionLogsAndMetrics(task, status);
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
  @VisibleForTesting
  void syncFromStorage()
  {
    startStopLock.readLock().lock();
    final DateTime syncStartTime = DateTimes.nowUtc();

    try {
      if (active) {
        final Map<String, TaskInfo<Task, TaskStatus>> newTasks =
            CollectionUtils.toMap(taskStorage.getActiveTaskInfos(), (taskInfo) -> taskInfo.getTask().getId(), Function.identity());
        final Map<String, TaskInfo<Task, TaskStatus>> oldTasks =
            CollectionUtils.mapValues(activeTasks, entry -> entry.taskInfo);

        // Identify the tasks that have been added or removed from the storage
        final MapDifference<String, TaskInfo<Task, TaskStatus>> mapDifference = Maps.difference(oldTasks, newTasks);
        final Collection<TaskInfo<Task, TaskStatus>> addedTasks = mapDifference.entriesOnlyOnRight().values();
        final Collection<TaskInfo<Task, TaskStatus>> removedTasks = mapDifference.entriesOnlyOnLeft().values();

        // Remove tasks not present in metadata store if their lastUpdatedTime is before syncStartTime
        int numTasksRemoved = 0;
        for (TaskInfo<Task, TaskStatus> task : removedTasks) {
          if (removeTaskInternal(task.getId(), syncStartTime)) {
            ++numTasksRemoved;
          }
        }

        // Add new tasks present in metadata store if their lastUpdatedTime is before syncStartTime
        for (TaskInfo<Task, TaskStatus> task : addedTasks) {
          addTaskInternal(task, syncStartTime);
        }

        log.info(
            "Synced [%d] tasks from storage (%d tasks added, %d tasks removable, %d tasks removed).",
            newTasks.size(), addedTasks.size(), removedTasks.size(), numTasksRemoved
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
      startStopLock.readLock().unlock();
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

  private Map<RowKey, Long> getDeltaValues(Map<RowKey, Long> total, Map<RowKey, Long> prev)
  {
    final Map<RowKey, Long> deltaValues = new HashMap<>();
    total.forEach(
        (dataSource, totalCount) -> deltaValues.put(
            dataSource,
            totalCount - prev.getOrDefault(dataSource, 0L)
        )
    );
    return deltaValues;
  }

  public Map<RowKey, Long> getSuccessfulTaskCount()
  {
    final Map<RowKey, Long> total = CollectionUtils.mapValues(totalSuccessfulTaskCount, AtomicLong::get);
    synchronized (totalSuccessfulTaskCount) {
      Map<RowKey, Long> delta = getDeltaValues(total, prevTotalSuccessfulTaskCount);
      prevTotalSuccessfulTaskCount = total;
      return delta;
    }
  }

  public Map<RowKey, Long> getFailedTaskCount()
  {
    final Map<RowKey, Long> total = CollectionUtils.mapValues(totalFailedTaskCount, AtomicLong::get);
    synchronized (totalFailedTaskCount) {
      Map<RowKey, Long> delta = getDeltaValues(total, prevTotalFailedTaskCount);
      prevTotalFailedTaskCount = total;
      return delta;
    }
  }

  private Map<String, RowKey> getCurrentTaskDatasources()
  {
    return activeTasks.values().stream()
                      .filter(entry -> !entry.isComplete)
                      .map(entry -> entry.taskInfo.getTask())
                      .collect(Collectors.toMap(Task::getId, TaskQueue::getMetricKey));
  }

  public Map<RowKey, Long> getRunningTaskCount()
  {
    final Map<String, RowKey> taskDatasources = getCurrentTaskDatasources();
    return taskRunner.getRunningTasks()
                     .stream()
                     .collect(Collectors.toMap(
                         e -> taskDatasources.getOrDefault(e.getTaskId(), RowKey.empty()),
                         e -> 1L,
                         Long::sum
                     ));
  }

  public Map<RowKey, Long> getPendingTaskCount()
  {
    final Map<String, RowKey> taskDatasources = getCurrentTaskDatasources();
    return taskRunner.getPendingTasks()
                     .stream()
                     .collect(Collectors.toMap(
                         e -> taskDatasources.getOrDefault(e.getTaskId(), RowKey.empty()),
                         e -> 1L,
                         Long::sum
                     ));
  }

  public Map<RowKey, Long> getWaitingTaskCount()
  {
    Set<String> runnerKnownTaskIds = taskRunner.getKnownTasks()
                                               .stream()
                                               .map(TaskRunnerWorkItem::getTaskId)
                                               .collect(Collectors.toSet());

    return activeTasks.values().stream()
                      .filter(entry -> !entry.isComplete)
                      .map(entry -> entry.taskInfo.getTask())
                      .filter(task -> !runnerKnownTaskIds.contains(task.getId()))
                      .collect(Collectors.toMap(TaskQueue::getMetricKey, task -> 1L, Long::sum));
  }

  /**
   * Gets the current status of this task either from the {@link TaskRunner}
   * or from the {@link TaskStorage} (if not available with the TaskRunner).
   */
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

  /**
   * Returns an optional containing the task payload after successfully redacting credentials.
   * Returns an absent optional if there is no task payload corresponding to the taskId in memory.
   * Throws DruidException if password could not be redacted due to serialization / deserialization failure
   */
  public Optional<Task> getActiveTask(String id)
  {
    final Optional<TaskInfo<Task, TaskStatus>> taskInfo = getActiveTaskInfo(id);
    if (!taskInfo.isPresent()) {
      return Optional.absent();
    }

    Task task = taskInfo.get().getTask();
    if (task != null) {
      try {
        // Write and read the value using a mapper with password redaction mixin.
        task = passwordRedactingMapper.readValue(passwordRedactingMapper.writeValueAsString(task), Task.class);
      }
      catch (JsonProcessingException e) {
        log.error(e, "Failed to serialize or deserialize task with id [%s].", task.getId());
        throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                            .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                            .build(e, "Failed to serialize or deserialize task[%s].", task.getId());
      }
    }
    return Optional.fromNullable(task);
  }

  /**
   * Gets the {@link TaskInfo} for the given {@code taskId} from {@link #activeTasks} if present,
   * otherwise returns an empty optional.
   */
  public Optional<TaskInfo<Task, TaskStatus>> getActiveTaskInfo(String taskId)
  {
    final TaskEntry entry = activeTasks.get(taskId);
    if (entry == null) {
      return Optional.absent();
    }
    return Optional.of(entry.taskInfo);
  }

  /**
   * List of all active and completed task infos currently being managed by this TaskQueue.
   */
  public List<TaskInfo<Task, TaskStatus>> getTaskInfos()
  {
    return activeTasks.values().stream().map(entry -> entry.taskInfo).collect(Collectors.toList());
  }

  /**
   * List of all active and completed tasks currently being managed by this TaskQueue.
   */
  public List<Task> getTasks()
  {
    return getTaskInfos().stream().map(TaskInfo::getTask).collect(Collectors.toList());
  }

  /**
   * Returns a map of currently active tasks for the given datasource.
   */
  public Map<String, Task> getActiveTasksForDatasource(String datasource)
  {
    return activeTasks.values().stream().filter(
        entry -> !entry.isComplete
                 && entry.taskInfo.getDataSource().equals(datasource)
    ).map(
        entry -> entry.taskInfo.getTask()
    ).collect(
        Collectors.toMap(Task::getId, Function.identity())
    );
  }

  private void emitTaskCompletionLogsAndMetrics(final Task task, final TaskStatus status)
  {
    if (status.isComplete()) {
      final ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent.builder();
      IndexTaskUtils.setTaskDimensions(metricBuilder, task);
      IndexTaskUtils.setTaskStatusDimensions(metricBuilder, status);

      emitter.emit(metricBuilder.setMetric("task/run/time", status.getDuration()));

      if (status.isSuccess()) {
        Counters.incrementAndGetLong(totalSuccessfulTaskCount, getMetricKey(task));
      } else {
        Counters.incrementAndGetLong(totalFailedTaskCount, getMetricKey(task));
      }

      log.info(
          "Completed task[%s] with status[%s] in [%d]ms.",
          task.getId(), status, status.getDuration()
      );
    }
  }

  private void validateTaskPayload(Task task)
  {
    try {
      String payload = passwordRedactingMapper.writeValueAsString(task);
      if (config.getMaxTaskPayloadSize() != null && config.getMaxTaskPayloadSize().getBytesInInt() < payload.length()) {
        throw InvalidInput.exception(
            "Task[%s] has payload of size[%d] but max allowed size is [%d]. " +
            "Reduce the size of the task payload or increase 'druid.indexer.queue.maxTaskPayloadSize'.",
            task.getId(), payload.length(), config.getMaxTaskPayloadSize()
        );
      } else if (payload.length() > TASK_SIZE_WARNING_THRESHOLD) {
        log.warn(
            "Task[%s] of datasource[%s] has payload size[%d] larger than the recommended maximum[%d]. " +
            "Large task payloads may cause stability issues in the Overlord and may fail while persisting to the metadata store." +
            "Such tasks may be rejected by the Overlord in future Druid versions.",
            task.getId(),
            task.getDataSource(),
            payload.length(),
            TASK_SIZE_WARNING_THRESHOLD
        );
      }
    }
    catch (JsonProcessingException e) {
      log.error(e, "Failed to parse task payload for validation");
      throw DruidException.defensive(
          "Failed to parse task payload for validation"
      );
    }
  }

  /**
   * Performs a thread-safe update on the task entry for the given task ID,
   * and sets the {@link TaskEntry#lastUpdatedTime} to now.
   */
  private void updateTaskEntry(String taskId, Consumer<TaskEntry> updateOperation)
  {
    addOrUpdateTaskEntry(
        taskId,
        existingEntry -> {
          updateOperation.accept(existingEntry);
          if (existingEntry != null) {
            existingEntry.lastUpdatedTime = DateTimes.nowUtc();
          }
          return existingEntry;
        }
    );
  }

  /**
   * Performs a thread-safe upsert operation on the task entry for the given task ID.
   */
  TaskEntry addOrUpdateTaskEntry(String taskId, Function<TaskEntry, TaskEntry> updateOperation)
  {
    startStopLock.readLock().lock();
    try {
      return activeTasks.compute(
          taskId,
          (id, existingEntry) -> updateOperation.apply(existingEntry)
      );
    }
    finally {
      startStopLock.readLock().unlock();
    }
  }

  private void removeTaskLock(Task task)
  {
    taskLockbox.remove(task);
  }

  /**
   * Represents an entry in this task queue.
   */
  static class TaskEntry
  {
    private TaskInfo<Task, TaskStatus> taskInfo;

    private DateTime lastUpdatedTime;
    private ListenableFuture<TaskStatus> future = null;
    private boolean isComplete = false;

    TaskEntry(TaskInfo<Task, TaskStatus> taskInfo)
    {
      this.taskInfo = taskInfo;
      this.lastUpdatedTime = DateTimes.nowUtc();
    }

    /**
     * Updates the {@link TaskStatus} for the task associated with this {@link TaskEntry} and sets the corresponding
     * update time.
     */
    public void updateStatus(TaskStatus status, DateTime updateTime)
    {
      this.taskInfo.withStatus(status);
      this.lastUpdatedTime = updateTime;
    }
  }

  private static RowKey getMetricKey(final Task task)
  {
    if (task == null) {
      return RowKey.empty();
    }
    return RowKey.with(Dimension.DATASOURCE, task.getDataSource())
                 .and(Dimension.TASK_TYPE, task.getType());
  }
}
