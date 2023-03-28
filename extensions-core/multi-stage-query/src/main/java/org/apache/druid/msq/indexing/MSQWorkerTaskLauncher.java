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

package org.apache.druid.msq.indexing;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.ControllerImpl;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.exec.WorkerManagerClient;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TaskStartTimeoutFault;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForJob;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForWorker;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * Like {@link org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor}, but different.
 */
public class MSQWorkerTaskLauncher
{
  private static final Logger log = new Logger(MSQWorkerTaskLauncher.class);
  private static final long HIGH_FREQUENCY_CHECK_MILLIS = 100;
  private static final long LOW_FREQUENCY_CHECK_MILLIS = 2000;
  private static final long SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS = 10000;
  private static final long SHUTDOWN_TIMEOUT_MS = Duration.ofMinutes(1).toMillis();
  private int currentRelaunchCount = 0;

  // States for "state" variable.
  private enum State
  {
    NEW,
    STARTED,
    STOPPED
  }

  private final String controllerTaskId;
  private final String dataSource;
  private final ControllerContext context;
  private final ExecutorService exec;
  private final long maxTaskStartDelayMillis;

  // Mutable state meant to be accessible by threads outside the main loop.
  private final SettableFuture<?> stopFuture = SettableFuture.create();
  private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
  private final AtomicBoolean cancelTasksOnStop = new AtomicBoolean();

  // Set by launchTasksIfNeeded.
  @GuardedBy("taskIds")
  private int desiredTaskCount = 0;

  // Set by the main loop when it acknowledges a new desiredTaskCount.
  @GuardedBy("taskIds")
  private int acknowledgedDesiredTaskCount = 0;

  // Worker number -> task ID.
  @GuardedBy("taskIds")
  private final List<String> taskIds = new ArrayList<>();

  // Worker number -> whether the task has fully started up or not.
  @GuardedBy("taskIds")
  private final IntSet fullyStartedTasks = new IntOpenHashSet();

  // Mutable state accessible only to the main loop. LinkedHashMap since order of key set matters. Tasks are added
  // here once they are submitted for running, but before they are fully started up.
  // taskId -> taskTracker
  private final Map<String, TaskTracker> taskTrackers = new LinkedHashMap<>();

  // Set of tasks which are issued a cancel request by the controller.
  private final Set<String> canceledWorkerTasks = ConcurrentHashMap.newKeySet();

  private final Map<String, Object> taskContextOverrides;

  // tasks to clean up due to retries
  private final Set<String> tasksToCleanup = ConcurrentHashMap.newKeySet();

  // workers to relaunch
  private final Set<Integer> workersToRelaunch = ConcurrentHashMap.newKeySet();

  private final ConcurrentHashMap<Integer, List<String>> workerToTaskIds = new ConcurrentHashMap<>();
  private final RetryTask retryTask;

  public MSQWorkerTaskLauncher(
      final String controllerTaskId,
      final String dataSource,
      final ControllerContext context,
      final RetryTask retryTask,
      final Map<String, Object> taskContextOverrides,
      final long maxTaskStartDelayMillis
  )
  {
    this.controllerTaskId = controllerTaskId;
    this.dataSource = dataSource;
    this.context = context;
    this.taskContextOverrides = taskContextOverrides;
    this.exec = Execs.singleThreaded(
        "multi-stage-query-task-launcher[" + StringUtils.encodeForFormat(controllerTaskId) + "]-%s"
    );
    this.retryTask = retryTask;
    this.maxTaskStartDelayMillis = maxTaskStartDelayMillis;
  }

  /**
   * Launches tasks, blocking until they are all in RUNNING state. Returns a future that resolves successfully when
   * all tasks end successfully or are canceled. The returned future resolves to an exception if one of the tasks fails
   * without being explicitly canceled, or if something else goes wrong.
   */
  public ListenableFuture<?> start()
  {
    if (state.compareAndSet(State.NEW, State.STARTED)) {
      exec.submit(() -> {
        try {
          mainLoop();
        }
        catch (Throwable e) {
          log.warn(e, "Error encountered in main loop. Abandoning worker tasks.");
        }
      });
    }

    // Return an "everything is done" future that callers can wait for.
    return stopFuture;
  }

  /**
   * Stops all tasks, blocking until they exit. Returns quietly, no matter whether there was an exception
   * associated with the future from {@link #start()} or not.
   */
  public void stop(final boolean interrupt)
  {
    if (state.compareAndSet(State.STARTED, State.STOPPED)) {
      if (interrupt) {
        cancelTasksOnStop.set(true);
      }

      synchronized (taskIds) {
        // Wake up sleeping mainLoop.
        taskIds.notifyAll();
      }

      // Only shutdown the executor when transitioning from STARTED.
      exec.shutdown();
    } else if (state.get() == State.STOPPED) {
      // interrupt = true is sticky: don't reset on interrupt = false.
      if (interrupt) {
        cancelTasksOnStop.set(true);
      }
    } else {
      throw new ISE("Cannot stop(%s) from state [%s]", interrupt, state.get());
    }

    // Block until stopped.
    try {
      FutureUtils.getUnchecked(stopFuture, false);
    }
    catch (Throwable ignored) {
      // Suppress.
    }
  }

  /**
   * Get the list of currently-active tasks.
   */
  public List<String> getActiveTasks()
  {
    synchronized (taskIds) {
      return ImmutableList.copyOf(taskIds);
    }
  }

  /**
   * Launch additional tasks, if needed, to bring the size of {@link #taskIds} up to {@code taskCount}. If enough
   * tasks are already running, this method does nothing.
   */
  public void launchTasksIfNeeded(final int taskCount) throws InterruptedException
  {
    synchronized (taskIds) {
      if (taskCount > desiredTaskCount) {
        desiredTaskCount = taskCount;
        taskIds.notifyAll();
      }

      while (taskIds.size() < taskCount || !IntStream.range(0, taskCount).allMatch(fullyStartedTasks::contains)) {
        if (stopFuture.isDone() || stopFuture.isCancelled()) {
          FutureUtils.getUnchecked(stopFuture, false);
          throw new ISE("Stopped");
        }

        taskIds.wait();
      }
    }
  }

  /**
   * Queues worker for relaunch. A noop if the worker is already in the queue.
   *
   * @param workerNumber
   */
  public void submitForRelaunch(int workerNumber)
  {
    workersToRelaunch.add(workerNumber);
  }

  /**
   * Blocks the call untill the worker tasks are ready to be contacted for work.
   *
   * @param workerSet
   * @throws InterruptedException
   */
  public void waitUntilWorkersReady(Set<Integer> workerSet) throws InterruptedException
  {
    synchronized (taskIds) {
      while (!fullyStartedTasks.containsAll(workerSet)) {
        if (stopFuture.isDone() || stopFuture.isCancelled()) {
          FutureUtils.getUnchecked(stopFuture, false);
          throw new ISE("Stopped");
        }
        taskIds.wait();
      }
    }
  }


  /**
   * Checks if the controller has canceled the input taskId. This method is used in {@link ControllerImpl}
   * to figure out if the worker taskId is canceled by the controller. If yes, the errors from that worker taskId
   * are ignored for the error reports.
   *
   * @return true if task is canceled by the controller, else false
   */
  public boolean isTaskCanceledByController(String taskId)
  {
    return canceledWorkerTasks.contains(taskId);
  }


  public boolean isTaskLatest(String taskId)
  {
    int worker = MSQTasks.workerFromTaskId(taskId);
    synchronized (taskIds) {
      return taskId.equals(taskIds.get(worker));
    }
  }

  private void mainLoop()
  {
    try {
      Throwable caught = null;

      while (state.get() == State.STARTED) {
        final long loopStartTime = System.currentTimeMillis();

        try {
          runNewTasks();
          updateTaskTrackersAndTaskIds();
          checkForErroneousTasks();
          relaunchTasks();
          cleanFailedTasksWhichAreRelaunched();
        }
        catch (Throwable e) {
          state.set(State.STOPPED);
          cancelTasksOnStop.set(true);
          caught = e;
          break;
        }

        // Sleep for a bit, maybe.
        sleep(computeSleepTime(System.currentTimeMillis() - loopStartTime), false);
      }

      // Only valid transition out of STARTED.
      assert state.get() == State.STOPPED;

      final long stopStartTime = System.currentTimeMillis();

      while (taskTrackers.values().stream().anyMatch(tracker -> !tracker.isComplete())) {
        final long loopStartTime = System.currentTimeMillis();

        if (cancelTasksOnStop.get()) {
          shutDownTasks();
        }

        updateTaskTrackersAndTaskIds();

        // Sleep for a bit, maybe.
        final long now = System.currentTimeMillis();

        if (now > stopStartTime + SHUTDOWN_TIMEOUT_MS) {
          if (caught != null) {
            throw caught;
          } else {
            throw new ISE("Task shutdown timed out (limit = %,dms)", SHUTDOWN_TIMEOUT_MS);
          }
        }

        sleep(computeSleepTime(now - loopStartTime), true);
      }

      if (caught != null) {
        throw caught;
      }

      stopFuture.set(null);
    }
    catch (Throwable e) {
      if (!stopFuture.isDone()) {
        stopFuture.setException(e);
      }
    }

    synchronized (taskIds) {
      // notify taskIds so launchWorkersIfNeeded can wake up, if it is sleeping, and notice stopFuture is done.
      taskIds.notifyAll();
    }
  }

  /**
   * Used by the main loop to launch new tasks up to {@link #desiredTaskCount}. Adds trackers to {@link #taskTrackers}
   * for newly launched tasks.
   */
  private void runNewTasks()
  {
    final Map<String, Object> taskContext = new HashMap<>();

    for (Map.Entry<String, Object> taskContextOverride : taskContextOverrides.entrySet()) {
      if (taskContextOverride.getKey() == null || taskContextOverride.getValue() == null) {
        continue;
      }
      taskContext.put(taskContextOverride.getKey(), taskContextOverride.getValue());
    }

    final int firstTask;
    final int taskCount;

    synchronized (taskIds) {
      firstTask = taskIds.size();
      taskCount = desiredTaskCount;
      acknowledgedDesiredTaskCount = desiredTaskCount;
    }

    for (int i = firstTask; i < taskCount; i++) {
      final MSQWorkerTask task = new MSQWorkerTask(
          controllerTaskId,
          dataSource,
          i,
          taskContext,
          0
      );

      taskTrackers.put(task.getId(), new TaskTracker(i, task));
      workerToTaskIds.compute(i, (workerId, taskIds) -> {
        if (taskIds == null) {
          taskIds = new ArrayList<>();
        }
        taskIds.add(task.getId());
        return taskIds;
      });

      context.workerManager().run(task.getId(), task);

      synchronized (taskIds) {
        taskIds.add(task.getId());
        taskIds.notifyAll();
      }
    }
  }

  /**
   * Returns a pair which contains the number of currently running worker tasks and the number of worker tasks that are
   * not yet fully started as left and right respectively.
   */
  public WorkerCount getWorkerTaskCount()
  {
    synchronized (taskIds) {
      int runningTasks = fullyStartedTasks.size();
      int pendingTasks = desiredTaskCount - runningTasks;
      return new WorkerCount(runningTasks, pendingTasks);
    }
  }

  /**
   * Used by the main loop to update {@link #taskTrackers} and {@link #fullyStartedTasks}.
   */
  private void updateTaskTrackersAndTaskIds()
  {
    final Set<String> taskStatusesNeeded = new HashSet<>();
    for (final Map.Entry<String, TaskTracker> taskEntry : taskTrackers.entrySet()) {
      if (!taskEntry.getValue().isComplete()) {
        taskStatusesNeeded.add(taskEntry.getKey());
      }
    }

    if (!taskStatusesNeeded.isEmpty()) {
      final WorkerManagerClient workerManager = context.workerManager();
      final Map<String, TaskStatus> statuses = workerManager.statuses(taskStatusesNeeded);

      for (Map.Entry<String, TaskStatus> statusEntry : statuses.entrySet()) {
        final String taskId = statusEntry.getKey();
        final TaskTracker tracker = taskTrackers.get(taskId);
        tracker.status = statusEntry.getValue();

        if (!tracker.status.getStatusCode().isComplete() && tracker.unknownLocation()) {
          // Look up location if not known. Note: this location is not used to actually contact the task. For that,
          // we have SpecificTaskServiceLocator. This location is only used to determine if a task has started up.
          tracker.initialLocation = workerManager.location(taskId);
        }

        if (tracker.status.getStatusCode() == TaskState.RUNNING && !tracker.unknownLocation()) {
          synchronized (taskIds) {
            fullyStartedTasks.add(tracker.workerNumber);
            taskIds.notifyAll();
          }
        }
      }
    }
  }

  /**
   * Used by the main loop to generate exceptions if any tasks have failed, have taken too long to start up, or
   * have gone inexplicably missing.
   * <p>
   * Throws an exception if some task is erroneous.
   */
  private void checkForErroneousTasks()
  {
    final int numTasks = taskTrackers.size();

    Iterator<Map.Entry<String, TaskTracker>> taskTrackerIterator = taskTrackers.entrySet().iterator();

    while (taskTrackerIterator.hasNext()) {
      final Map.Entry<String, TaskTracker> taskEntry = taskTrackerIterator.next();
      final String taskId = taskEntry.getKey();
      final TaskTracker tracker = taskEntry.getValue();
      if (tracker.isRetrying()) {
        continue;
      }

      if (tracker.status == null) {
        removeWorkerFromFullyStartedWorkers(tracker);
        final String errorMessage = StringUtils.format("Task [%s] status missing", taskId);
        log.info(errorMessage + ". Trying to relaunch the worker");
        tracker.enableRetrying();
        retryTask.retry(
            tracker.msqWorkerTask,
            UnknownFault.forMessage(errorMessage)
        );

      } else if (tracker.didRunTimeOut(maxTaskStartDelayMillis) && !canceledWorkerTasks.contains(taskId)) {
        removeWorkerFromFullyStartedWorkers(tracker);
        throw new MSQException(new TaskStartTimeoutFault(numTasks + 1, maxTaskStartDelayMillis));
      } else if (tracker.didFail() && !canceledWorkerTasks.contains(taskId)) {
        removeWorkerFromFullyStartedWorkers(tracker);
        log.info("Task[%s] failed because %s. Trying to relaunch the worker", taskId, tracker.status.getErrorMsg());
        tracker.enableRetrying();
        retryTask.retry(tracker.msqWorkerTask, new WorkerFailedFault(taskId, tracker.status.getErrorMsg()));
      }
    }
  }

  private void removeWorkerFromFullyStartedWorkers(TaskTracker tracker)
  {
    synchronized (taskIds) {
      fullyStartedTasks.remove(tracker.msqWorkerTask.getWorkerNumber());
    }
  }


  private void relaunchTasks()
  {
    Iterator<Integer> iterator = workersToRelaunch.iterator();

    while (iterator.hasNext()) {
      int worker = iterator.next();
      workerToTaskIds.compute(worker, (workerId, taskHistory) -> {

        if (taskHistory == null || taskHistory.isEmpty()) {
          throw new ISE("TaskHistory cannot by null for worker %d", workerId);
        }
        String latestTaskId = taskHistory.get(taskHistory.size() - 1);

        TaskTracker tracker = taskTrackers.get(latestTaskId);
        if (tracker == null) {
          throw new ISE("Did not find taskTracker for latest taskId[%s]", latestTaskId);
        }
        // if task is not failed donot retry
        if (!tracker.isComplete()) {
          return taskHistory;
        }

        MSQWorkerTask toRelaunch = tracker.msqWorkerTask;
        MSQWorkerTask relaunchedTask = toRelaunch.getRetryTask();

        // check relaunch limits
        checkRelaunchLimitsOrThrow(tracker, toRelaunch);
        // clean up trackers and tasks
        tasksToCleanup.add(latestTaskId);
        taskTrackers.remove(latestTaskId);
        log.info(
            "Relaunching worker[%d] with new task id[%s] with worker relaunch count[%d] and job relaunch count[%d]",
            relaunchedTask.getWorkerNumber(),
            relaunchedTask.getId(),
            toRelaunch.getRetryCount(),
            currentRelaunchCount
        );

        currentRelaunchCount += 1;
        taskTrackers.put(relaunchedTask.getId(), new TaskTracker(relaunchedTask.getWorkerNumber(), relaunchedTask));
        synchronized (taskIds) {
          fullyStartedTasks.remove(relaunchedTask.getWorkerNumber());
          taskIds.notifyAll();
        }

        context.workerManager().run(relaunchedTask.getId(), relaunchedTask);
        taskHistory.add(relaunchedTask.getId());

        synchronized (taskIds) {
          // replace taskId with the retry taskID for the same worker number
          taskIds.set(toRelaunch.getWorkerNumber(), relaunchedTask.getId());
          taskIds.notifyAll();
        }
        return taskHistory;

      });
      iterator.remove();
    }
  }

  private void checkRelaunchLimitsOrThrow(TaskTracker tracker, MSQWorkerTask relaunchTask)
  {
    if (relaunchTask.getRetryCount() > Limits.PER_WORKER_RELAUNCH_LIMIT) {
      throw new MSQException(new TooManyAttemptsForWorker(
          Limits.PER_WORKER_RELAUNCH_LIMIT,
          relaunchTask.getId(),
          relaunchTask.getWorkerNumber(),
          tracker.status.getErrorMsg()
      ));
    }
    if (currentRelaunchCount > Limits.TOTAL_RELAUNCH_LIMIT) {
      throw new MSQException(new TooManyAttemptsForJob(
          Limits.TOTAL_RELAUNCH_LIMIT,
          currentRelaunchCount,
          relaunchTask.getId(),
          tracker.status.getErrorMsg()
      ));
    }
  }

  private void shutDownTasks()
  {

    cleanFailedTasksWhichAreRelaunched();
    for (final Map.Entry<String, TaskTracker> taskEntry : taskTrackers.entrySet()) {
      final String taskId = taskEntry.getKey();
      final TaskTracker tracker = taskEntry.getValue();
      if (!canceledWorkerTasks.contains(taskId)
          && (tracker.status == null || !tracker.status.getStatusCode().isComplete())) {
        canceledWorkerTasks.add(taskId);
        context.workerManager().cancel(taskId);
      }
    }

  }

  /**
   * Cleans the task indentified in {@link MSQWorkerTaskLauncher#relaunchTasks()} for relaunch. Asks the overlord to cancel the task.
   */
  private void cleanFailedTasksWhichAreRelaunched()
  {
    Iterator<String> tasksToCancel = tasksToCleanup.iterator();
    while (tasksToCancel.hasNext()) {
      String taskId = tasksToCancel.next();
      try {
        if (canceledWorkerTasks.add(taskId)) {
          try {
            context.workerManager().cancel(taskId);
          }
          catch (Exception ignore) {
            //ignoring cancellation exception
          }
        }
      }
      finally {
        tasksToCancel.remove();
      }

    }
  }

  /**
   * Used by the main loop to decide how often to check task status.
   */
  private long computeSleepTime(final long loopDurationMs)
  {
    final OptionalLong maxTaskStartTime =
        taskTrackers.values().stream().mapToLong(tracker -> tracker.startTimeMs).max();

    if (maxTaskStartTime.isPresent() &&
        System.currentTimeMillis() - maxTaskStartTime.getAsLong() < SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS) {
      return HIGH_FREQUENCY_CHECK_MILLIS - loopDurationMs;
    } else {
      return LOW_FREQUENCY_CHECK_MILLIS - loopDurationMs;
    }
  }

  private void sleep(final long sleepMillis, final boolean shuttingDown) throws InterruptedException
  {
    if (sleepMillis > 0) {
      if (shuttingDown) {
        Thread.sleep(sleepMillis);
      } else {
        // wait on taskIds so we can wake up early if needed.
        synchronized (taskIds) {
          // desiredTaskCount is set by launchTasksIfNeeded, and acknowledgedDesiredTaskCount is set by mainLoop when
          // it acknowledges a new target. If these are not equal, do another run immediately and launch more tasks.
          if (acknowledgedDesiredTaskCount == desiredTaskCount) {
            taskIds.wait(sleepMillis);
          }
        }
      }
    } else {
      // No wait, but check interrupted status anyway.
      if (Thread.interrupted()) {
        throw new InterruptedException();
      }
    }
  }

  /**
   * Tracker for information about a worker. Mutable.
   */
  private static class TaskTracker
  {
    private final int workerNumber;
    private final long startTimeMs = System.currentTimeMillis();
    private final MSQWorkerTask msqWorkerTask;
    private TaskStatus status;
    private TaskLocation initialLocation;

    private boolean isRetrying = false;

    public TaskTracker(int workerNumber, MSQWorkerTask msqWorkerTask)
    {
      this.workerNumber = workerNumber;
      this.msqWorkerTask = msqWorkerTask;
    }

    public boolean unknownLocation()
    {
      return initialLocation == null || TaskLocation.unknown().equals(initialLocation);
    }

    public boolean isComplete()
    {
      return status != null && status.getStatusCode().isComplete();
    }

    public boolean didFail()
    {
      return status != null && status.getStatusCode().isFailure();
    }

    public boolean didRunTimeOut(final long maxTaskStartDelayMillis)
    {
      return (status == null || status.getStatusCode() == TaskState.RUNNING)
             && unknownLocation()
             && System.currentTimeMillis() - startTimeMs > maxTaskStartDelayMillis;
    }

    /**
     * Enables retrying for the task
     */
    public void enableRetrying()
    {
      isRetrying = true;
    }

    /**
     * Checks is the task is retrying,
     *
     * @return
     */
    public boolean isRetrying()
    {
      return isRetrying;
    }
  }
}
