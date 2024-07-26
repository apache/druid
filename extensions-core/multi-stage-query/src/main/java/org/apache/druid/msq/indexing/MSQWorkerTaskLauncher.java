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
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.client.indexing.TaskStatusResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.exec.MSQTasks;
import org.apache.druid.msq.exec.RetryCapableWorkerManager;
import org.apache.druid.msq.exec.WorkerFailureListener;
import org.apache.druid.msq.exec.WorkerStats;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TaskStartTimeoutFault;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForJob;
import org.apache.druid.msq.indexing.error.TooManyAttemptsForWorker;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.rpc.indexing.OverlordClient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Like {@link org.apache.druid.indexing.common.task.batch.parallel.TaskMonitor}, but different.
 */
public class MSQWorkerTaskLauncher implements RetryCapableWorkerManager
{
  private static final Logger log = new Logger(MSQWorkerTaskLauncher.class);
  private static final long HIGH_FREQUENCY_CHECK_MILLIS = 100;
  private static final long LOW_FREQUENCY_CHECK_MILLIS = 2000;
  private static final long SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS = 10000;
  private static final long SHUTDOWN_TIMEOUT_MILLIS = Duration.ofMinutes(1).toMillis();
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
  private final OverlordClient overlordClient;
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

  // Task ID -> worker number. Only set for active workers.
  @GuardedBy("taskIds")
  private final Map<String, Integer> taskIdToWorkerNumber = new HashMap<>();

  // Worker number -> whether the task has fully started up or not.
  @GuardedBy("taskIds")
  private final IntSet fullyStartedTasks = new IntOpenHashSet();

  // Mutable state written only by the mainLoop() thread.
  // Tasks are added here once they are submitted for running, but before they are fully started up.
  // Uses a concurrent map because getWorkerStats() reads this map too, and getWorkerStats() can be called by various
  // other threads.
  private final ConcurrentHashMap<String, TaskTracker> taskTrackers = new ConcurrentHashMap<>();

  // Set of tasks which are issued a cancel request by the controller.
  private final Set<String> canceledWorkerTasks = ConcurrentHashMap.newKeySet();

  private final Map<String, Object> taskContextOverrides;

  // tasks to clean up due to retries
  private final Set<String> tasksToCleanup = ConcurrentHashMap.newKeySet();

  // workers to relaunch
  private final Set<Integer> workersToRelaunch = ConcurrentHashMap.newKeySet();

  // workers that failed, but without active work orders. There is no need to retry these unless a future stage
  // requires it.
  @GuardedBy("taskIds")
  private final Set<Integer> failedInactiveWorkers = ConcurrentHashMap.newKeySet();

  private final ConcurrentHashMap<Integer, List<String>> workerToTaskIds = new ConcurrentHashMap<>();
  private final WorkerFailureListener workerFailureListener;

  private final AtomicLong recentFullyStartedWorkerTimeInMillis = new AtomicLong(System.currentTimeMillis());

  public MSQWorkerTaskLauncher(
      final String controllerTaskId,
      final String dataSource,
      final OverlordClient overlordClient,
      final WorkerFailureListener workerFailureListener,
      final Map<String, Object> taskContextOverrides,
      final long maxTaskStartDelayMillis
  )
  {
    this.controllerTaskId = controllerTaskId;
    this.dataSource = dataSource;
    this.overlordClient = overlordClient;
    this.taskContextOverrides = taskContextOverrides;
    this.exec = Execs.singleThreaded(
        "multi-stage-query-task-launcher[" + StringUtils.encodeForFormat(controllerTaskId) + "]-%s"
    );
    this.workerFailureListener = workerFailureListener;
    this.maxTaskStartDelayMillis = maxTaskStartDelayMillis;
  }

  @Override
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

  @Override
  public void stop(final boolean interrupt)
  {
    if (state.compareAndSet(State.NEW, State.STOPPED)) {
      state.set(State.STOPPED);
      if (interrupt) {
        cancelTasksOnStop.set(true);
      }

      synchronized (taskIds) {
        // Wake up sleeping mainLoop.
        taskIds.notifyAll();
      }
      exec.shutdown();
      stopFuture.set(null);
    } else if (state.compareAndSet(State.STARTED, State.STOPPED)) {
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

  @Override
  public List<String> getWorkerIds()
  {
    synchronized (taskIds) {
      return ImmutableList.copyOf(taskIds);
    }
  }

  @Override
  public void launchWorkersIfNeeded(final int taskCount) throws InterruptedException
  {
    synchronized (taskIds) {
      retryInactiveTasksIfNeeded(taskCount);

      if (taskCount > desiredTaskCount) {
        desiredTaskCount = taskCount;
        taskIds.notifyAll();
      }

      while (taskIds.size() < taskCount || !allTasksStarted(taskCount)) {
        if (stopFuture.isDone() || stopFuture.isCancelled()) {
          FutureUtils.getUnchecked(stopFuture, false);
          throw new ISE("Stopped");
        }

        taskIds.wait();
      }
    }
  }

  public void retryInactiveTasksIfNeeded(int taskCount)
  {
    synchronized (taskIds) {
      // Fetch the list of workers which failed without work orders
      Iterator<Integer> iterator = failedInactiveWorkers.iterator();
      while (iterator.hasNext()) {
        Integer workerNumber = iterator.next();
        // If the controller expects the task to be running, queue it for retry
        if (workerNumber < taskCount) {
          submitForRelaunch(workerNumber);
          iterator.remove();
        }
      }
    }
  }

  Set<Integer> getWorkersToRelaunch()
  {
    return workersToRelaunch;
  }

  @Override
  public void submitForRelaunch(int workerNumber)
  {
    workersToRelaunch.add(workerNumber);
  }

  @Override
  public void reportFailedInactiveWorker(int workerNumber)
  {
    synchronized (taskIds) {
      failedInactiveWorkers.add(workerNumber);
    }
  }

  @Override
  public void waitForWorkers(Set<Integer> workerNumbers) throws InterruptedException
  {
    synchronized (taskIds) {
      while (!fullyStartedTasks.containsAll(workerNumbers)) {
        if (stopFuture.isDone() || stopFuture.isCancelled()) {
          FutureUtils.getUnchecked(stopFuture, false);
          throw new ISE("Stopped");
        }
        taskIds.wait();
      }
    }
  }

  @Override
  public boolean isTaskCanceledByController(String taskId)
  {
    return canceledWorkerTasks.contains(taskId);
  }

  @Override
  public int getWorkerNumber(String taskId)
  {
    return MSQTasks.workerFromTaskId(taskId);
  }

  @Override
  public boolean isWorkerActive(String taskId)
  {
    synchronized (taskIds) {
      return taskIdToWorkerNumber.get(taskId) != null;
    }
  }

  @Override
  public Map<Integer, List<WorkerStats>> getWorkerStats()
  {
    final Int2ObjectMap<List<WorkerStats>> workerStats = new Int2ObjectAVLTreeMap<>();

    for (Map.Entry<String, TaskTracker> taskEntry : taskTrackers.entrySet()) {
      final TaskTracker taskTracker = taskEntry.getValue();
      final TaskStatus taskStatus = taskTracker.statusRef.get();

      // taskStatus is null when TaskTrackers are first set up, and stay null until the first status call comes back.
      final TaskState statusCode = taskStatus != null ? taskStatus.getStatusCode() : null;

      // getDuration() returns -1 for running tasks. It's not calculated on-the-fly here since
      // taskTracker.startTimeMillis marks task submission time rather than the actual start.
      final long duration = taskStatus != null ? taskStatus.getDuration() : -1;

      workerStats.computeIfAbsent(taskTracker.workerNumber, k -> new ArrayList<>())
                 .add(new WorkerStats(taskEntry.getKey(), statusCode, duration, taskTracker.taskPendingTimeInMs()));
    }

    for (List<WorkerStats> workerStatsList : workerStats.values()) {
      workerStatsList.sort(Comparator.comparing(WorkerStats::getWorkerId));
    }
    return workerStats;
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
          log.warn(e, "Stopped due to exception in task management loop.");
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

        if (now > stopStartTime + SHUTDOWN_TIMEOUT_MILLIS) {
          if (caught != null) {
            throw caught;
          } else {
            throw new ISE("Task shutdown timed out (limit = %,dms)", SHUTDOWN_TIMEOUT_MILLIS);
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

      FutureUtils.getUnchecked(overlordClient.runTask(task.getId(), task), true);

      synchronized (taskIds) {
        taskIdToWorkerNumber.put(task.getId(), taskIds.size());
        taskIds.add(task.getId());
        taskIds.notifyAll();
      }
    }
  }

  /**
   * Returns a pair which contains the number of currently running worker tasks and the number of worker tasks that are
   * not yet fully started as left and right respectively.
   */
  @Override
  public WorkerCount getWorkerCount()
  {
    synchronized (taskIds) {
      if (stopFuture.isDone()) {
        return new WorkerCount(0, 0);
      } else {
        int runningTasks = fullyStartedTasks.size();
        int pendingTasks = desiredTaskCount - runningTasks;
        return new WorkerCount(runningTasks, pendingTasks);
      }
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
      final Map<String, TaskStatus> statuses =
          FutureUtils.getUnchecked(overlordClient.taskStatuses(taskStatusesNeeded), true);

      for (Map.Entry<String, TaskStatus> statusEntry : statuses.entrySet()) {
        final String taskId = statusEntry.getKey();
        final TaskTracker tracker = taskTrackers.get(taskId);
        tracker.updateStatus(statusEntry.getValue());
        TaskStatus status = tracker.statusRef.get();

        if (!status.getStatusCode().isComplete() && tracker.unknownLocation()) {
          // Look up location if not known. Note: this location is not used to actually contact the task. For that,
          // we have SpecificTaskServiceLocator. This location is only used to determine if a task has started up.
          final TaskStatusResponse taskStatusResponse =
              FutureUtils.getUnchecked(overlordClient.taskStatus(taskId), true);
          if (taskStatusResponse.getStatus() != null) {
            tracker.setLocation(taskStatusResponse.getStatus().getLocation());
          } else {
            tracker.setLocation(TaskLocation.unknown());
          }
        }

        if (status.getStatusCode() == TaskState.RUNNING && !tracker.unknownLocation()) {
          synchronized (taskIds) {
            if (fullyStartedTasks.add(tracker.workerNumber)) {
              recentFullyStartedWorkerTimeInMillis.set(System.currentTimeMillis());
              tracker.setFullyStartedTime(System.currentTimeMillis());
            }
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

    for (Map.Entry<String, TaskTracker> taskEntry : taskTrackersByWorkerNumber()) {
      final String taskId = taskEntry.getKey();
      final TaskTracker tracker = taskEntry.getValue();
      if (tracker.isRetrying()) {
        continue;
      }

      if (tracker.statusRef.get() == null) {
        removeWorkerFromFullyStartedWorkers(tracker);
        final String errorMessage = StringUtils.format("Task [%s] status missing", taskId);
        log.info(errorMessage + ". Trying to relaunch the worker");
        tracker.enableRetrying();
        workerFailureListener.onFailure(
            tracker.msqWorkerTask,
            UnknownFault.forMessage(errorMessage)
        );

      } else if (tracker.didRunTimeOut(maxTaskStartDelayMillis) && !canceledWorkerTasks.contains(taskId)) {
        removeWorkerFromFullyStartedWorkers(tracker);
        throw new MSQException(new TaskStartTimeoutFault(
            this.getWorkerCount().getPendingWorkerCount(),
            numTasks + 1,
            maxTaskStartDelayMillis
        ));
      } else if (tracker.didFail() && !canceledWorkerTasks.contains(taskId)) {
        removeWorkerFromFullyStartedWorkers(tracker);
        TaskStatus taskStatus = tracker.statusRef.get();
        log.info("Task[%s] failed because %s. Trying to relaunch the worker", taskId, taskStatus.getErrorMsg());
        tracker.enableRetrying();
        workerFailureListener.onFailure(
            tracker.msqWorkerTask,
            new WorkerFailedFault(taskId, taskStatus.getErrorMsg())
        );
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

        FutureUtils.getUnchecked(overlordClient.runTask(relaunchedTask.getId(), relaunchedTask), true);
        taskHistory.add(relaunchedTask.getId());

        synchronized (taskIds) {
          // replace taskId with the retry taskID for the same worker number
          taskIdToWorkerNumber.remove(taskIds.get(toRelaunch.getWorkerNumber()));
          taskIds.set(toRelaunch.getWorkerNumber(), relaunchedTask.getId());
          taskIdToWorkerNumber.put(relaunchedTask.getId(), toRelaunch.getWorkerNumber());
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
          tracker.statusRef.get().getErrorMsg()
      ));
    }
    if (currentRelaunchCount > Limits.TOTAL_RELAUNCH_LIMIT) {
      throw new MSQException(new TooManyAttemptsForJob(
          Limits.TOTAL_RELAUNCH_LIMIT,
          currentRelaunchCount,
          relaunchTask.getId(),
          tracker.statusRef.get().getErrorMsg()
      ));
    }
  }

  private void shutDownTasks()
  {

    cleanFailedTasksWhichAreRelaunched();
    for (final Map.Entry<String, TaskTracker> taskEntry : taskTrackersByWorkerNumber()) {
      final String taskId = taskEntry.getKey();
      final TaskTracker tracker = taskEntry.getValue();
      if ((!canceledWorkerTasks.contains(taskId))
          &&
          (!tracker.isComplete())) {
        canceledWorkerTasks.add(taskId);
        FutureUtils.getUnchecked(overlordClient.cancelTask(taskId), true);
      }
    }
  }

  /**
   * Cleans the task identified in {@link MSQWorkerTaskLauncher#relaunchTasks()} for relaunch. Asks the overlord to cancel the task.
   */
  private void cleanFailedTasksWhichAreRelaunched()
  {
    Iterator<String> tasksToCancel = tasksToCleanup.iterator();
    while (tasksToCancel.hasNext()) {
      String taskId = tasksToCancel.next();
      try {
        if (canceledWorkerTasks.add(taskId)) {
          try {
            FutureUtils.getUnchecked(overlordClient.cancelTask(taskId), true);
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
   * Whether {@link #fullyStartedTasks} contains all tasks from 0 (inclusive) to taskCount (exclusive).
   */
  @GuardedBy("taskIds")
  private boolean allTasksStarted(final int taskCount)
  {
    for (int i = 0; i < taskCount; i++) {
      if (!fullyStartedTasks.contains(i)) {
        return false;
      }
    }

    return true;
  }

  /**
   * Returns entries of {@link #taskTrackers} sorted by worker number.
   */
  private List<Map.Entry<String, TaskTracker>> taskTrackersByWorkerNumber()
  {
    return taskTrackers.entrySet()
                       .stream()
                       .sorted(Comparator.comparing(entry -> entry.getValue().workerNumber))
                       .collect(Collectors.toList());
  }

  /**
   * Used by the main loop to decide how often to check task status.
   */
  private long computeSleepTime(final long loopDurationMillis)
  {
    final OptionalLong maxTaskStartTime =
        taskTrackers.values().stream().mapToLong(tracker -> tracker.startTimeMillis).max();

    if (maxTaskStartTime.isPresent() &&
        System.currentTimeMillis() - maxTaskStartTime.getAsLong() < SWITCH_TO_LOW_FREQUENCY_CHECK_AFTER_MILLIS) {
      return HIGH_FREQUENCY_CHECK_MILLIS - loopDurationMillis;
    } else {
      return LOW_FREQUENCY_CHECK_MILLIS - loopDurationMillis;
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
  private class TaskTracker
  {
    private final int workerNumber;
    private final long startTimeMillis = System.currentTimeMillis();
    private final AtomicLong taskFullyStartedTimeRef = new AtomicLong();
    private final MSQWorkerTask msqWorkerTask;
    private final AtomicReference<TaskStatus> statusRef = new AtomicReference<>();
    private final AtomicReference<TaskLocation> initialLocationRef = new AtomicReference<>();

    private final AtomicBoolean isRetryingRef = new AtomicBoolean(false);

    public TaskTracker(int workerNumber, MSQWorkerTask msqWorkerTask)
    {
      this.workerNumber = workerNumber;
      this.msqWorkerTask = msqWorkerTask;
    }

    public boolean unknownLocation()
    {
      TaskLocation initialLocation = initialLocationRef.get();
      return initialLocation == null || TaskLocation.unknown().equals(initialLocation);
    }

    public boolean isComplete()
    {
      TaskStatus status = statusRef.get();
      return status != null && status.getStatusCode().isComplete();
    }

    public boolean didFail()
    {
      TaskStatus status = statusRef.get();
      return status != null && status.getStatusCode().isFailure();
    }

    /**
     * Checks if the task has timed out if all the following conditions are true:
     * 1. The task is still running.
     * 2. The location has never been reported by the task. If this is not the case, the task has started already.
     * 3. Task has taken more than maxTaskStartDelayMillis to start.
     * 4. No task has started in maxTaskStartDelayMillis. This is in case the cluster is scaling up and other workers
     * are starting.
     */
    public boolean didRunTimeOut(final long maxTaskStartDelayMillis)
    {
      long currentTimeMillis = System.currentTimeMillis();
      TaskStatus status = statusRef.get();
      return (status == null || status.getStatusCode() == TaskState.RUNNING)
             && unknownLocation()
             && currentTimeMillis - recentFullyStartedWorkerTimeInMillis.get() > maxTaskStartDelayMillis
             && currentTimeMillis - startTimeMillis > maxTaskStartDelayMillis;
    }

    /**
     * Enables retrying for the task
     */
    public void enableRetrying()
    {
      isRetryingRef.set(true);
    }

    /**
     * Checks is the task is retrying,
     */
    public boolean isRetrying()
    {
      return isRetryingRef.get();
    }

    public void setLocation(TaskLocation taskLocation)
    {
      initialLocationRef.set(taskLocation);
    }

    public void updateStatus(TaskStatus taskStatus)
    {
      statusRef.set(taskStatus);
    }

    public void setFullyStartedTime(long currentTimeMillis)
    {
      taskFullyStartedTimeRef.set(currentTimeMillis);
    }

    public long taskPendingTimeInMs()
    {
      long currentFullyStartingTime = taskFullyStartedTimeRef.get();
      if (currentFullyStartingTime == 0) {
        return System.currentTimeMillis() - startTimeMillis;
      } else {
        return Math.max(0, currentFullyStartingTime - startTimeMillis);
      }
    }
  }
}
