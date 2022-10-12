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
import org.apache.druid.msq.exec.WorkerManagerClient;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.indexing.error.TaskStartTimeoutFault;
import org.apache.druid.msq.indexing.error.UnknownFault;
import org.apache.druid.msq.indexing.error.WorkerFailedFault;
import org.apache.druid.msq.util.MultiStageQueryContext;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
  private final boolean durableStageStorageEnabled;

  @Nullable
  private final Long maxParseExceptions;

  // Mutable state meant to be accessible by threads outside the main loop.
  private final SettableFuture<?> stopFuture = SettableFuture.create();
  private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
  private final AtomicBoolean cancelTasksOnStop = new AtomicBoolean();

  @GuardedBy("taskIds")
  private int desiredTaskCount = 0;

  // Worker number -> task ID.
  @GuardedBy("taskIds")
  private final List<String> taskIds = new ArrayList<>();

  // Worker number -> whether the task has fully started up or not.
  @GuardedBy("taskIds")
  private final IntSet fullyStartedTasks = new IntOpenHashSet();

  // Mutable state accessible only to the main loop. LinkedHashMap since order of key set matters. Tasks are added
  // here once they are submitted for running, but before they are fully started up.
  private final Map<String, TaskTracker> taskTrackers = new LinkedHashMap<>();

  // Set of tasks which are issued a cancel request by the controller.
  private final Set<String> canceledWorkerTasks = ConcurrentHashMap.newKeySet();

  public MSQWorkerTaskLauncher(
      final String controllerTaskId,
      final String dataSource,
      final ControllerContext context,
      final boolean durableStageStorageEnabled,
      @Nullable final Long maxParseExceptions,
      final long maxTaskStartDelayMillis
  )
  {
    this.controllerTaskId = controllerTaskId;
    this.dataSource = dataSource;
    this.context = context;
    this.exec = Execs.singleThreaded(
        "multi-stage-query-task-launcher[" + StringUtils.encodeForFormat(controllerTaskId) + "]-%s"
    );
    this.durableStageStorageEnabled = durableStageStorageEnabled;
    this.maxParseExceptions = maxParseExceptions;
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
  public List<String> getTaskList()
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

    if (durableStageStorageEnabled) {
      taskContext.put(MultiStageQueryContext.CTX_ENABLE_DURABLE_SHUFFLE_STORAGE, true);
    }

    if (maxParseExceptions != null) {
      taskContext.put(MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED, maxParseExceptions);
    }

    final int firstTask;
    final int taskCount;

    synchronized (taskIds) {
      firstTask = taskIds.size();
      taskCount = desiredTaskCount;
    }

    for (int i = firstTask; i < taskCount; i++) {
      final MSQWorkerTask task = new MSQWorkerTask(
          controllerTaskId,
          dataSource,
          i,
          taskContext
      );

      taskTrackers.put(task.getId(), new TaskTracker(i));
      context.workerManager().run(task.getId(), task);

      synchronized (taskIds) {
        taskIds.add(task.getId());
        taskIds.notifyAll();
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
   *
   * Throws an exception if some task is erroneous.
   */
  private void checkForErroneousTasks()
  {
    final int numTasks = taskTrackers.size();

    for (final Map.Entry<String, TaskTracker> taskEntry : taskTrackers.entrySet()) {
      final String taskId = taskEntry.getKey();
      final TaskTracker tracker = taskEntry.getValue();

      if (tracker.status == null) {
        throw new MSQException(UnknownFault.forMessage(StringUtils.format("Task [%s] status missing", taskId)));
      }

      if (tracker.didRunTimeOut(maxTaskStartDelayMillis) && !canceledWorkerTasks.contains(taskId)) {
        throw new MSQException(new TaskStartTimeoutFault(numTasks + 1));
      }

      if (tracker.didFail() && !canceledWorkerTasks.contains(taskId)) {
        throw new MSQException(new WorkerFailedFault(taskId, tracker.status.getErrorMsg()));
      }
    }
  }

  private void shutDownTasks()
  {
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
          taskIds.wait(sleepMillis);
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
    private TaskStatus status;
    private TaskLocation initialLocation;

    public TaskTracker(int workerNumber)
    {
      this.workerNumber = workerNumber;
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
  }
}
