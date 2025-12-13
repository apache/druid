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

package org.apache.druid.indexing.overlord.hrtr;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.WorkerNodeService;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.IndexTaskUtils;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerUtils;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningService;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerSelectStrategy;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * TODO: add a more descriptive title
 */
public class HttpRemoteTaskRunnerV2 implements WorkerTaskRunner, TaskLogStreamer, WorkerHolder.Listener
{
  private static final EmittingLogger log = new EmittingLogger(HttpRemoteTaskRunnerV2.class);

  public static final String TASK_DISCOVERED_COUNT = "task/discovered/count";
  private static final int FIND_WORKER_BACKOFF_DELAY_MILLIS = 10_000;
  private static final int FIND_WORKER_BACKOFF_RETRIES = 4;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  // Executor for assigning pending tasks to workers.
  private final ExecutorService pendingTasksExec;

  // All known tasks, TaskID -> HttpRemoteTaskRunnerWorkItem
  // This is a ConcurrentMap as some of the reads are done without holding the lock.
  private final ConcurrentHashMap<String, HttpRemoteTaskRunnerWorkItem> tasks = new ConcurrentHashMap<>();

  private final PriorityBlockingQueue<PendingTaskQueueItem> pendingTasks = new PriorityBlockingQueue<>();

  // All discovered workers, "host:port" -> WorkerHolder
  private final ConcurrentHashMap<String, WorkerHolder> workers = new ConcurrentHashMap<>();

  // Executor for syncing state of each worker.
  private final ScheduledExecutorService workersSyncExec;

  // Internal worker state counters
  private final AtomicLong blackListedWorkersCount = new AtomicLong(0);

  // Executor to complete cleanup of workers which have disappeared.
  private final ListeningScheduledExecutorService cleanupExec;
  private final ConcurrentMap<String, ScheduledFuture> removedWorkerCleanups = new ConcurrentHashMap<>();

  // Lock for synchronizing worker state transitions to minimize races between scheduling/accounting/adhoc worker routines
  private final Object workerStateLock = new Object();
  // Lock for waiting/notifying on task state transitions
  private final Object taskStateLock = new Object();

  // task runner listeners
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  private final ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy;
  private ProvisioningService provisioningService;

  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final HttpClient httpClient;
  private final ObjectMapper objectMapper;

  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final HttpRemoteTaskRunnerConfig config;

  private final TaskStorage taskStorage;
  private final ServiceEmitter emitter;

  private volatile DruidNodeDiscovery.Listener nodeDiscoveryListener;

  public HttpRemoteTaskRunnerV2(
      ObjectMapper objectMapper,
      HttpRemoteTaskRunnerConfig config,
      HttpClient httpClient,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      TaskStorage taskStorage,
      ServiceEmitter emitter
  )
  {
    this.objectMapper = objectMapper;
    this.config = config;
    this.httpClient = httpClient;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.taskStorage = taskStorage;
    this.workerConfigRef = workerConfigRef;
    this.emitter = emitter;

    this.pendingTasksExec = Execs.multiThreaded(
        config.getPendingTasksRunnerNumThreads(),
        "hrtr-pending-tasks-runner-%d"
    );

    this.workersSyncExec = ScheduledExecutors.fixed(
        config.getWorkerSyncNumThreads(),
        "HttpRemoteTaskRunnerV2-worker-sync-%d"
    );

    this.cleanupExec = MoreExecutors.listeningDecorator(
        ScheduledExecutors.fixed(1, "HttpRemoteTaskRunnerV2-Worker-Cleanup-%d")
    );

    this.provisioningStrategy = provisioningStrategy;
  }

  @Override
  @LifecycleStart
  public void start()
  {
    if (!lifecycleLock.canStart()) {
      return;
    }

    try {
      log.info("Starting...");

      startWorkersHandling();

      ScheduledExecutors.scheduleAtFixedRate(
          cleanupExec,
          Period.ZERO.toStandardDuration(),
          config.getWorkerBlackListCleanupPeriod().toStandardDuration(),
          this::checkAndRemoveWorkersFromBlackList
      );

      provisioningService = provisioningStrategy.makeProvisioningService(this);

      scheduleSyncMonitoring();

      startPendingTaskHandling();

      lifecycleLock.started();

      log.info("Started.");
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  /**
   * Must not be used outside of this class and {@link HttpRemoteTaskRunnerResource}
   */
  Map<String, ImmutableWorkerInfo> getWorkersEligibleToRunTasks()
  {
    return Maps.transformEntries(
        Maps.filterEntries(
            workers,
            input -> input.getValue().getState() == WorkerHolder.State.READY &&
                     input.getValue().isInitialized() &&
                     input.getValue().isEnabled()
        ),
        (String key, WorkerHolder value) -> value.toImmutable()
    );
  }

  @GuardedBy("workerStateLock")
  private ImmutableWorkerInfo findWorkerToRunTask(Task task)
  {
    WorkerBehaviorConfig workerConfig = workerConfigRef.get();
    WorkerSelectStrategy strategy;
    if (workerConfig == null || workerConfig.getSelectStrategy() == null) {
      strategy = WorkerBehaviorConfig.DEFAULT_STRATEGY;
      log.debug("No worker selection strategy set. Using default of [%s]", strategy.getClass().getSimpleName());
    } else {
      strategy = workerConfig.getSelectStrategy();
    }

    return strategy.findWorkerForTask(
        config,
        ImmutableMap.copyOf(getWorkersEligibleToRunTasks()),
        task
    );
  }

  private boolean runTaskOnWorker(
      final String taskId,
      final String workerHost
  ) throws InterruptedException
  {
    log.info("Assigning task[%s] to worker[%s]", taskId, workerHost);

    final HttpRemoteTaskRunnerWorkItem workItem = tasks.get(taskId);
    final WorkerHolder workerHolder = workers.get(workerHost);

    Preconditions.checkState(workItem != null, "No task item found for task[%s]", taskId);

    // Worker was removed, gracefully bail
    if (workerHolder == null) {
      log.warn("No worker found for host[%s]", workerHost);
      return false;
    }

    Preconditions.checkState(
        workerHolder.getState() == WorkerHolder.State.PENDING_ASSIGN,
        "Found invalid state[%s] for worker[%s], expected state[%s]",
        workerHolder.getState(),
        workerHost,
        WorkerHolder.State.PENDING_ASSIGN
    );

    if (workerHolder.assignTask(workItem.getTask())) {
      // Don't assign new tasks until the task we just assigned is actually running
      // on a worker - this avoids overflowing a worker with tasks
      long waitMs = config.getTaskAssignmentTimeout().toStandardDuration().getMillis();
      long waitStart = System.currentTimeMillis();
      boolean isTaskAssignmentTimedOut = false;

      final AtomicBoolean taskStartedOnWorker = new AtomicBoolean(false);
      synchronized (taskStateLock) {
        while (!taskStartedOnWorker.get()) {
          tasks.compute(
              taskId,
              (key, taskEntry) -> {
                if (taskEntry != null && taskEntry.isRunningOnWorker(workerHolder.getWorker())) {
                  taskStartedOnWorker.set(true);
                }
                return taskEntry;
              }
          );

          long remaining = waitMs - (System.currentTimeMillis() - waitStart);
          if (remaining > 0) {
            taskStateLock.wait(remaining);
          } else {
            isTaskAssignmentTimedOut = true;
            break;
          }
        }
      }

      if (isTaskAssignmentTimedOut) {
        log.makeAlert(
            "Task assignment timed out on worker[%s], never ran task[%s] in timeout[%s]!",
            workerHost,
            taskId,
            config.getTaskAssignmentTimeout()
        ).emit();

        throw new ISE(
            "Task assignment timed out on worker[%s], never ran task[%s] in timeout[%s]! See overlord and middleManager/indexer logs for more details.",
            workerHost,
            taskId,
            config.getTaskAssignmentTimeout()
        );
      }

      return true;
    } else {
      return false;
    }
  }

  // CAUTION: This method calls RemoteTaskRunnerWorkItem.setResult(..) which results in TaskQueue.notifyStatus() being called
  // because that is attached by TaskQueue to task result future. So, this method must not be called with "statusLock"
  // held. See https://github.com/apache/druid/issues/6201
  private void taskComplete(
      String taskId,
      String workerHost,
      TaskStatus taskStatus
  )
  {
    Preconditions.checkState(!Thread.holdsLock(workerStateLock), "Current thread must not hold workerStateLock.");
    Preconditions.checkState(!Thread.holdsLock(taskStateLock), "Current thread must not hold taskStateLock.");

    AtomicBoolean taskCompleted = new AtomicBoolean(false);

    tasks.compute(
        taskId,
        (key, taskEntry) -> {
          Preconditions.checkState(taskEntry != null, "Expected task[%s] to exist", taskId);
          if (taskEntry.getResult().isDone()) {
            // This is not the first complete event.
            try {
              TaskState lastKnownState = taskEntry.getResult().get().getStatusCode();
              if (taskStatus.getStatusCode() != lastKnownState) {
                log.warn(
                    "The state of the new task complete event is different from its last known state. "
                    + "New state[%s], last known state[%s]",
                    taskStatus.getStatusCode(),
                    lastKnownState
                );
              }
            }
            catch (InterruptedException e) {
              log.warn(e, "Interrupted while getting the last known task status.");
              Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
              // This case should not really happen.
              log.warn(e, "Failed to get the last known task status. Ignoring this failure.");
            }
          } else {
            // Notify interested parties
            taskEntry.setResult(taskStatus);
            taskCompleted.set(true);
          }

          return taskEntry;
        }
    );

    if (workerHost != null) {
      synchronized (workerStateLock) {
        workers.compute(
            workerHost,
            (key, workerHolder) -> {
              if (workerHolder != null) {
                log.info(
                    "Worker[%s] completed task[%s] with status[%s]",
                    workerHolder.getWorker().getHost(),
                    taskStatus.getId(),
                    taskStatus.getStatusCode()
                );
                // Worker is done with this task
                workerHolder.setLastCompletedTaskTime(DateTimes.nowUtc());
                blacklistWorkerIfNeeded(taskStatus, workerHolder);
              } else {
                log.warn("Could not find worker[%s]", workerHost);
              }
              return workerHolder;
            }
        );
      }
    }

    // Notify listeners outside both tasks/workers critical sections to avoid deadlock
    if (taskCompleted.get()) {
      TaskRunnerUtils.notifyStatusChanged(listeners, taskStatus.getId(), taskStatus);
    }

    // Notify interested parties that a worker is potentially free and/or a task status updated
    notifyWatchers();
  }

  private void startWorkersHandling() throws InterruptedException
  {
    final CountDownLatch workerViewInitialized = new CountDownLatch(1);
    DruidNodeDiscovery druidNodeDiscovery =
        druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY);
    this.nodeDiscoveryListener = new DruidNodeDiscovery.Listener()
    {
      @Override
      public void nodesAdded(Collection<DiscoveryDruidNode> nodes)
      {
        nodes.forEach(node -> addWorker(toWorker(node)));
      }

      @Override
      public void nodesRemoved(Collection<DiscoveryDruidNode> nodes)
      {
        nodes.forEach(node -> removeWorker(toWorker(node)));
      }

      @Override
      public void nodeViewInitialized()
      {
        //CountDownLatch.countDown() does nothing when count has already reached 0.
        workerViewInitialized.countDown();
      }

      @Override
      public void nodeViewInitializedTimedOut()
      {
        nodeViewInitialized();
      }
    };

    druidNodeDiscovery.registerListener(nodeDiscoveryListener);

    long workerDiscoveryStartTime = System.currentTimeMillis();
    while (!workerViewInitialized.await(30, TimeUnit.SECONDS)) {
      if (System.currentTimeMillis() - workerDiscoveryStartTime > TimeUnit.MINUTES.toMillis(5)) {
        throw new ISE("Couldn't discover workers.");
      } else {
        log.info("Waiting for worker discovery...");
      }
    }
    log.info("Discovered [%d] workers.", workers.size());

    // Wait till all worker state is synced so that we know which worker is running/completed what tasks or else
    // We would start assigning tasks which are pretty soon going to be reported by discovered workers.
    workers.forEach((workerHost, workerEntry) -> {
      try {
        workerEntry.waitForInitialization();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    log.info("Workers have synced state successfully.");
  }

  private Worker toWorker(DiscoveryDruidNode node)
  {
    final WorkerNodeService workerNodeService = node.getService(
        WorkerNodeService.DISCOVERY_SERVICE_KEY,
        WorkerNodeService.class
    );
    if (workerNodeService == null) {
      // this shouldn't typically happen, but just in case it does, make a dummy worker to allow the callbacks to
      // continue since addWorker/removeWorker only need worker.getHost()
      return new Worker(
          node.getDruidNode().getServiceScheme(),
          node.getDruidNode().getHostAndPortToUse(),
          null,
          0,
          "",
          WorkerConfig.DEFAULT_CATEGORY
      );
    }
    return new Worker(
        node.getDruidNode().getServiceScheme(),
        node.getDruidNode().getHostAndPortToUse(),
        workerNodeService.getIp(),
        workerNodeService.getCapacity(),
        workerNodeService.getVersion(),
        workerNodeService.getCategory()
    );
  }

  @VisibleForTesting
  void addWorker(final Worker worker)
  {
    log.info("Adding worker[%s]", worker.getHost());
    synchronized (workerStateLock) {
      workers.compute(
          worker.getHost(), (key, workerEntry) -> {
            cancelWorkerCleanup(worker.getHost());

            // There cannot be any new tasks assigned to this worker as the entry has not been published yet.
            // That being said, there can be callbacks in taskAddedOrUpdated() where some task suddenly begins running
            // on this worker. That method still blocks on this key lock, so it will occur strictly before/after this insertion.
            if (workerEntry == null) {
              log.info("Unrecognized worker[%s], rebuilding task mapping", worker.getHost());
              final List<TaskAnnouncement> expectedAnnouncements = new ArrayList<>();
              // It might be a worker that existed before, temporarily went away and came back. We might have a set of
              // tasks that we think are running on this worker. Provide that information to WorkerHolder that
              // manages the task syncing with that worker.
              tasks.forEach((taskId, taskEntry) -> {
                if (taskEntry.isRunningOnWorker(worker)) {
                  // This announcement is only used to notify when a task has disappeared on the worker
                  // So it is okay to set the dataSource and taskResource to null as they will not be used
                  expectedAnnouncements.add(
                      TaskAnnouncement.create(
                          taskEntry.getTaskId(),
                          taskEntry.getTaskType(),
                          null,
                          TaskStatus.running(taskEntry.getTaskId()),
                          taskEntry.getLocation(),
                          null
                      )
                  );
                }
              });

              workerEntry = createWorkerHolder(
                  objectMapper,
                  httpClient,
                  config,
                  workersSyncExec,
                  this,
                  worker,
                  expectedAnnouncements
              );
              workerEntry.start();
            } else {
              log.info("Worker[%s] already exists", worker.getHost());
            }
            return workerEntry;
          }
      );

      // Notify any waiters that there is a new worker available
      workerStateLock.notifyAll();
    }
  }

  protected WorkerHolder createWorkerHolder(
      ObjectMapper objectMapper,
      HttpClient httpClient,
      HttpRemoteTaskRunnerConfig config,
      ScheduledExecutorService workersSyncExec,
      WorkerHolder.Listener listener,
      Worker worker,
      List<TaskAnnouncement> knownAnnouncements
  )
  {
    return new WorkerHolder(objectMapper, httpClient, config, workersSyncExec, listener, worker, knownAnnouncements);
  }

  @VisibleForTesting
  void removeWorker(final Worker worker)
  {
    // Acquire workerLock to ensure atomicity between worker removal and competing scheduling routines
    final WorkerHolder workerEntry;
    synchronized (workerStateLock) {
      workerEntry = workers.remove(worker.getHost());
    }

    // Perform the cleanup operations outside the lock to avoid excessive locking/deadlock
    if (workerEntry != null) {
      log.info("Removing worker[%s]", worker.getHost());
      try {
        workerEntry.stop();
        scheduleTasksCleanupForWorker(worker.getHost());
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      log.warn("Asked to remove a non-existent worker[%s]", worker.getHost());
    }
  }

  private void cancelWorkerCleanup(String workerHost)
  {
    ScheduledFuture previousCleanup = removedWorkerCleanups.remove(workerHost);
    if (previousCleanup != null) {
      log.info("Cancelling worker[%s] scheduled task cleanup", workerHost);
      previousCleanup.cancel(false);
    }
  }

  private void scheduleTasksCleanupForWorker(final String workerHostAndPort)
  {
    cancelWorkerCleanup(workerHostAndPort);

    final ListenableScheduledFuture<?> cleanupTask = cleanupExec.schedule(
        () -> {
          log.info("Running scheduled cleanup for worker[%s]", workerHostAndPort);
          try {
            final Set<HttpRemoteTaskRunnerWorkItem> tasksToFail = new HashSet<>();
            tasks.forEach((taskId, taskEntry) -> {
              if (taskEntry.getState().inProgress()) {
                if (taskEntry.getWorker() != null && taskEntry.getWorker().getHost().equals(workerHostAndPort)) {
                  tasksToFail.add(taskEntry);
                }
              }
            });

            for (HttpRemoteTaskRunnerWorkItem taskItem : tasksToFail) {
              if (!taskItem.getResult().isDone()) {
                log.warn(
                    "Failing task[%s] because worker[%s] disappeared and did not report within cleanup timeout[%s]",
                    taskItem.getTaskId(),
                    workerHostAndPort,
                    config.getTaskCleanupTimeout()
                );
                // taskComplete(..) must be called outside workerStatusLock, see comments on method.
                taskComplete(
                    taskItem.getTaskId(),
                    null,
                    TaskStatus.failure(
                        taskItem.getTaskId(),
                        StringUtils.format(
                            "The worker that this task was assigned disappeared and "
                            + "did not report cleanup within timeout[%s]. "
                            + "See overlord and middleManager/indexer logs for more details.",
                            config.getTaskCleanupTimeout()
                        )
                    )
                );
              }
            }
          }
          catch (Exception e) {
            log.makeAlert(e, "Exception while cleaning up worker[%s]", workerHostAndPort).emit();
            throw new RuntimeException(e);
          }
        },
        config.getTaskCleanupTimeout().toStandardDuration().getMillis(),
        TimeUnit.MILLISECONDS
    );

    removedWorkerCleanups.put(workerHostAndPort, cleanupTask);

    // Remove this entry from removedWorkerCleanups when done, if it's actually the one in there.
    Futures.addCallback(
        cleanupTask,
        new FutureCallback<Object>()
        {
          @Override
          public void onSuccess(Object result)
          {
            removedWorkerCleanups.remove(workerHostAndPort, cleanupTask);
          }

          @Override
          public void onFailure(Throwable t)
          {
            removedWorkerCleanups.remove(workerHostAndPort, cleanupTask);
          }
        },
        MoreExecutors.directExecutor()
    );
  }

  private void scheduleSyncMonitoring()
  {
    workersSyncExec.scheduleAtFixedRate(
        () -> {
          log.debug("Running worker sync monitoring");

          try {
            syncMonitoring();
          }
          catch (Exception ex) {
            log.makeAlert(ex, "Exception in worker sync monitoring").emit();
          }
        },
        1,
        5,
        TimeUnit.MINUTES
    );
  }

  @VisibleForTesting
  void syncMonitoring()
  {
    // Ensure that the collection is not being modified during iteration. Iterate over a copy
    final Set<Map.Entry<String, WorkerHolder>> workerEntrySet = ImmutableSet.copyOf(workers.entrySet());
    for (Map.Entry<String, WorkerHolder> e : workerEntrySet) {
      WorkerHolder workerHolder = e.getValue();
      if (workerHolder.getUnderlyingSyncer().needsReset()) {
        // TODO: do we want to make this remove/add atomic (e.g. acquire workerLock before)
        if (workers.containsKey(e.getKey())) {
          log.makeAlert(
              "Worker[%s] is not syncing properly. Current state is [%s]. Resetting it.",
              workerHolder.getWorker().getHost(),
              workerHolder.getUnderlyingSyncer().getDebugInfo()
          ).emit();
          removeWorker(workerHolder.getWorker());
          addWorker(workerHolder.getWorker());
        }
      }
    }
  }

  /**
   * This method returns the debugging information exposed by {@link HttpRemoteTaskRunnerResource} and meant
   * for that use only. It must not be used for any other purpose.
   */
  Map<String, Object> getWorkerSyncerDebugInfo()
  {
    Preconditions.checkArgument(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    Map<String, Object> result = Maps.newHashMapWithExpectedSize(workers.size());
    for (Map.Entry<String, WorkerHolder> e : ImmutableSet.copyOf(workers.entrySet())) {
      WorkerHolder serverHolder = e.getValue();
      result.put(
          e.getKey(),
          serverHolder.getUnderlyingSyncer().getDebugInfo()
      );
    }
    return result;
  }

  private void checkAndRemoveWorkersFromBlackList()
  {
    final AtomicBoolean shouldRunPendingTasks = new AtomicBoolean(false);

    synchronized (workerStateLock) {
      for (final String workerHost : workers.keySet()) {
        workers.computeIfPresent(
            workerHost,
            (workerHostKey, workerEntry) -> {
              if (workerEntry.getState() == WorkerHolder.State.BLACKLISTED) {
                if (shouldRemoveNodeFromBlackList(workerEntry)) {
                  log.debug("Removing worker[%s] from blacklist", workerHost);
                  workerEntry.resetContinuouslyFailedTasksCount();
                  workerEntry.setBlacklistedUntil(null);
                  workerEntry.setState(WorkerHolder.State.READY);
                  shouldRunPendingTasks.set(true);
                } else {
                  log.debug("Skipping removal of worker[%s] from blacklist", workerHost);
                }
              }
              return workerEntry;
            }
        );
      }

      if (shouldRunPendingTasks.get()) {
        workerStateLock.notifyAll();
      }
    }
  }

  /**
   * This method should be called under the corresponding worker key lock.
   */
  private boolean shouldRemoveNodeFromBlackList(WorkerHolder workerHolder)
  {
    if (blackListedWorkersCount.get() > workers.size() * (config.getMaxPercentageBlacklistWorkers() / 100.0)) {
      log.info(
          "Removing [%s] from blacklist because percentage of blacklisted workers exceeds [%d]",
          workerHolder.getWorker(),
          config.getMaxPercentageBlacklistWorkers()
      );

      return true;
    }

    DateTime blacklistedUntil = workerHolder.getBlacklistedUntil();
    if (blacklistedUntil == null) {
      // Blacklisted without an expiry time - should not happen, but remove from blacklist
      log.warn("Worker[%s] is blacklisted but has no blacklistedUntil time set. Removing from blacklist.",
               workerHolder.getWorker());
      return true;
    }

    long remainingMillis = blacklistedUntil.getMillis() - System.currentTimeMillis();
    if (remainingMillis <= 0) {
      log.info("Removing [%s] from blacklist because backoff time elapsed", workerHolder.getWorker());
      return true;
    }

    log.info("[%s] still blacklisted for [%,ds]", workerHolder.getWorker(), remainingMillis / 1000);
    return false;
  }

  /**
   * This method should be called under the corresponding worker key lock.
   */
  private void blacklistWorkerIfNeeded(TaskStatus taskStatus, WorkerHolder workerHolder)
  {
    if (taskStatus.isSuccess()) {
      workerHolder.resetContinuouslyFailedTasksCount();
      if (workerHolder.getState() == WorkerHolder.State.BLACKLISTED) {
        workerHolder.setBlacklistedUntil(null);
        workerHolder.setState(WorkerHolder.State.READY);
        blackListedWorkersCount.decrementAndGet();
        log.info(
            "Worker[%s] removed from blacklist because task[%s] finished with SUCCESS",
            workerHolder.getWorker(),
            taskStatus.getId()
        );
      }
    } else if (taskStatus.isFailure()) {
      workerHolder.incrementContinuouslyFailedTasksCount();
    }

    if (workerHolder.getContinuouslyFailedTasksCount() > config.getMaxRetriesBeforeBlacklist() &&
        blackListedWorkersCount.get() <= workers.size() * (config.getMaxPercentageBlacklistWorkers() / 100.0) - 1) {
      // If worker is active, blacklist it
      if (workerHolder.getState() == WorkerHolder.State.READY
          || workerHolder.getState() == WorkerHolder.State.PENDING_ASSIGN) {
        workerHolder.setBlacklistedUntil(DateTimes.nowUtc().plus(config.getWorkerBlackListBackoffTime()));
        workerHolder.setState(WorkerHolder.State.BLACKLISTED);
        blackListedWorkersCount.incrementAndGet();
        log.info(
            "Blacklisting worker[%s] until [%s] after [%,d] failed tasks in a row.",
            workerHolder.getWorker(),
            workerHolder.getBlacklistedUntil(),
            workerHolder.getContinuouslyFailedTasksCount()
        );
      }
    }
  }

  @Override
  public Collection<ImmutableWorkerInfo> getWorkers()
  {
    return workers.values().stream().map(WorkerHolder::toImmutable).collect(Collectors.toList());
  }

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    // Want this synchronized with lazy-worker routine
    synchronized (workerStateLock) {
      return workers.values()
                    .stream()
                    .filter(w -> w.getState() == WorkerHolder.State.LAZY)
                    .map(WorkerHolder::getWorker)
                    .collect(Collectors.toList());
    }
  }

  @Override
  public Collection<Worker> markWorkersLazy(Predicate<ImmutableWorkerInfo> isLazyWorker, int maxLazyWorkers)
  {
    // Search for new workers to mark lazy.
    // Status lock is used to prevent any tasks being assigned to workers while we mark them lazy
    synchronized (workerStateLock) {
      AtomicInteger numMarkedLazy = new AtomicInteger(getLazyWorkers().size());
      workers.forEach((workerHostKey, workerHolder) -> {
        if (numMarkedLazy.get() >= maxLazyWorkers) {
          return;
        }
        try {
          if (isWorkerOkForMarkingLazy(workerHolder) && isLazyWorker.apply(workerHolder.toImmutable())) {
            log.info("Marking worker[%s] as lazy", workerHolder.getWorker().getHost());
            workerHolder.setState(WorkerHolder.State.LAZY);
            numMarkedLazy.incrementAndGet();
          }
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      });

      log.info("Marked [%d] workers as lazy", numMarkedLazy.get());
      return getLazyWorkers();
    }
  }

  @GuardedBy("workerStateLock")
  private boolean isWorkerOkForMarkingLazy(WorkerHolder workerHolder)
  {
    // Check that worker is not already lazy, and does not have any in-flight tasks being assigned to it.
    if (workerHolder.getState() == WorkerHolder.State.LAZY
        || workerHolder.getState() == WorkerHolder.State.PENDING_ASSIGN) {
      log.debug(
          "Skipping marking worker[%s] with incompatiable state[%s]",
          workerHolder.getWorker().getHost(),
          workerHolder.getState()
      );
      return false;
    }

    // Check that worker has no in-flight/running tasks associated with it
    final AtomicBoolean inProgress = new AtomicBoolean(false);
    tasks.forEach((taskId, taskEntry) -> {
      if (taskEntry.getState().inProgress()) {
        if (taskEntry.getWorker() != null && taskEntry.getWorker()
                                                      .getHost()
                                                      .equals(workerHolder.getWorker().getHost())) {
          inProgress.set(true);
        }
      }
    });
    return !inProgress.get();
  }

  @Override
  public WorkerTaskRunnerConfig getConfig()
  {
    return config;
  }

  @Override
  public Collection<Task> getPendingTaskPayloads()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState().isPending())
                .map(HttpRemoteTaskRunnerWorkItem::getTask)
                .collect(Collectors.toList());
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskId, long offset) throws IOException
  {
    HttpRemoteTaskRunnerWorkItem taskRunnerWorkItem = tasks.get(taskId);
    Worker worker = null;
    if (taskRunnerWorkItem != null && taskRunnerWorkItem.getState() != HttpRemoteTaskRunnerWorkItem.State.COMPLETE) {
      worker = taskRunnerWorkItem.getWorker();
    }

    if (worker == null || !workers.containsKey(worker.getHost())) {
      // Worker is not running this task, it might be available in deep storage
      return Optional.absent();
    } else {
      // Worker is still running this task
      final URL url = TaskRunnerUtils.makeWorkerURL(
          worker,
          "/druid/worker/v1/task/%s/log?offset=%s",
          taskId,
          Long.toString(offset)
      );

      try {
        return Optional.of(httpClient.go(
            new Request(HttpMethod.GET, url),
            new InputStreamResponseHandler()
        ).get());
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      catch (ExecutionException e) {
        // Unwrap if possible
        Throwables.propagateIfPossible(e.getCause(), IOException.class);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskId) throws IOException
  {
    HttpRemoteTaskRunnerWorkItem taskRunnerWorkItem = tasks.get(taskId);
    Worker worker = null;
    if (taskRunnerWorkItem != null && taskRunnerWorkItem.getState() != HttpRemoteTaskRunnerWorkItem.State.COMPLETE) {
      worker = taskRunnerWorkItem.getWorker();
    }

    if (worker == null || !workers.containsKey(worker.getHost())) {
      // Worker is not running this task, it might be available in deep storage
      return Optional.absent();
    } else {
      // Worker is still running this task
      TaskLocation taskLocation = taskRunnerWorkItem.getLocation();

      if (TaskLocation.unknown().equals(taskLocation)) {
        // No location known for this task. It may have not been assigned a location yet.
        return Optional.absent();
      }

      final URL url = TaskRunnerUtils.makeTaskLocationURL(
          taskLocation,
          "/druid/worker/v1/chat/%s/liveReports",
          taskId
      );

      try {
        return Optional.of(httpClient.go(
            new Request(HttpMethod.GET, url),
            new InputStreamResponseHandler()
        ).get());
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      catch (ExecutionException e) {
        // Unwrap if possible
        Throwables.propagateIfPossible(e.getCause(), IOException.class);
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return ImmutableList.of();
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listener.getListenerId())) {
        throw new ISE("Listener [%s] already registered", listener.getListenerId());
      }
    }

    final Pair<TaskRunnerListener, Executor> listenerPair = Pair.of(listener, executor);

    tasks.forEach((taskId, taskEntry) -> {
      if (taskEntry.getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
        TaskRunnerUtils.notifyLocationChanged(
            ImmutableList.of(listenerPair),
            taskId,
            taskEntry.getLocation()
        );
      }
    });

    log.info("Registered listener [%s]", listener.getListenerId());
    listeners.add(listenerPair);
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listenerId)) {
        listeners.remove(pair);
        log.info("Unregistered listener [%s]", listenerId);
        return;
      }
    }
  }

  @Override
  public ListenableFuture<TaskStatus> run(Task task)
  {
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS), "TaskRunner not started.");

    AtomicReference<ListenableFuture<TaskStatus>> taskFuture = new AtomicReference<>();

    log.info("Adding task[%s]", task.getId());

    tasks.compute(
        task.getId(), (id, entry) -> {
          // Task already exists, but in case it was discovered from a worker on start()
          // and TaskAnnouncement does not have Task instance, add it.
          if (entry != null) {
            if (entry.getTask() == null) {
              entry.setTask(task);
            }
          } else {
            entry = new HttpRemoteTaskRunnerWorkItem(
                task.getId(),
                null,
                null,
                task,
                task.getType(),
                HttpRemoteTaskRunnerWorkItem.State.PENDING
            );
            pendingTasks.offer(new PendingTaskQueueItem(task));
          }

          taskFuture.set(entry.getResult());
          return entry;
        }
    );

    return taskFuture.get();
  }

  private void startPendingTaskHandling()
  {
    for (int i = 0; i < config.getPendingTasksRunnerNumThreads(); i++) {
      pendingTasksExec.submit(
          () -> {
            try {
              if (!lifecycleLock.awaitStarted()) {
                log.makeAlert("Lifecycle not started, PendingTaskExecution loop will not run.").emit();
                return;
              }

              pendingTasksExecutionLoop();
            }
            catch (Throwable t) {
              log.makeAlert(t, "Error while waiting for lifecycle start. PendingTaskExecution loop will not run")
                 .emit();
            }
            finally {
              log.info("PendingTaskExecution loop exited.");
            }
          }
      );
    }
  }

  @VisibleForTesting
  void pendingTasksExecutionLoop()
  {
    while (!Thread.interrupted() && lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      try {
        final PendingTaskQueueItem taskItem = pendingTasks.poll(1, TimeUnit.MINUTES);
        if (taskItem == null) {
          log.info("Found no available tasks. Waiting for tasks to assign.");
          continue;
        }
        final String taskId = taskItem.getTask().getId();

        ImmutableWorkerInfo workerToAssign;
        int workerFetchRetries = 0;

        synchronized (workerStateLock) {
          do {
            workerToAssign = findWorkerToRunTask(taskItem.getTask());
            if (workerToAssign == null) {
              log.warn("No workers available to run task[%s]. Waiting", taskId);
              workerStateLock.wait(FIND_WORKER_BACKOFF_DELAY_MILLIS); // yield the lock and wait a bit
            }
          } while (workerToAssign == null && workerFetchRetries++ < FIND_WORKER_BACKOFF_RETRIES);

          // Exhausted worker assignment retries, let's backoff a bit
          if (workerToAssign == null) {
            log.warn("Failed to find workers available to run task[%s]. Sending to back of queue", taskId);
            pendingTasks.put(taskItem.withFreshSequenceNumber());
            continue;
          }

          // Mark this worker as unassignable while task is being assigned
          workers.compute(
              workerToAssign.getWorker().getHost(), (key, entry) -> {
                Preconditions.checkState(
                    entry != null,
                    "Expected selected worker[%s] to be available",
                    entry.getWorker().getHost()
                );
                Preconditions.checkState(
                    entry.getState() == WorkerHolder.State.READY,
                    "Expected worker[%s] state to be READY, got [%s]",
                    entry.getWorker().getHost(),
                    entry.getState()
                );

                entry.setState(WorkerHolder.State.PENDING_ASSIGN);
                return entry;
              }
          );

          // Mark this task as pending worker assign
          tasks.compute(
              taskId,
              (key, entry) -> {
                Preconditions.checkState(entry != null, "Expected task[%s] to be in tasks set", taskId);
                Preconditions.checkState(
                    entry.getState() == HttpRemoteTaskRunnerWorkItem.State.PENDING,
                    "Expected task[%s] state to be PENDING, got state[%s]",
                    taskId,
                    entry.getState()
                );

                entry.setState(HttpRemoteTaskRunnerWorkItem.State.PENDING_WORKER_ASSIGN);
                return entry;
              }
          );
        }

        final String workerHost = workerToAssign.getWorker().getHost();
        try {
          if (!runTaskOnWorker(taskId, workerHost)) {
            log.warn("Failed to assign task[%s] to worker[%s]. Sending to back of queue", taskId, workerHost);
            pendingTasks.put(taskItem.withFreshSequenceNumber());
          } else {
            log.info("Assigned task[%s] to worker[%s]", taskId, workerHost);
          }
        }
        catch (InterruptedException ex) {
          log.info("Got InterruptedException while assigning task[%s].", taskId);
          throw ex;
        }
        catch (Throwable th) {
          log.makeAlert(th, "Exception while trying to assign task")
             .addData("taskId", taskId)
             .emit();

          // taskComplete(..) must be called outside workerStatusLock, see comments on method.
          taskComplete(
              taskId,
              null,
              TaskStatus.failure(
                  taskId,
                  StringUtils.format(
                      "Failed to assign this task to worker[%s]. See overlord logs for more details.",
                      workerHost
                  )
              )
          );
        }
        finally {
          // Allow the worker to accept tasks again
          synchronized (workerStateLock) {
            workers.compute(
                workerHost,
                (key, entry) -> {
                  if (entry == null) {
                    log.warn("Could not find worker[%s]", workerHost);
                  } else {
                    // Only reset the worker status if PENDING_ASSIGN
                    // If LAZY/BLACKLISTED, either the worker is getting trashed eminently or will be auto-reset.
                    entry.compareAndExchangeState(WorkerHolder.State.PENDING_ASSIGN, WorkerHolder.State.READY);
                  }
                  return entry;
                }
            );
          }

          notifyWatchers();
        }
      }
      catch (InterruptedException e) {
        log.warn("Interrupted, stopping pending task execution loop.");
        Thread.currentThread().interrupt();
      }
      catch (Throwable th) {
        log.makeAlert(th, "Unknown Exception while trying to assign tasks.").emit();
      }
    }

    log.warn("Pending tasks execution loop exited");
  }

  @Override
  public void shutdown(String taskId, String reason)
  {
    log.info("Shutdown task[%s] because [%s]", taskId, reason);

    AtomicReference<WorkerHolder> workerHolderRunningTaskRef = new AtomicReference<>();
    final AtomicBoolean wasComplete = new AtomicBoolean(false);
    tasks.compute(
        taskId,
        (key, entry) -> {
          if (entry != null) {
            if (entry.getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
              workerHolderRunningTaskRef.set(workers.get(entry.getWorker().getHost()));
            } else if (entry.getState() == HttpRemoteTaskRunnerWorkItem.State.COMPLETE) {
              wasComplete.set(true);
              entry = null; // delete the entry
            }
          } else {
            log.debug("Asked to shutdown task[%s], but task not found. Already cleaned up.", taskId);
          }
          return entry;
        }
    );

    if (workerHolderRunningTaskRef.get() != null) {
      log.debug(
          "Got shutdown request for task[%s]. Asking worker[%s] to kill it.",
          taskId,
          workerHolderRunningTaskRef.get().getWorker().getHost()
      );
      workerHolderRunningTaskRef.get().shutdownTask(taskId);
    } else if (wasComplete.get()) {
      log.debug("Task[%s] already completed, no shutdown needed.", taskId);
    } else {
      log.debug("Task[%s] not found or not running, no shutdown needed.", taskId);
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    try {
      log.info("Stopping...");

      if (provisioningService != null) {
        provisioningService.close();
      }
      pendingTasksExec.shutdownNow();
      workersSyncExec.shutdownNow();
      cleanupExec.shutdown();

      log.info("Removing listener");
      DruidNodeDiscovery druidNodeDiscovery =
          druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY);
      druidNodeDiscovery.removeListener(nodeDiscoveryListener);

      log.info("Stopping worker holders");
      workers.forEach((workerHost, workerHolder) -> {
        try {
          workerHolder.stop();
        }
        catch (Exception e) {
          log.error(e, e.getMessage());
        }
      });
    }
    finally {
      lifecycleLock.exitStop();
    }

    log.info("Stopped.");
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING)
                .collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState().isPending())
                .collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    return ImmutableList.copyOf(tasks.values());
  }

  public Collection<? extends TaskRunnerWorkItem> getCompletedTasks()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState() == HttpRemoteTaskRunnerWorkItem.State.COMPLETE)
                .collect(Collectors.toList());
  }

  @Nullable
  @Override
  public RunnerTaskState getRunnerTaskState(String taskId)
  {
    final HttpRemoteTaskRunnerWorkItem workItem = tasks.get(taskId);
    if (workItem == null) {
      return null;
    } else {
      return workItem.getState().toRunnerTaskState();
    }
  }

  @Override
  public TaskLocation getTaskLocation(String taskId)
  {
    final HttpRemoteTaskRunnerWorkItem workItem = tasks.get(taskId);
    if (workItem == null) {
      return TaskLocation.unknown();
    } else {
      return workItem.getLocation();
    }
  }

  public Collection<ImmutableWorkerInfo> getBlackListedWorkers()
  {
    return workers.values()
                  .stream()
                  .filter(w -> w.getState() == WorkerHolder.State.BLACKLISTED)
                  .map(WorkerHolder::toImmutable)
                  .collect(Collectors.toList());
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.fromNullable(provisioningService.getStats());
  }

  @Override
  public void taskAddedOrUpdated(final TaskAnnouncement announcement, final WorkerHolder workerHolder)
  {
    final String taskId = announcement.getTaskId();
    final Worker worker = workerHolder.getWorker();

    log.debug(
        "Worker[%s] wrote status[%s] for task[%s] on [%s]",
        worker.getHost(),
        announcement.getTaskStatus().getStatusCode(),
        taskId,
        announcement.getTaskLocation()
    );

    final AtomicBoolean shouldShutdownTask = new AtomicBoolean(false);
    final AtomicBoolean isTaskCompleted = new AtomicBoolean(false);

    tasks.compute(
        taskId,
        (key, taskEntry) -> {
          if (taskEntry == null) {
            // Try to find information about it in the TaskStorage
            Optional<TaskStatus> knownStatusInStorage = taskStorage.getStatus(taskId);

            if (knownStatusInStorage.isPresent()) {
              switch (knownStatusInStorage.get().getStatusCode()) {
                case RUNNING:
                  taskEntry = new HttpRemoteTaskRunnerWorkItem(
                      taskId,
                      worker,
                      TaskLocation.unknown(),
                      null,
                      announcement.getTaskType(),
                      HttpRemoteTaskRunnerWorkItem.State.RUNNING
                  );
                  final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
                  metricBuilder.setDimension(DruidMetrics.TASK_ID, taskId);
                  emitter.emit(metricBuilder.setMetric(TASK_DISCOVERED_COUNT, 1L));
                  break;
                case SUCCESS:
                case FAILED:
                  if (!announcement.getTaskStatus().isComplete()) {
                    log.info(
                        "Worker[%s] reported status for completed, known from taskStorage, task[%s]. Ignored.",
                        worker.getHost(),
                        taskId
                    );
                  }
                  break;
                default:
                  log.makeAlert(
                      "Found unrecognized state[%s] of task[%s] in taskStorage. Notification[%s] from worker[%s] is ignored.",
                      knownStatusInStorage.get().getStatusCode(),
                      taskId,
                      announcement,
                      worker.getHost()
                  ).emit();
              }
            } else {
              log.warn(
                  "Worker[%s] reported status[%s] for unknown task[%s]. Ignored.",
                  worker.getHost(),
                  announcement.getStatus(),
                  taskId
              );
            }
          }

          if (taskEntry == null) {
            if (!announcement.getTaskStatus().isComplete()) {
              shouldShutdownTask.set(true);
            }
          } else {
            switch (announcement.getTaskStatus().getStatusCode()) {
              case RUNNING:
                switch (taskEntry.getState()) {
                  case PENDING:
                  case PENDING_WORKER_ASSIGN:
                    taskEntry.setWorker(worker);
                    taskEntry.setState(HttpRemoteTaskRunnerWorkItem.State.RUNNING);
                    log.info("Task[%s] started RUNNING on worker[%s].", taskId, worker.getHost());

                    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
                    IndexTaskUtils.setTaskDimensions(metricBuilder, taskEntry.getTask());
                    emitter.emit(metricBuilder.setMetric(
                                     "task/pending/time",
                                     new Duration(taskEntry.getCreatedTime(), DateTimes.nowUtc()).getMillis()
                                 )
                    );

                    // fall through
                  case RUNNING:
                    if (worker.getHost().equals(taskEntry.getWorker().getHost())) {
                      if (!announcement.getTaskLocation().equals(taskEntry.getLocation())) {
                        log.info(
                            "Task[%s] location changed on worker[%s]. new location[%s].",
                            taskId,
                            worker.getHost(),
                            announcement.getTaskLocation()
                        );
                        taskEntry.setLocation(announcement.getTaskLocation());
                        TaskRunnerUtils.notifyLocationChanged(listeners, taskId, announcement.getTaskLocation());
                      }
                    } else {
                      log.warn(
                          "Found worker[%s] running task[%s] which is being run by another worker[%s]. Notification ignored.",
                          worker.getHost(),
                          taskId,
                          taskEntry.getWorker().getHost()
                      );
                      shouldShutdownTask.set(true);
                    }
                    break;
                  case COMPLETE:
                    log.warn(
                        "Worker[%s] reported status for completed task[%s]. Ignored.",
                        worker.getHost(),
                        taskId
                    );
                    shouldShutdownTask.set(true);
                    break;
                  default:
                    log.makeAlert(
                        "Found unrecognized state[%s] of task[%s]. Notification[%s] from worker[%s] is ignored.",
                        taskEntry.getState(),
                        taskId,
                        announcement,
                        worker.getHost()
                    ).emit();
                }
                break;
              case FAILED:
              case SUCCESS:
                switch (taskEntry.getState()) {
                  case PENDING:
                  case PENDING_WORKER_ASSIGN:
                    taskEntry.setWorker(worker);
                    taskEntry.setState(HttpRemoteTaskRunnerWorkItem.State.RUNNING);
                    log.info("Task[%s] finished on worker[%s].", taskId, worker.getHost());
                    // fall through
                  case RUNNING:
                    if (worker.getHost().equals(taskEntry.getWorker().getHost())) {
                      if (!announcement.getTaskLocation().equals(taskEntry.getLocation())) {
                        log.info(
                            "Task[%s] location changed on worker[%s]. new location[%s].",
                            taskId,
                            worker.getHost(),
                            announcement.getTaskLocation()
                        );
                        taskEntry.setLocation(announcement.getTaskLocation());
                        TaskRunnerUtils.notifyLocationChanged(listeners, taskId, announcement.getTaskLocation());
                      }

                      isTaskCompleted.set(true);
                    } else {
                      log.warn(
                          "Worker[%s] reported completed task[%s] which is being run by another worker[%s]. Notification ignored.",
                          worker.getHost(),
                          taskId,
                          taskEntry.getWorker().getHost()
                      );
                    }
                    break;
                  case COMPLETE:
                    // this can happen when a worker is restarted and reports its list of completed tasks again.
                    break;
                  default:
                    log.makeAlert(
                        "Found unrecognized state[%s] of task[%s]. Notification[%s] from worker[%s] is ignored.",
                        taskEntry.getState(),
                        taskId,
                        announcement,
                        worker.getHost()
                    ).emit();
                }
                break;
              default:
                log.makeAlert(
                    "Worker[%s] reported unrecognized state[%s] for task[%s].",
                    worker.getHost(),
                    announcement.getTaskStatus().getStatusCode(),
                    taskId
                ).emit();
            }
          }
          return taskEntry;
        }
    );

    if (isTaskCompleted.get()) {
      // taskComplete(..) must be called outside statusLock, see comments on method.
      taskComplete(taskId, worker.getHost(), announcement.getTaskStatus());
    }

    if (shouldShutdownTask.get()) {
      log.warn("Killing task[%s] on worker[%s]", taskId, worker.getHost());
      workerHolder.shutdownTask(taskId);
    }

    // Notify interested parties
    notifyWatchers();
  }

  @Override
  public void stateChanged(boolean enabled, WorkerHolder workerHolder)
  {
    notifyWatchers();
  }

  private void notifyWatchers()
  {
    synchronized (workerStateLock) {
      workerStateLock.notifyAll();
    }
    synchronized (taskStateLock) {
      taskStateLock.notifyAll();
    }
  }

  @Override
  public Map<String, Long> getTotalTaskSlotCount()
  {
    Map<String, Long> totalPeons = new HashMap<>();
    for (ImmutableWorkerInfo worker : getWorkers()) {
      String workerCategory = worker.getWorker().getCategory();
      int workerCapacity = worker.getWorker().getCapacity();
      totalPeons.compute(
          workerCategory,
          (category, totalCapacity) -> totalCapacity == null ? workerCapacity : totalCapacity + workerCapacity
      );
    }

    return totalPeons;
  }

  @Override
  public Map<String, Long> getIdleTaskSlotCount()
  {
    Map<String, Long> totalIdlePeons = new HashMap<>();
    for (ImmutableWorkerInfo worker : getWorkersEligibleToRunTasks().values()) {
      String workerCategory = worker.getWorker().getCategory();
      int workerAvailableCapacity = worker.getAvailableCapacity();
      totalIdlePeons.compute(
          workerCategory,
          (category, availableCapacity) -> availableCapacity == null
                                           ? workerAvailableCapacity
                                           : availableCapacity + workerAvailableCapacity
      );
    }

    return totalIdlePeons;
  }

  @Override
  public Map<String, Long> getUsedTaskSlotCount()
  {
    Map<String, Long> totalUsedPeons = new HashMap<>();
    for (ImmutableWorkerInfo worker : getWorkers()) {
      String workerCategory = worker.getWorker().getCategory();
      int workerUsedCapacity = worker.getCurrCapacityUsed();
      totalUsedPeons.compute(
          workerCategory,
          (category, usedCapacity) -> usedCapacity == null ? workerUsedCapacity : usedCapacity + workerUsedCapacity
      );
    }

    return totalUsedPeons;
  }

  @Override
  public Map<String, Long> getLazyTaskSlotCount()
  {
    Map<String, Long> totalLazyPeons = new HashMap<>();
    for (Worker worker : getLazyWorkers()) {
      String workerCategory = worker.getCategory();
      int workerLazyPeons = worker.getCapacity();
      totalLazyPeons.compute(
          workerCategory,
          (category, lazyPeons) -> lazyPeons == null ? workerLazyPeons : lazyPeons + workerLazyPeons
      );
    }

    return totalLazyPeons;
  }

  @Override
  public Map<String, Long> getBlacklistedTaskSlotCount()
  {
    Map<String, Long> totalBlacklistedPeons = new HashMap<>();
    for (ImmutableWorkerInfo worker : getBlackListedWorkers()) {
      String workerCategory = worker.getWorker().getCategory();
      int workerBlacklistedPeons = worker.getWorker().getCapacity();
      totalBlacklistedPeons.compute(
          workerCategory,
          (category, blacklistedPeons) -> blacklistedPeons == null
                                          ? workerBlacklistedPeons
                                          : blacklistedPeons + workerBlacklistedPeons
      );
    }

    return totalBlacklistedPeons;
  }

  @Override
  public int getTotalCapacity()
  {
    return getWorkers().stream().mapToInt(workerInfo -> workerInfo.getWorker().getCapacity()).sum();
  }


  /**
   * Retrieves the maximum capacity of the task runner when autoscaling is enabled.*
   *
   * @return The maximum capacity as an integer value. Returns -1 if the maximum
   * capacity cannot be determined or if autoscaling is not enabled.
   */
  @Override
  public int getMaximumCapacityWithAutoscale()
  {
    int maximumCapacity = -1;
    WorkerBehaviorConfig workerBehaviorConfig = workerConfigRef.get();
    if (workerBehaviorConfig == null) {
      // Auto scale not setup
      log.debug("Cannot calculate maximum worker capacity as worker behavior config is not configured");
    } else if (workerBehaviorConfig instanceof DefaultWorkerBehaviorConfig) {
      DefaultWorkerBehaviorConfig defaultWorkerBehaviorConfig = (DefaultWorkerBehaviorConfig) workerBehaviorConfig;
      if (defaultWorkerBehaviorConfig.getAutoScaler() == null) {
        // Auto scale not setup
        log.debug("Cannot calculate maximum worker capacity as auto scaler not configured");
      } else {
        int maxWorker = defaultWorkerBehaviorConfig.getAutoScaler().getMaxNumWorkers();
        int expectedWorkerCapacity = provisioningStrategy.getExpectedWorkerCapacity(getWorkers());
        maximumCapacity = expectedWorkerCapacity == -1 ? -1 : maxWorker * expectedWorkerCapacity;
      }
    }
    return maximumCapacity;
  }

  @Override
  public int getUsedCapacity()
  {
    return getWorkers().stream().mapToInt(ImmutableWorkerInfo::getCurrCapacityUsed).sum();
  }

  private static class HttpRemoteTaskRunnerWorkItem extends RemoteTaskRunnerWorkItem
  {
    enum State
    {
      // Task has been given to HRTR, but a worker to run this task hasn't been identified yet.
      PENDING(0, true, RunnerTaskState.PENDING),

      // A Worker has been identified to run this task, but request to run task hasn't been made to worker yet
      // or worker hasn't acknowledged the task yet.
      PENDING_WORKER_ASSIGN(1, true, RunnerTaskState.PENDING),

      RUNNING(2, false, RunnerTaskState.RUNNING),
      COMPLETE(3, false, RunnerTaskState.NONE);

      private final int index;
      private final boolean isPending;
      private final RunnerTaskState runnerTaskState;

      State(int index, boolean isPending, RunnerTaskState runnerTaskState)
      {
        this.index = index;
        this.isPending = isPending;
        this.runnerTaskState = runnerTaskState;
      }

      boolean isPending()
      {
        return isPending;
      }

      boolean inProgress()
      {
        return isPending || runnerTaskState == RunnerTaskState.RUNNING;
      }

      RunnerTaskState toRunnerTaskState()
      {
        return runnerTaskState;
      }
    }

    private volatile Task task;
    private volatile State state;

    HttpRemoteTaskRunnerWorkItem(
        String taskId,
        Worker worker,
        TaskLocation location,
        @Nullable Task task,
        String taskType,
        State state
    )
    {
      super(taskId, task == null ? null : task.getType(), worker, location, task == null ? null : task.getDataSource());
      this.state = Preconditions.checkNotNull(state);
      Preconditions.checkArgument(task == null || taskType == null || taskType.equals(task.getType()));

      // It is possible to have it null when the TaskRunner is just started and discovered this taskId from a worker,
      // notifications don't contain whole Task instance but just metadata about the task.
      this.task = task;
    }

    public boolean isRunningOnWorker(Worker candidateWorker)
    {
      return getState() == State.RUNNING &&
             getWorker() != null &&
             Objects.equal(getWorker().getHost(), candidateWorker.getHost());
    }

    public Task getTask()
    {
      return task;
    }

    public void setTask(Task task)
    {
      this.task = task;
      if (getTaskType() == null) {
        setTaskType(task.getType());
      } else {
        Preconditions.checkArgument(getTaskType().equals(task.getType()));
      }
    }

    @JsonProperty
    public State getState()
    {
      return state;
    }

    @Override
    public void setResult(TaskStatus status)
    {
      setState(State.COMPLETE);
      super.setResult(status);
    }

    public void setState(State state)
    {
      Preconditions.checkArgument(
          state.index - this.state.index > 0,
          "Invalid state transition from [%s] to [%s]",
          this.state,
          state
      );

      setStateUnconditionally(state);
    }

    private void setStateUnconditionally(State state)
    {
      if (log.isDebugEnabled()) {
        // Exception is logged to know what led to this call.
        log.debug(
            new RuntimeException("Stacktrace..."),
            "Setting task[%s] work item state from [%s] to [%s].",
            getTaskId(),
            this.state,
            state
        );
      }
      this.state = state;
    }
  }

  private static class PendingTaskQueueItem implements Comparable<PendingTaskQueueItem>
  {
    private static final AtomicLong SEQUENCE_GENERATOR = new AtomicLong(0);

    private final Task task;
    private final long sequenceNumber;

    PendingTaskQueueItem(Task task)
    {
      this(task, SEQUENCE_GENERATOR.getAndIncrement());
    }

    @VisibleForTesting
    PendingTaskQueueItem(Task task, long sequenceNumber)
    {
      this.task = task;
      this.sequenceNumber = sequenceNumber;
    }

    public Task getTask()
    {
      return task;
    }

    /**
     * Creates a new PendingTaskQueueItem for the same task with a fresh sequence number.
     * This is used when requeueing a task to ensure it goes to the back of the queue
     * for its priority level.
     */
    public PendingTaskQueueItem withFreshSequenceNumber()
    {
      return new PendingTaskQueueItem(task, SEQUENCE_GENERATOR.getAndIncrement());
    }

    /**
     * Compares this item to another for priority queue ordering.
     * Items are ordered by:
     * 1. Task priority (higher priority first - note the reversed comparison)
     * 2. Sequence number (lower sequence number first - FIFO)
     */
    @Override
    public int compareTo(PendingTaskQueueItem other)
    {
      int priorityComparison = Integer.compare(other.task.getPriority(), this.task.getPriority());
      if (priorityComparison != 0) {
        return priorityComparison;
      }
      return Long.compare(this.sequenceNumber, other.sequenceNumber);
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      PendingTaskQueueItem that = (PendingTaskQueueItem) o;
      return sequenceNumber == that.sequenceNumber && task.equals(that.task);
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(task, sequenceNumber);
    }
  }
}
