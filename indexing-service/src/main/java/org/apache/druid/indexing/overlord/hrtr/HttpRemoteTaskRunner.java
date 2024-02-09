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
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
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
import org.apache.curator.framework.CuratorFramework;
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
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.handler.codec.http.HttpMethod;
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
import java.util.Iterator;
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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A Remote TaskRunner to manage tasks on Middle Manager nodes using internal-discovery({@link DruidNodeDiscoveryProvider})
 * to discover them and Http.
 * Middle Managers manages list of assigned/completed tasks on disk and expose 3 HTTP endpoints
 * 1. POST request for assigning a task
 * 2. POST request for shutting down a task
 * 3. GET request for getting list of assigned, running, completed tasks on Middle Manager and its enable/disable status.
 * This endpoint is implemented to support long poll and holds the request till there is a change. This class
 * sends the next request immediately as the previous finishes to keep the state up-to-date.
 * <p>
 * ZK_CLEANUP_TODO : As of 0.11.1, it is required to cleanup task status paths from ZK which are created by the
 * workers to support deprecated RemoteTaskRunner. So a method "scheduleCompletedTaskStatusCleanupFromZk()" is added'
 * which should be removed in the release that removes RemoteTaskRunner legacy ZK updation WorkerTaskMonitor class.
 */
public class HttpRemoteTaskRunner implements WorkerTaskRunner, TaskLogStreamer
{
  private static final EmittingLogger log = new EmittingLogger(HttpRemoteTaskRunner.class);

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  // Executor for assigning pending tasks to workers.
  private final ExecutorService pendingTasksExec;

  // All known tasks, TaskID -> HttpRemoteTaskRunnerWorkItem
  // This is a ConcurrentMap as some of the reads are done without holding the lock.
  @GuardedBy("statusLock")
  private final ConcurrentMap<String, HttpRemoteTaskRunnerWorkItem> tasks = new ConcurrentHashMap<>();

  // This is the list of pending tasks in the order they arrived, exclusively manipulated/used by thread that
  // gives a new task to this class and threads in pendingTasksExec that are responsible for assigning tasks to
  // workers.
  @GuardedBy("statusLock")
  private final List<String> pendingTaskIds = new ArrayList<>();

  // All discovered workers, "host:port" -> WorkerHolder
  private final ConcurrentMap<String, WorkerHolder> workers = new ConcurrentHashMap<>();

  // Executor for syncing state of each worker.
  private final ScheduledExecutorService workersSyncExec;

  // Workers that have been marked as lazy. these workers are not running any tasks and can be terminated safely by the scaling policy.
  private final ConcurrentMap<String, WorkerHolder> lazyWorkers = new ConcurrentHashMap<>();

  // Workers that have been blacklisted.
  private final ConcurrentHashMap<String, WorkerHolder> blackListedWorkers = new ConcurrentHashMap<>();

  // workers which were assigned a task and are yet to acknowledge same.
  // Map: workerId -> taskId
  // all writes are guarded
  @GuardedBy("statusLock")
  private final ConcurrentMap<String, String> workersWithUnacknowledgedTask = new ConcurrentHashMap<>();

  // Executor to complete cleanup of workers which have disappeared.
  private final ListeningScheduledExecutorService cleanupExec;
  private final ConcurrentMap<String, ScheduledFuture> removedWorkerCleanups = new ConcurrentHashMap<>();


  private final Object statusLock = new Object();

  // task runner listeners
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  private final ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy;
  private ProvisioningService provisioningService;

  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final HttpClient httpClient;
  private final ObjectMapper smileMapper;

  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final HttpRemoteTaskRunnerConfig config;

  private final TaskStorage taskStorage;
  private final ServiceEmitter emitter;

  // ZK_CLEANUP_TODO : Remove these when RemoteTaskRunner and WorkerTaskMonitor are removed.
  private static final Joiner JOINER = Joiner.on("/");

  @Nullable // Null, if zk is disabled
  private final CuratorFramework cf;

  @Nullable // Null, if zk is disabled
  private final ScheduledExecutorService zkCleanupExec;
  private final IndexerZkConfig indexerZkConfig;
  private volatile DruidNodeDiscovery.Listener nodeDiscoveryListener;

  public HttpRemoteTaskRunner(
      ObjectMapper smileMapper,
      HttpRemoteTaskRunnerConfig config,
      HttpClient httpClient,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      TaskStorage taskStorage,
      @Nullable CuratorFramework cf,
      IndexerZkConfig indexerZkConfig,
      ServiceEmitter emitter
  )
  {
    this.smileMapper = smileMapper;
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
        "HttpRemoteTaskRunner-worker-sync-%d"
    );

    this.cleanupExec = MoreExecutors.listeningDecorator(
        ScheduledExecutors.fixed(1, "HttpRemoteTaskRunner-Worker-Cleanup-%d")
    );

    if (cf != null) {
      this.cf = cf;
      this.zkCleanupExec = ScheduledExecutors.fixed(
          1,
          "HttpRemoteTaskRunner-zk-cleanup-%d"
      );
    } else {
      this.cf = null;
      this.zkCleanupExec = null;
    }

    this.indexerZkConfig = indexerZkConfig;

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

      scheduleCompletedTaskStatusCleanupFromZk();

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

  private void scheduleCompletedTaskStatusCleanupFromZk()
  {
    if (cf == null) {
      return;
    }

    zkCleanupExec.scheduleAtFixedRate(
        () -> {
          try {
            List<String> workers;
            try {
              workers = cf.getChildren().forPath(indexerZkConfig.getStatusPath());
            }
            catch (KeeperException.NoNodeException e) {
              // statusPath doesn't exist yet; can occur if no middleManagers have started.
              workers = ImmutableList.of();
            }

            Set<String> knownActiveTaskIds = new HashSet<>();
            if (!workers.isEmpty()) {
              for (Task task : taskStorage.getActiveTasks()) {
                knownActiveTaskIds.add(task.getId());
              }
            }

            for (String workerId : workers) {
              String workerStatusPath = JOINER.join(indexerZkConfig.getStatusPath(), workerId);

              List<String> taskIds;
              try {
                taskIds = cf.getChildren().forPath(workerStatusPath);
              }
              catch (KeeperException.NoNodeException e) {
                taskIds = ImmutableList.of();
              }

              for (String taskId : taskIds) {
                if (!knownActiveTaskIds.contains(taskId)) {
                  String taskStatusPath = JOINER.join(workerStatusPath, taskId);
                  try {
                    cf.delete().guaranteed().forPath(taskStatusPath);
                  }
                  catch (KeeperException.NoNodeException e) {
                    log.info("Failed to delete taskStatusPath[%s].", taskStatusPath);
                  }
                }
              }
            }
          }
          catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
          }
          catch (Exception ex) {
            log.error(ex, "Unknown error while doing task status cleanup in ZK.");
          }
        },
        1,
        5,
        TimeUnit.MINUTES
    );
  }

  /**
   * Must not be used outside of this class and {@link HttpRemoteTaskRunnerResource}
   */
  @SuppressWarnings("GuardedBy") // Read on workersWithUnacknowledgedTask is safe
  Map<String, ImmutableWorkerInfo> getWorkersEligibleToRunTasks()
  {
    // In this class, this method is called with statusLock held.
    // writes to workersWithUnacknowledgedTask are always guarded by statusLock.
    // however writes to lazyWorker/blacklistedWorkers aren't necessarily guarded by same lock, so technically there
    // could be races in that a task could get assigned to a worker which in another thread is concurrently being
    // marked lazy/blacklisted , but that is ok because that is equivalent to this worker being picked for task and
    // being assigned lazy/blacklisted right after even when the two threads hold a mutually exclusive lock.
    return Maps.transformEntries(
        Maps.filterEntries(
            workers,
            input -> !lazyWorkers.containsKey(input.getKey()) &&
                     !workersWithUnacknowledgedTask.containsKey(input.getKey()) &&
                     !blackListedWorkers.containsKey(input.getKey()) &&
                     input.getValue().isInitialized() &&
                     input.getValue().isEnabled()
        ),
        (String key, WorkerHolder value) -> value.toImmutable()
    );
  }

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
      final HttpRemoteTaskRunnerWorkItem workItem,
      final String workerHost
  ) throws InterruptedException
  {
    String taskId = workItem.getTaskId();
    WorkerHolder workerHolder = workers.get(workerHost);

    if (workerHolder == null || lazyWorkers.containsKey(workerHost) || blackListedWorkers.containsKey(workerHost)) {
      log.info("Not assigning task[%s] to removed or marked lazy/blacklisted worker[%s]", taskId, workerHost);
      return false;
    }

    log.info("Assigning task [%s] to worker [%s]", taskId, workerHost);

    if (workerHolder.assignTask(workItem.getTask())) {
      // Don't assign new tasks until the task we just assigned is actually running
      // on a worker - this avoids overflowing a worker with tasks
      long waitMs = config.getTaskAssignmentTimeout().toStandardDuration().getMillis();
      long waitStart = System.currentTimeMillis();
      boolean isTaskAssignmentTimedOut = false;
      synchronized (statusLock) {
        while (tasks.containsKey(taskId) && tasks.get(taskId).getState().isPending()) {
          long remaining = waitMs - (System.currentTimeMillis() - waitStart);
          if (remaining > 0) {
            statusLock.wait(remaining);
          } else {
            isTaskAssignmentTimedOut = true;
            break;
          }
        }
      }

      if (isTaskAssignmentTimedOut) {
        log.makeAlert(
            "Task assignment timed out on worker [%s], never ran task [%s] in timeout[%s]!",
            workerHost,
            taskId,
            config.getTaskAssignmentTimeout()
        ).emit();
        // taskComplete(..) must be called outside of statusLock, see comments on method.
        taskComplete(
            workItem,
            workerHolder,
            TaskStatus.failure(
                taskId,
                StringUtils.format(
                    "The worker that this task is assigned did not start it in timeout[%s]. "
                    + "See overlord and middleManager/indexer logs for more details.",
                    config.getTaskAssignmentTimeout()
                )
            )
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
      HttpRemoteTaskRunnerWorkItem taskRunnerWorkItem,
      WorkerHolder workerHolder,
      TaskStatus taskStatus
  )
  {
    Preconditions.checkState(!Thread.holdsLock(statusLock), "Current thread must not hold statusLock.");
    Preconditions.checkNotNull(taskRunnerWorkItem, "taskRunnerWorkItem");
    Preconditions.checkNotNull(taskStatus, "taskStatus");
    if (workerHolder != null) {
      log.info(
          "Worker[%s] completed task[%s] with status[%s]",
          workerHolder.getWorker().getHost(),
          taskStatus.getId(),
          taskStatus.getStatusCode()
      );
      // Worker is done with this task
      workerHolder.setLastCompletedTaskTime(DateTimes.nowUtc());
    }

    if (taskRunnerWorkItem.getResult().isDone()) {
      // This is not the first complete event.
      try {
        TaskState lastKnownState = taskRunnerWorkItem.getResult().get().getStatusCode();
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
      taskRunnerWorkItem.setResult(taskStatus);
      TaskRunnerUtils.notifyStatusChanged(listeners, taskStatus.getId(), taskStatus);

      // Update success/failure counters, Blacklist node if there are too many failures.
      if (workerHolder != null) {
        blacklistWorkerIfNeeded(taskStatus, workerHolder);
      }
    }

    synchronized (statusLock) {
      statusLock.notifyAll();
    }
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
    log.info("[%s] Workers are discovered.", workers.size());

    // Wait till all worker state is sync'd so that we know which worker is running/completed what tasks or else
    // We would start assigning tasks which are pretty soon going to be reported by discovered workers.
    for (WorkerHolder worker : workers.values()) {
      log.info("Waiting for worker[%s] to sync state...", worker.getWorker().getHost());
      worker.waitForInitialization();
    }
    log.info("Workers have sync'd state successfully.");
  }

  private Worker toWorker(DiscoveryDruidNode node)
  {
    final WorkerNodeService workerNodeService = node.getService(WorkerNodeService.DISCOVERY_SERVICE_KEY, WorkerNodeService.class);
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
    synchronized (workers) {
      log.info("Worker[%s] reportin' for duty!", worker.getHost());
      cancelWorkerCleanup(worker.getHost());

      WorkerHolder holder = workers.get(worker.getHost());
      if (holder == null) {
        List<TaskAnnouncement> expectedAnnouncements = new ArrayList<>();
        synchronized (statusLock) {
          // It might be a worker that existed before, temporarily went away and came back. We might have a set of
          // tasks that we think are running on this worker. Provide that information to WorkerHolder that
          // manages the task syncing with that worker.
          for (Map.Entry<String, HttpRemoteTaskRunnerWorkItem> e : tasks.entrySet()) {
            if (e.getValue().getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
              Worker w = e.getValue().getWorker();
              if (w != null && w.getHost().equals(worker.getHost()) && e.getValue().getTask() != null) {
                expectedAnnouncements.add(
                    TaskAnnouncement.create(
                        e.getValue().getTask(),
                        TaskStatus.running(e.getKey()),
                        e.getValue().getLocation()
                    )
                );
              }
            }
          }
        }
        holder = createWorkerHolder(
            smileMapper,
            httpClient,
            config,
            workersSyncExec,
            this::taskAddedOrUpdated,
            worker,
            expectedAnnouncements
        );
        holder.start();
        workers.put(worker.getHost(), holder);
      } else {
        log.info("Worker[%s] already exists.", worker.getHost());
      }
    }

    synchronized (statusLock) {
      statusLock.notifyAll();
    }
  }

  protected WorkerHolder createWorkerHolder(
      ObjectMapper smileMapper,
      HttpClient httpClient,
      HttpRemoteTaskRunnerConfig config,
      ScheduledExecutorService workersSyncExec,
      WorkerHolder.Listener listener,
      Worker worker,
      List<TaskAnnouncement> knownAnnouncements
  )
  {
    return new WorkerHolder(smileMapper, httpClient, config, workersSyncExec, listener, worker, knownAnnouncements);
  }

  private void removeWorker(final Worker worker)
  {
    synchronized (workers) {
      log.info("Kaboom! Worker[%s] removed!", worker.getHost());

      WorkerHolder workerHolder = workers.remove(worker.getHost());

      if (workerHolder != null) {
        try {
          workerHolder.stop();
          scheduleTasksCleanupForWorker(worker.getHost());
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
        finally {
          checkAndRemoveWorkersFromBlackList();
        }
      }
      lazyWorkers.remove(worker.getHost());
    }
  }

  private boolean cancelWorkerCleanup(String workerHost)
  {
    ScheduledFuture previousCleanup = removedWorkerCleanups.remove(workerHost);
    if (previousCleanup != null) {
      log.info("Cancelling Worker[%s] scheduled task cleanup", workerHost);
      previousCleanup.cancel(false);
    }
    return previousCleanup != null;
  }

  private void scheduleTasksCleanupForWorker(final String workerHostAndPort)
  {
    cancelWorkerCleanup(workerHostAndPort);

    final ListenableScheduledFuture<?> cleanupTask = cleanupExec.schedule(
        () -> {
          log.info("Running scheduled cleanup for Worker[%s]", workerHostAndPort);
          try {
            Set<HttpRemoteTaskRunnerWorkItem> tasksToFail = new HashSet<>();
            synchronized (statusLock) {
              for (Map.Entry<String, HttpRemoteTaskRunnerWorkItem> e : tasks.entrySet()) {
                if (e.getValue().getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
                  Worker w = e.getValue().getWorker();
                  if (w != null && w.getHost().equals(workerHostAndPort)) {
                    tasksToFail.add(e.getValue());
                  }
                }
              }
            }

            for (HttpRemoteTaskRunnerWorkItem taskItem : tasksToFail) {
              if (!taskItem.getResult().isDone()) {
                log.warn(
                    "Failing task[%s] because worker[%s] disappeared and did not report within cleanup timeout[%s].",
                    taskItem.getTaskId(),
                    workerHostAndPort,
                    config.getTaskCleanupTimeout()
                );
                // taskComplete(..) must be called outside of statusLock, see comments on method.
                taskComplete(
                    taskItem,
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
            log.makeAlert("Exception while cleaning up worker[%s]", workerHostAndPort).emit();
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
          log.debug("Running the Sync Monitoring.");

          try {
            syncMonitoring();
          }
          catch (Exception ex) {
            if (ex instanceof InterruptedException) {
              Thread.currentThread().interrupt();
            } else {
              log.makeAlert(ex, "Exception in sync monitoring.").emit();
            }
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
        synchronized (workers) {
          // check again that server is still there and only then reset.
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
  }

  /**
   * This method returns the debugging information exposed by {@link HttpRemoteTaskRunnerResource} and meant
   * for that use only. It must not be used for any other purpose.
   */
  Map<String, Object> getWorkerSyncerDebugInfo()
  {
    Preconditions.checkArgument(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    Map<String, Object> result = Maps.newHashMapWithExpectedSize(workers.size());
    for (Map.Entry<String, WorkerHolder> e : workers.entrySet()) {
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
    boolean shouldRunPendingTasks = false;

    Iterator<Map.Entry<String, WorkerHolder>> iterator = blackListedWorkers.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, WorkerHolder> e = iterator.next();
      if (shouldRemoveNodeFromBlackList(e.getValue())) {
        iterator.remove();
        e.getValue().resetContinuouslyFailedTasksCount();
        e.getValue().setBlacklistedUntil(null);
        shouldRunPendingTasks = true;
      }
    }

    if (shouldRunPendingTasks) {
      synchronized (statusLock) {
        statusLock.notifyAll();
      }
    }
  }

  private boolean shouldRemoveNodeFromBlackList(WorkerHolder workerHolder)
  {
    if (!workers.containsKey(workerHolder.getWorker().getHost())) {
      return true;
    }

    if (blackListedWorkers.size() > workers.size() * (config.getMaxPercentageBlacklistWorkers() / 100.0)) {
      log.info(
          "Removing [%s] from blacklist because percentage of blacklisted workers exceeds [%d]",
          workerHolder.getWorker(),
          config.getMaxPercentageBlacklistWorkers()
      );

      return true;
    }

    long remainingMillis = workerHolder.getBlacklistedUntil().getMillis() - System.currentTimeMillis();
    if (remainingMillis <= 0) {
      log.info("Removing [%s] from blacklist because backoff time elapsed", workerHolder.getWorker());
      return true;
    }

    log.info("[%s] still blacklisted for [%,ds]", workerHolder.getWorker(), remainingMillis / 1000);
    return false;
  }

  private void blacklistWorkerIfNeeded(TaskStatus taskStatus, WorkerHolder workerHolder)
  {
    synchronized (blackListedWorkers) {
      if (taskStatus.isSuccess()) {
        workerHolder.resetContinuouslyFailedTasksCount();
        if (blackListedWorkers.remove(workerHolder.getWorker().getHost()) != null) {
          workerHolder.setBlacklistedUntil(null);
          log.info("[%s] removed from blacklist because a task finished with SUCCESS", workerHolder.getWorker());
        }
      } else if (taskStatus.isFailure()) {
        workerHolder.incrementContinuouslyFailedTasksCount();
      }

      if (workerHolder.getContinuouslyFailedTasksCount() > config.getMaxRetriesBeforeBlacklist() &&
          blackListedWorkers.size() <= workers.size() * (config.getMaxPercentageBlacklistWorkers() / 100.0) - 1) {
        workerHolder.setBlacklistedUntil(DateTimes.nowUtc().plus(config.getWorkerBlackListBackoffTime()));
        if (blackListedWorkers.put(workerHolder.getWorker().getHost(), workerHolder) == null) {
          log.info(
              "Blacklisting [%s] until [%s] after [%,d] failed tasks in a row.",
              workerHolder.getWorker(),
              workerHolder.getBlacklistedUntil(),
              workerHolder.getContinuouslyFailedTasksCount()
          );
        }
      }
    }
  }

  @Override
  public Collection<ImmutableWorkerInfo> getWorkers()
  {
    return workers.values().stream().map(worker -> worker.toImmutable()).collect(Collectors.toList());
  }

  @VisibleForTesting
  ConcurrentMap<String, WorkerHolder> getWorkersForTestingReadOnly()
  {
    return workers;
  }

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    return lazyWorkers.values().stream().map(holder -> holder.getWorker()).collect(Collectors.toList());
  }

  @Override
  public Collection<Worker> markWorkersLazy(Predicate<ImmutableWorkerInfo> isLazyWorker, int maxLazyWorkers)
  {
    // skip the lock and bail early if we should not mark any workers lazy (e.g. number
    // of current workers is at or below the minNumWorkers of autoscaler config)
    if (lazyWorkers.size() >= maxLazyWorkers) {
      return getLazyWorkers();
    }

    // Search for new workers to mark lazy.
    // Status lock is used to prevent any tasks being assigned to workers while we mark them lazy
    synchronized (statusLock) {
      for (Map.Entry<String, WorkerHolder> worker : workers.entrySet()) {
        if (lazyWorkers.size() >= maxLazyWorkers) {
          break;
        }
        final WorkerHolder workerHolder = worker.getValue();
        try {
          if (isWorkerOkForMarkingLazy(workerHolder.getWorker()) && isLazyWorker.apply(workerHolder.toImmutable())) {
            log.info("Adding Worker[%s] to lazySet!", workerHolder.getWorker().getHost());
            lazyWorkers.put(worker.getKey(), workerHolder);
          }
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }

    return getLazyWorkers();
  }

  private boolean isWorkerOkForMarkingLazy(Worker worker)
  {
    // Check that worker is not running any tasks and no task is being assigned to it.
    synchronized (statusLock) {
      if (workersWithUnacknowledgedTask.containsKey(worker.getHost())) {
        return false;
      }

      for (Map.Entry<String, HttpRemoteTaskRunnerWorkItem> e : tasks.entrySet()) {
        if (e.getValue().getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
          Worker w = e.getValue().getWorker();
          if (w != null && w.getHost().equals(worker.getHost())) {
            return false;
          }
        }
      }

      return true;
    }
  }

  @Override
  public WorkerTaskRunnerConfig getConfig()
  {
    return config;
  }

  @Override
  public Collection<Task> getPendingTaskPayloads()
  {
    synchronized (statusLock) {
      return tasks.values()
                  .stream()
                  .filter(item -> item.getState().isPending())
                  .map(HttpRemoteTaskRunnerWorkItem::getTask)
                  .collect(Collectors.toList());
    }
  }

  @Override
  public Optional<InputStream> streamTaskLog(String taskId, long offset) throws IOException
  {
    @SuppressWarnings("GuardedBy") // Read on tasks is safe
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
    @SuppressWarnings("GuardedBy") // Read on tasks is safe
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

    synchronized (statusLock) {
      for (Map.Entry<String, HttpRemoteTaskRunnerWorkItem> entry : tasks.entrySet()) {
        if (entry.getValue().getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
          TaskRunnerUtils.notifyLocationChanged(
              ImmutableList.of(listenerPair),
              entry.getKey(),
              entry.getValue().getLocation()
          );
        }
      }

      log.info("Registered listener [%s]", listener.getListenerId());
      listeners.add(listenerPair);
    }
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
    Preconditions.checkState(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS), "not started");

    synchronized (statusLock) {
      HttpRemoteTaskRunnerWorkItem existing = tasks.get(task.getId());

      if (existing != null) {
        log.info("Assigned a task[%s] that is known already. Ignored.", task.getId());
        if (existing.getTask() == null) {
          // in case it was discovered from a worker on start() and TaskAnnouncement does not have Task instance
          // in it.
          existing.setTask(task);
        }
        return existing.getResult();
      } else {
        log.info("Adding pending task[%s].", task.getId());
        HttpRemoteTaskRunnerWorkItem taskRunnerWorkItem = new HttpRemoteTaskRunnerWorkItem(
            task.getId(),
            null,
            null,
            task,
            task.getType(),
            HttpRemoteTaskRunnerWorkItem.State.PENDING
        );
        tasks.put(task.getId(), taskRunnerWorkItem);
        pendingTaskIds.add(task.getId());

        statusLock.notifyAll();

        return taskRunnerWorkItem.getResult();
      }
    }
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

  private void pendingTasksExecutionLoop()
  {
    while (!Thread.interrupted() && lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      try {
        // Find one pending task to run and a worker to run on
        HttpRemoteTaskRunnerWorkItem taskItem = null;
        ImmutableWorkerInfo immutableWorker = null;

        synchronized (statusLock) {
          Iterator<String> iter = pendingTaskIds.iterator();
          while (iter.hasNext()) {
            String taskId = iter.next();
            HttpRemoteTaskRunnerWorkItem ti = tasks.get(taskId);

            if (ti == null || !ti.getState().isPending()) {
              // happens if the task was shutdown, failed or observed running by a worker
              iter.remove();
              continue;
            }

            if (ti.getState() == HttpRemoteTaskRunnerWorkItem.State.PENDING_WORKER_ASSIGN) {
              // picked up by another pending task executor thread which is in the process of trying to
              // run it on a worker, skip to next.
              continue;
            }

            if (ti.getTask() == null) {
              // this is not supposed to happen except for a bug, we want to mark this task failed but
              // taskComplete(..) can not be called while holding statusLock. See the javadoc on that
              // method.
              // so this will get marked failed afterwards outside of current synchronized block.
              taskItem = ti;
              break;
            }

            immutableWorker = findWorkerToRunTask(ti.getTask());
            if (immutableWorker == null) {
              continue;
            }

            String prevUnackedTaskId = workersWithUnacknowledgedTask.putIfAbsent(
                immutableWorker.getWorker().getHost(),
                taskId
            );
            if (prevUnackedTaskId != null) {
              log.makeAlert(
                  "Found worker[%s] with unacked task[%s] but still was identified to run task[%s].",
                  immutableWorker.getWorker().getHost(),
                  prevUnackedTaskId,
                  taskId
              ).emit();
            }

            // set state to PENDING_WORKER_ASSIGN before releasing the lock so that this task item is not picked
            // up by another task execution thread.
            // note that we can't simply delete this task item from pendingTaskIds or else we would have to add it
            // back if this thread couldn't run this task for any reason, which we will know at some later time
            // and also we will need to add it back to its old position in the list. that becomes complex quickly.
            // Instead we keep the PENDING_WORKER_ASSIGN to notify other task execution threads not to pick this one up.
            // And, it is automatically removed by any of the task execution threads when they notice that
            // ti.getState().isPending() is false (at the beginning of this loop)
            ti.setState(HttpRemoteTaskRunnerWorkItem.State.PENDING_WORKER_ASSIGN);
            taskItem = ti;
            break;
          }

          if (taskItem == null) {
            // Either no pending task is found or no suitable worker is found for any of the pending tasks.
            // statusLock.notifyAll() is called whenever a new task shows up or if there is a possibility for a task
            // to successfully get worker to run, for example when a new worker shows up, a task slot opens up
            // because some task completed etc.
            statusLock.wait(TimeUnit.MINUTES.toMillis(1));
            continue;
          }
        }

        String taskId = taskItem.getTaskId();

        if (taskItem.getTask() == null) {
          log.makeAlert("No Task obj found in TaskItem for taskID[%s]. Failed.", taskId).emit();
          // taskComplete(..) must be called outside of statusLock, see comments on method.
          taskComplete(
              taskItem,
              null,
              TaskStatus.failure(
                  taskId,
                  "No payload found for this task. "
                  + "See overlord logs and middleManager/indexer logs for more details."
              )
          );
          continue;
        }

        if (immutableWorker == null) {
          throw new ISE("Unexpected state: null immutableWorker");
        }

        try {
          // this will send HTTP request to worker for assigning task
          if (!runTaskOnWorker(taskItem, immutableWorker.getWorker().getHost())) {
            if (taskItem.getState() == HttpRemoteTaskRunnerWorkItem.State.PENDING_WORKER_ASSIGN) {
              taskItem.revertStateFromPendingWorkerAssignToPending();
            }
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

          // taskComplete(..) must be called outside of statusLock, see comments on method.
          taskComplete(
              taskItem,
              null,
              TaskStatus.failure(taskId, "Failed to assign this task. See overlord logs for more details.")
          );
        }
        finally {
          synchronized (statusLock) {
            workersWithUnacknowledgedTask.remove(immutableWorker.getWorker().getHost());
            statusLock.notifyAll();
          }
        }

      }
      catch (InterruptedException ex) {
        log.info("Interrupted, will Exit.");
        Thread.currentThread().interrupt();
      }
      catch (Throwable th) {
        log.makeAlert(th, "Unknown Exception while trying to assign tasks.").emit();
      }
    }
  }

  /**
   * Must not be used outside of this class and {@link HttpRemoteTaskRunnerResource}
   */
  List<String> getPendingTasksList()
  {
    synchronized (statusLock) {
      return ImmutableList.copyOf(pendingTaskIds);
    }
  }

  @Override
  public void shutdown(String taskId, String reason)
  {
    if (!lifecycleLock.awaitStarted(1, TimeUnit.SECONDS)) {
      log.info("This TaskRunner is stopped or not yet started. Ignoring shutdown command for task: %s", taskId);
      return;
    }

    WorkerHolder workerHolderRunningTask = null;
    synchronized (statusLock) {
      log.info("Shutdown [%s] because: [%s]", taskId, reason);
      HttpRemoteTaskRunnerWorkItem taskRunnerWorkItem = tasks.get(taskId);
      if (taskRunnerWorkItem != null) {
        if (taskRunnerWorkItem.getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
          workerHolderRunningTask = workers.get(taskRunnerWorkItem.getWorker().getHost());
          if (workerHolderRunningTask == null) {
            log.info("Can't shutdown! No worker running task[%s]", taskId);
          }
        } else if (taskRunnerWorkItem.getState() == HttpRemoteTaskRunnerWorkItem.State.COMPLETE) {
          tasks.remove(taskId);
        }
      } else {
        log.info("Received shutdown task[%s], but can't find it. Ignored.", taskId);
      }
    }

    //shutdown is called outside of lock as we don't want to hold the lock while sending http request
    //to worker.
    if (workerHolderRunningTask != null) {
      log.debug(
          "Got shutdown request for task[%s]. Asking worker[%s] to kill it.",
          taskId,
          workerHolderRunningTask.getWorker().getHost()
      );
      workerHolderRunningTask.shutdownTask(taskId);
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
      synchronized (workers) {
        workers.values().forEach(w -> {
          try {
            w.stop();
          }
          catch (Exception e) {
            log.error(e, e.getMessage());
          }
        });
      }
    }
    finally {
      lifecycleLock.exitStop();
    }

    log.info("Stopped.");
  }

  @Override
  @SuppressWarnings("GuardedBy") // Read on tasks is safe
  public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING)
                .collect(Collectors.toList());
  }

  @Override
  @SuppressWarnings("GuardedBy") // Read on tasks is safe
  public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState().isPending())
                .collect(Collectors.toList());
  }

  @Override
  @SuppressWarnings("GuardedBy") // Read on tasks is safe
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    return ImmutableList.copyOf(tasks.values());
  }

  @SuppressWarnings("GuardedBy") // Read on tasks is safe
  public Collection<? extends TaskRunnerWorkItem> getCompletedTasks()
  {
    return tasks.values()
                .stream()
                .filter(item -> item.getState() == HttpRemoteTaskRunnerWorkItem.State.COMPLETE)
                .collect(Collectors.toList());
  }

  @Nullable
  @Override
  @SuppressWarnings("GuardedBy") // Read on tasks is safe
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
  @SuppressWarnings("GuardedBy") // Read on tasks is safe
  public TaskLocation getTaskLocation(String taskId)
  {
    final HttpRemoteTaskRunnerWorkItem workItem = tasks.get(taskId);
    if (workItem == null) {
      return TaskLocation.unknown();
    } else {
      return workItem.getLocation();
    }
  }

  public List<String> getBlacklistedWorkers()
  {
    return blackListedWorkers.values().stream().map(
        (holder) -> holder.getWorker().getHost()
    ).collect(Collectors.toList());
  }

  public Collection<ImmutableWorkerInfo> getBlackListedWorkers()
  {
    return ImmutableList.copyOf(Collections2.transform(blackListedWorkers.values(), WorkerHolder::toImmutable));
  }

  /**
   * Must not be used outside of this class and {@link HttpRemoteTaskRunnerResource} , used for read only.
   */
  @SuppressWarnings("GuardedBy")
  Map<String, String> getWorkersWithUnacknowledgedTasks()
  {
    return workersWithUnacknowledgedTask;
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.fromNullable(provisioningService.getStats());
  }

  @VisibleForTesting
  public void taskAddedOrUpdated(final TaskAnnouncement announcement, final WorkerHolder workerHolder)
  {
    final String taskId = announcement.getTaskId();
    final Worker worker = workerHolder.getWorker();

    log.debug(
        "Worker[%s] wrote [%s] status for task [%s] on [%s]",
        worker.getHost(),
        announcement.getTaskStatus().getStatusCode(),
        taskId,
        announcement.getTaskLocation()
    );

    HttpRemoteTaskRunnerWorkItem taskItem;
    boolean shouldShutdownTask = false;
    boolean isTaskCompleted = false;

    synchronized (statusLock) {
      taskItem = tasks.get(taskId);
      if (taskItem == null) {
        // Try to find information about it in the TaskStorage
        Optional<TaskStatus> knownStatusInStorage = taskStorage.getStatus(taskId);

        if (knownStatusInStorage.isPresent()) {
          switch (knownStatusInStorage.get().getStatusCode()) {
            case RUNNING:
              taskItem = new HttpRemoteTaskRunnerWorkItem(
                  taskId,
                  worker,
                  TaskLocation.unknown(),
                  null,
                  announcement.getTaskType(),
                  HttpRemoteTaskRunnerWorkItem.State.RUNNING
              );
              tasks.put(taskId, taskItem);
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

      if (taskItem == null) {
        if (!announcement.getTaskStatus().isComplete()) {
          shouldShutdownTask = true;
        }
      } else {
        switch (announcement.getTaskStatus().getStatusCode()) {
          case RUNNING:
            switch (taskItem.getState()) {
              case PENDING:
              case PENDING_WORKER_ASSIGN:
                taskItem.setWorker(worker);
                taskItem.setState(HttpRemoteTaskRunnerWorkItem.State.RUNNING);
                log.info("Task[%s] started RUNNING on worker[%s].", taskId, worker.getHost());

                final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder();
                IndexTaskUtils.setTaskDimensions(metricBuilder, taskItem.getTask());
                emitter.emit(metricBuilder.setMetric(
                    "task/pending/time",
                    new Duration(taskItem.getCreatedTime(), DateTimes.nowUtc()).getMillis())
                );

                // fall through
              case RUNNING:
                if (worker.getHost().equals(taskItem.getWorker().getHost())) {
                  if (!announcement.getTaskLocation().equals(taskItem.getLocation())) {
                    log.info(
                        "Task[%s] location changed on worker[%s]. new location[%s].",
                        taskId,
                        worker.getHost(),
                        announcement.getTaskLocation()
                    );
                    taskItem.setLocation(announcement.getTaskLocation());
                    TaskRunnerUtils.notifyLocationChanged(listeners, taskId, announcement.getTaskLocation());
                  }
                } else {
                  log.warn(
                      "Found worker[%s] running task[%s] which is being run by another worker[%s]. Notification ignored.",
                      worker.getHost(),
                      taskId,
                      taskItem.getWorker().getHost()
                  );
                  shouldShutdownTask = true;
                }
                break;
              case COMPLETE:
                log.warn(
                    "Worker[%s] reported status for completed task[%s]. Ignored.",
                    worker.getHost(),
                    taskId
                );
                shouldShutdownTask = true;
                break;
              default:
                log.makeAlert(
                    "Found unrecognized state[%s] of task[%s]. Notification[%s] from worker[%s] is ignored.",
                    taskItem.getState(),
                    taskId,
                    announcement,
                    worker.getHost()
                ).emit();
            }
            break;
          case FAILED:
          case SUCCESS:
            switch (taskItem.getState()) {
              case PENDING:
              case PENDING_WORKER_ASSIGN:
                taskItem.setWorker(worker);
                taskItem.setState(HttpRemoteTaskRunnerWorkItem.State.RUNNING);
                log.info("Task[%s] finished on worker[%s].", taskId, worker.getHost());
                // fall through
              case RUNNING:
                if (worker.getHost().equals(taskItem.getWorker().getHost())) {
                  if (!announcement.getTaskLocation().equals(taskItem.getLocation())) {
                    log.info(
                        "Task[%s] location changed on worker[%s]. new location[%s].",
                        taskId,
                        worker.getHost(),
                        announcement.getTaskLocation()
                    );
                    taskItem.setLocation(announcement.getTaskLocation());
                    TaskRunnerUtils.notifyLocationChanged(listeners, taskId, announcement.getTaskLocation());
                  }

                  isTaskCompleted = true;
                } else {
                  log.warn(
                      "Worker[%s] reported completed task[%s] which is being run by another worker[%s]. Notification ignored.",
                      worker.getHost(),
                      taskId,
                      taskItem.getWorker().getHost()
                  );
                }
                break;
              case COMPLETE:
                // this can happen when a worker is restarted and reports its list of completed tasks again.
                break;
              default:
                log.makeAlert(
                    "Found unrecognized state[%s] of task[%s]. Notification[%s] from worker[%s] is ignored.",
                    taskItem.getState(),
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
    }

    if (isTaskCompleted) {
      // taskComplete(..) must be called outside of statusLock, see comments on method.
      taskComplete(taskItem, workerHolder, announcement.getTaskStatus());
    }

    if (shouldShutdownTask) {
      log.warn("Killing task[%s] on worker[%s].", taskId, worker.getHost());
      workerHolder.shutdownTask(taskId);
    }

    synchronized (statusLock) {
      statusLock.notifyAll();
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
          (category, availableCapacity) -> availableCapacity == null ? workerAvailableCapacity : availableCapacity + workerAvailableCapacity
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
          (category, blacklistedPeons) -> blacklistedPeons == null ? workerBlacklistedPeons : blacklistedPeons + workerBlacklistedPeons
      );
    }

    return totalBlacklistedPeons;
  }

  @Override
  public int getTotalCapacity()
  {
    return getWorkers().stream().mapToInt(workerInfo -> workerInfo.getWorker().getCapacity()).sum();
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

      RunnerTaskState toRunnerTaskState()
      {
        return runnerTaskState;
      }
    }

    private Task task;
    private State state;

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

    public void revertStateFromPendingWorkerAssignToPending()
    {
      Preconditions.checkState(
          this.state == State.PENDING_WORKER_ASSIGN,
          "Can't move state from [%s] to [%s]",
          this.state,
          State.PENDING
      );

      setStateUnconditionally(State.PENDING);
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
}
