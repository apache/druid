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
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.WorkerNodeService;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
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
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
public class HttpRemoteTaskRunnerV2 implements WorkerTaskRunner, TaskLogStreamer, WorkerHolder.Listener
{
  public static final String TASK_DISCOVERED_COUNT = "task/discovered/count";

  private static final EmittingLogger log = new EmittingLogger(HttpRemoteTaskRunnerV2.class);

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

  // Workers that have been blacklisted.
  private final ConcurrentHashMap<String, WorkerHolder> blackListedWorkers = new ConcurrentHashMap<>();

  // Executor to complete cleanup of workers which have disappeared.
  private final ListeningScheduledExecutorService cleanupExec;
  private final ConcurrentMap<String, ScheduledFuture> removedWorkerCleanups = new ConcurrentHashMap<>();

  private final ReadWriteLock statusLock = new ReentrantReadWriteLock();

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


  ////////////////// NEW //////////////////
  ////////////////// NEW //////////////////

  public HttpRemoteTaskRunnerV2(
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

  @GuardedBy("statusLock")
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
      final Task task,
      final String workerHost
  ) throws InterruptedException
  {
    // TODO: implement
    return false;
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
    // TODO: implement
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
    log.info("[%s] Workers are discovered.", workers.size());

    // Wait till all worker state is sync'd so that we know which worker is running/completed what tasks or else
    // We would start assigning tasks which are pretty soon going to be reported by discovered workers.
    for (String workerHost : workers.keySet()) {
      workers.computeIfPresent(workerHost, (key, workerEntry) -> {
        try {
          workerEntry.waitForInitialization();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        return workerEntry;
      });
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
    log.info("Worker[%s] reportin' for duty!", worker.getHost());
    statusLock.writeLock().lock();
    try {
      workers.compute(
          worker.getHost(), (key, workerEntry) -> {
            cancelWorkerCleanup(worker.getHost());

            // There cannot be any new tasks assigned to this worker as the entry has not been published yet.
            // That being said, there can be callbacks in taskAddedOrUpdated() where some task suddenly begins running
            // on this worker. That method still blocks on this key lock, so it will occur strictly before/after this insertion.
            if (workerEntry == null) {
              log.info("Unrecognized Worker[%s], rebuilding task mapping", worker.getHost());
              final List<TaskAnnouncement> expectedAnnouncements = new ArrayList<>();
              // It might be a worker that existed before, temporarily went away and came back. We might have a set of
              // tasks that we think are running on this worker. Provide that information to WorkerHolder that
              // manages the task syncing with that worker.
              for (Map.Entry<String, HttpRemoteTaskRunnerWorkItem> e : tasks.entrySet()) {
                HttpRemoteTaskRunnerWorkItem workItem = e.getValue();
                if (workItem.isRunningOnWorker(worker)) {
                  // This announcement is only used to notify when a task has disappeared on the worker
                  // So it is okay to set the dataSource and taskResource to null as they will not be used
                  expectedAnnouncements.add(
                      TaskAnnouncement.create(
                          workItem.getTaskId(),
                          workItem.getTaskType(),
                          null,
                          TaskStatus.running(workItem.getTaskId()),
                          workItem.getLocation(),
                          null
                      )
                  );
                }
              }

              workerEntry = createWorkerHolder(
                  smileMapper,
                  httpClient,
                  config,
                  workersSyncExec,
                  this,
                  worker,
                  expectedAnnouncements
              );
              workerEntry.start();
            } else {
              log.info("Worker[%s] already exists.", worker.getHost());
            }
            return workerEntry;
          }
      );

      // Notify any waiters that there is a new worker available
      statusLock.notifyAll();
    } finally {
      statusLock.writeLock().unlock();
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
    workers.compute(worker.getHost(), (key, workerEntry) -> {
      if (workerEntry != null) {
        try {
          workerEntry.stop();
          scheduleTasksCleanupForWorker(worker.getHost());
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        log.warn("Asked to remove a non-existent Worker[%s].", worker.getHost());
      }
      return null; // remove the worker entry
    });
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

            for (Map.Entry<String, HttpRemoteTaskRunnerWorkItem> e : tasks.entrySet()) {
              if (e.getValue().getState().inProgress()) {
                Worker w = e.getValue().getWorker();
                if (w != null && w.getHost().equals(workerHostAndPort)) {
                  tasksToFail.add(e.getValue());
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
    final AtomicBoolean shouldRunPendingTasks = new AtomicBoolean(false);

    for (final String workerHost : workers.keySet()) {
      workers.computeIfPresent(workerHost, (key, workerEntry) -> {
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
      });
    }

    if (shouldRunPendingTasks.get()) {
      // TODO: handle signal
    }
  }

  /**
    This method should be called under the corresponding worker key lock.
  */
  private boolean shouldRemoveNodeFromBlackList(WorkerHolder workerHolder)
  {
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
    return workers.values().stream().map(WorkerHolder::toImmutable).collect(Collectors.toList());
  }

  @VisibleForTesting
  ConcurrentMap<String, WorkerHolder> getWorkersForTestingReadOnly()
  {
    return workers;
  }

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    return workers.values().stream().filter(w -> w.getState() == WorkerHolder.State.LAZY).map(WorkerHolder::getWorker).collect(Collectors.toList());
  }

  @Override
  public Collection<Worker> markWorkersLazy(Predicate<ImmutableWorkerInfo> isLazyWorker, int maxLazyWorkers)
  {
    // Search for new workers to mark lazy.
    // Status lock is used to prevent any tasks being assigned to workers while we mark them lazy
    statusLock.writeLock().lock();
    try {
      AtomicInteger numMarkedLazy = new AtomicInteger(getLazyWorkers().size());
      workers.forEach((key, workerHolder) -> {
        if (numMarkedLazy.get() >= maxLazyWorkers) {
          return;
        }
        try {
          if (isWorkerOkForMarkingLazy(workerHolder) && isLazyWorker.apply(workerHolder.toImmutable())) {
            log.info("Adding Worker[%s] to lazySet!", workerHolder.getWorker().getHost());
            workerHolder.setState(WorkerHolder.State.LAZY);
            numMarkedLazy.incrementAndGet();
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    } finally {
      statusLock.writeLock().unlock();
    }
    return getLazyWorkers();
  }

  @GuardedBy("statusLock")
  private boolean isWorkerOkForMarkingLazy(WorkerHolder workerHolder)
  {
    // Check that worker is not already lazy, and does not have any in-flight tasks being assigned to it.
    if (workerHolder.getState() == WorkerHolder.State.LAZY || workerHolder.getState() == WorkerHolder.State.PENDING_ASSIGN) {
      log.debug("Skipping marking lazy worker[%s] with state[%s]", workerHolder.getWorker().getHost(), workerHolder.getState());
      return false;
    }

    // Check that worker has no in-flight/running tasks associated with it
    for (Map.Entry<String, HttpRemoteTaskRunnerWorkItem> e : tasks.entrySet()) {
      if (e.getValue().getState().inProgress()) {
        Worker w = e.getValue().getWorker();
        if (w != null && w.getHost().equals(workerHolder.getWorker().getHost())) {
          return false;
        }
      }
    }
    return true;
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

    for (final String taskId : tasks.keySet()) {
      tasks.computeIfPresent(taskId, (key, taskEntry) -> {
        if (taskEntry.getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
          TaskRunnerUtils.notifyLocationChanged(
              ImmutableList.of(listenerPair),
              taskId,
              taskEntry.getLocation()
          );
        }
        return taskEntry;
      });
    }

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

    tasks.compute(task.getId(), (id, entry) -> {
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
        pendingTasks.offer(new PendingTaskQueueItem(task)); // TODO: make Task::getPriority + time-based order the priority
      }

      taskFuture.set(entry.getResult());
      return entry;
    });

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

  private void pendingTasksExecutionLoop()
  {
    while (!Thread.interrupted() && lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      try {
        final PendingTaskQueueItem taskItem = pendingTasks.poll(1, TimeUnit.MINUTES);
        if (taskItem == null) {
          log.info("Found no available tasks. Waiting for tasks to assign.");
          continue;
        }

        ImmutableWorkerInfo workerToAssign;
        int workerFetchRetries = 0;
        statusLock.writeLock().lock();
        try {
          do {
            workerToAssign = findWorkerToRunTask(taskItem.getTask());
            if (workerToAssign == null) {
              log.warn("No workers available to run task[%s]. Waiting", taskItem.getTask().getId());

            }
          } while (workerToAssign == null && ++workerFetchRetries < 2); // TODO: cleanup

          // Mark this worker as unassignable while task is being assigned
          workers.compute(workerToAssign.getWorker().getHost(), (key, entry) -> {
            Preconditions.checkState(entry != null, "Expected selected worker[%s] to be available", entry.getWorker().getHost());
            Preconditions.checkState(entry.getState() == WorkerHolder.State.READY, "Expected worker[%s] state to be READY, got [%s]", entry.getWorker().getHost(), entry.getState());

            entry.setState(WorkerHolder.State.PENDING_ASSIGN);
            return entry;
          });
        } finally {
          statusLock.writeLock().unlock();
        }

        tasks.compute(taskItem.getTask().getId(), (key, entry) -> {
          Preconditions.checkState(entry != null, "Expected task[%s] to be in tasks set", taskItem.getTask().getId());
          Preconditions.checkState(entry.getState() == HttpRemoteTaskRunnerWorkItem.State.PENDING, "Expected task[%s] to be PENDING", taskItem.getTask().getId());

          entry.setState(HttpRemoteTaskRunnerWorkItem.State.PENDING_WORKER_ASSIGN);
          return entry;
        });

        // TODO: implement
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
    log.info("Shutdown task[%s] because: [%s]", taskId, reason);

    AtomicReference<WorkerHolder> workerHolderRunningTaskRef = new AtomicReference<>();
    tasks.compute(taskId, (key, entry) -> {
      if (entry != null) {
        if (entry.getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
          workerHolderRunningTaskRef.set(workers.get(entry.getWorker().getHost()));
        } else if (entry.getState() == HttpRemoteTaskRunnerWorkItem.State.COMPLETE) {
          entry = null; // delete the entry
        }
      } else {
        log.info("Received shutdown task[%s], but can't find it. Ignored.", taskId);
      }
      return entry;
    });

    if (workerHolderRunningTaskRef.get() != null) {
      log.debug(
          "Got shutdown request for task[%s]. Asking worker[%s] to kill it.",
          taskId,
          workerHolderRunningTaskRef.get().getWorker().getHost()
      );
      workerHolderRunningTaskRef.get().shutdownTask(taskId);
    } else {
      log.info("Can't shutdown! No worker running task[%s]", taskId);
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
    return workers.values().stream().filter(w -> w.getState() == WorkerHolder.State.BLACKLISTED).map(WorkerHolder::toImmutable).collect(Collectors.toList());
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.fromNullable(provisioningService.getStats());
  }

  @Override
  public void taskAddedOrUpdated(final TaskAnnouncement announcement, final WorkerHolder workerHolder)
  {
    // TODO: implement
  }

  @Override
  public void stateChanged(boolean enabled, WorkerHolder workerHolder)
  {
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


  /**
   * Retrieves the maximum capacity of the task runner when autoscaling is enabled.*
   * @return The maximum capacity as an integer value. Returns -1 if the maximum
   *         capacity cannot be determined or if autoscaling is not enabled.
   */
  @Override
  public int getMaximumCapacityWithAutoscale()
  {
    int maximumCapacity = -1;
    WorkerBehaviorConfig workerBehaviorConfig = workerConfigRef.get();
    if (workerBehaviorConfig == null) {
      // Auto scale not setup
      log.debug("Cannot calculate maximum worker capacity as worker behavior config is not configured");
      maximumCapacity = -1;
    } else if (workerBehaviorConfig instanceof DefaultWorkerBehaviorConfig) {
      DefaultWorkerBehaviorConfig defaultWorkerBehaviorConfig = (DefaultWorkerBehaviorConfig) workerBehaviorConfig;
      if (defaultWorkerBehaviorConfig.getAutoScaler() == null) {
        // Auto scale not setup
        log.debug("Cannot calculate maximum worker capacity as auto scaler not configured");
        maximumCapacity = -1;
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

  private static class PendingTaskQueueItem implements Comparable<PendingTaskQueueItem> {
    private final Task task;
    private final DateTime createdAt; // original insertion time
    
    PendingTaskQueueItem(Task task) {
      this.task = task;
      this.createdAt = DateTime.now();
    }
    
    public Task getTask() {
      return task;
    }

    public DateTime getCreatedAt()
    {
      return createdAt;
    }

    @Override
    public int compareTo(PendingTaskQueueItem other)
    {
      return this.createdAt.compareTo(other.createdAt);
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
      return task.equals(that.task) && createdAt.equals(that.createdAt);
    }

    @Override
    public int hashCode()
    {
      return Objects.hashCode(task, createdAt);
    }
  }
}

/*
Global Synchronization Points:
1. Selecting a worker from the queue for assignment to a task – need to ensure no other thread is assigning a task to this worker
  - Can be solved by exclusive lock over workers to select a worker, mark it as PENDING_ASSIGN
2. Checking all known running tasks to "reload" a worker state if it has gone away – need to ensure no tasks are being concurrently assigned to this worker
  - Can be solved by acquiring worker-IP key-level lock before iterating tasks, to ensure all tasks are accounted for.
3. Mark workers lazy – need to ensure no tasks are being concurrently assigned to this worker
  - Task can only be added from add() and taskAddedOrUpdated(). Assuming you hold the same exclusive lock over the workers (see #1), you can check the workers by doing an atomic get and set:
    - Check if the worker status is not READY
    - Check if any RUNNING tasks with this worker
4. Registering a listener (notify state has changed) – need to ensure no concurrent access? This doesn't seem like a requirement

Races:
- Non-existent/Blacklisted/Lazy/Duplicate worker selected for task assignment
  - Worker state updates should be exclusive, i.e the task assignment algo should see unobstructed view of workers until operation completes.
- Task add/shutdown racing with each other
  - Task is added (either explicitly via add() or via taskAddedOrUpdated()) and immediately shutdown(), while assignment loop continues
  - Task is shutdown() and add()/taskAddedOrUpdated() is run, overriding shutdown()
*/
