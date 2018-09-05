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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
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
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerSelectStrategy;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
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
 * Middle Managers expose 3 HTTP endpoints
 *  1. POST request for assigning a task
 *  2. POST request for shutting down a task
 *  3. GET request for getting list of assigned, running, completed tasks on Middle Manager and its enable/disable status.
 *    This endpoint is implemented to support long poll and holds the request till there is a change. This class
 *    sends the next request immediately as the previous finishes to keep the state up-to-date.
 *
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

  // All known tasks
  private final ConcurrentMap<String, HttpRemoteTaskRunnerWorkItem> tasks = new ConcurrentHashMap<>();

  // All discovered workers.
  private final ConcurrentMap<String, WorkerHolder> workers = new ConcurrentHashMap<>();

  // Executor for syncing state of each worker.
  private final ScheduledExecutorService workersSyncExec;

  // Workers that have been marked as lazy. these workers are not running any tasks and can be terminated safely by the scaling policy.
  private final ConcurrentMap<String, WorkerHolder> lazyWorkers = new ConcurrentHashMap<>();

  // Workers that have been blacklisted.
  private final ConcurrentHashMap<String, WorkerHolder> blackListedWorkers = new ConcurrentHashMap<>();

  // workers which were assigned a task and are yet to acknowledge same.
  // Map: workerId -> taskId
  private final ConcurrentMap<String, String> workersWithUnacknowledgedTask = new ConcurrentHashMap<>();

  // Executor to complete cleanup of workers which have disappeared.
  private final ListeningScheduledExecutorService cleanupExec;
  private final ConcurrentMap<String, ScheduledFuture> removedWorkerCleanups = new ConcurrentHashMap<>();

  // Guards the pending/running/complete lists of tasks and list of workers
  // statusLock.notifyAll() is called whenever there is a possibility of worker slot to run task becoming available.
  // statusLock.notifyAll() is called whenever a task status or location changes.
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

  // ZK_CLEANUP_TODO : Remove these when RemoteTaskRunner and WorkerTaskMonitor are removed.
  private static final Joiner JOINER = Joiner.on("/");
  private final CuratorFramework cf;
  private final ScheduledExecutorService zkCleanupExec;
  private final IndexerZkConfig indexerZkConfig;

  public HttpRemoteTaskRunner(
      ObjectMapper smileMapper,
      HttpRemoteTaskRunnerConfig config,
      HttpClient httpClient,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ProvisioningStrategy<WorkerTaskRunner> provisioningStrategy,
      DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      TaskStorage taskStorage,
      CuratorFramework cf,
      IndexerZkConfig indexerZkConfig
  )
  {
    this.smileMapper = smileMapper;
    this.config = config;
    this.httpClient = httpClient;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.taskStorage = taskStorage;
    this.workerConfigRef = workerConfigRef;

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

    this.cf = cf;
    this.indexerZkConfig = indexerZkConfig;
    this.zkCleanupExec = ScheduledExecutors.fixed(
        1,
        "HttpRemoteTaskRunner-zk-cleanup-%d"
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

      scheduleCompletedTaskStatusCleanupFromZk();

      startWorkersHandling();

      ScheduledExecutors.scheduleAtFixedRate(
          cleanupExec,
          Period.ZERO.toStandardDuration(),
          config.getWorkerBlackListCleanupPeriod().toStandardDuration(),
          () -> checkAndRemoveWorkersFromBlackList()
      );

      provisioningService = provisioningStrategy.makeProvisioningService(this);

      scheduleSyncMonitoring();
      lifecycleLock.started();

      log.info("Started.");
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  private void scheduleCompletedTaskStatusCleanupFromZk()
  {
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
        ImmutableMap.copyOf(
            Maps.transformEntries(
                Maps.filterEntries(
                    workers, new Predicate<Map.Entry<String, WorkerHolder>>()
                    {
                      @Override
                      public boolean apply(Map.Entry<String, WorkerHolder> input)
                      {
                        return !lazyWorkers.containsKey(input.getKey()) &&
                               !workersWithUnacknowledgedTask.containsKey(input.getKey()) &&
                               !blackListedWorkers.containsKey(input.getKey());
                      }
                    }
                ),
                new Maps.EntryTransformer<String, WorkerHolder, ImmutableWorkerInfo>()
                {
                  @Override
                  public ImmutableWorkerInfo transformEntry(
                      String key, WorkerHolder value
                  )
                  {
                    return value.toImmutable();
                  }
                }
            )
        ),
        task
    );
  }

  private boolean runTaskOnWorker(
      final HttpRemoteTaskRunnerWorkItem workItem,
      final String workerHost
  ) throws Exception
  {
    String taskId = workItem.getTaskId();
    WorkerHolder workerHolder = workers.get(workerHost);

    if (workerHolder == null || lazyWorkers.containsKey(workerHost) || blackListedWorkers.containsKey(workerHost)) {
      log.info("Not assigning task[%s] to removed or marked lazy/blacklisted worker[%s]", taskId, workerHost);
      return false;
    }

    log.info("Asking Worker[%s] to run task[%s]", workerHost, taskId);

    if (workerHolder.assignTask(workItem.getTask())) {
      // Don't assign new tasks until the task we just assigned is actually running
      // on a worker - this avoids overflowing a worker with tasks
      long waitMs = config.getTaskAssignmentTimeout().toStandardDuration().getMillis();
      long waitStart = System.currentTimeMillis();
      boolean isTaskAssignmentTimedOut = false;
      synchronized (statusLock) {
        while (tasks.containsKey(taskId)
               && tasks.get(taskId).getState() == HttpRemoteTaskRunnerWorkItem.State.PENDING) {
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
        taskComplete(workItem, workerHolder, TaskStatus.failure(taskId));
      }

      return true;
    } else {
      return false;
    }
  }

  // CAUTION: This method calls RemoteTaskRunnerWorkItem.setResult(..) which results in TaskQueue.notifyStatus() being called
  // because that is attached by TaskQueue to task result future. So, this method must not be called with "statusLock"
  // held. See https://github.com/apache/incubator-druid/issues/6201
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

    // Notify interested parties
    taskRunnerWorkItem.setResult(taskStatus);
    TaskRunnerUtils.notifyStatusChanged(listeners, taskStatus.getId(), taskStatus);

    // Update success/failure counters, Blacklist node if there are too many failures.
    if (workerHolder != null) {
      blacklistWorkerIfNeeded(taskStatus, workerHolder);
    }

    synchronized (statusLock) {
      statusLock.notifyAll();
    }
  }

  private void startWorkersHandling() throws InterruptedException
  {
    final CountDownLatch workerViewInitialized = new CountDownLatch(1);
    DruidNodeDiscovery druidNodeDiscovery = druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_MM);
    druidNodeDiscovery.registerListener(
        new DruidNodeDiscovery.Listener()
        {
          @Override
          public void nodesAdded(List<DiscoveryDruidNode> nodes)
          {
            nodes.stream().forEach(node -> addWorker(toWorker(node)));

            //CountDownLatch.countDown() does nothing when count has already reached 0.
            workerViewInitialized.countDown();
          }

          @Override
          public void nodesRemoved(List<DiscoveryDruidNode> nodes)
          {
            nodes.stream().forEach(node -> removeWorker(toWorker(node)));
          }
        }
    );

    long workerDiscoveryStartTime = System.currentTimeMillis();
    while (!workerViewInitialized.await(30, TimeUnit.SECONDS)) {
      if (System.currentTimeMillis() - workerDiscoveryStartTime > TimeUnit.MINUTES.toMillis(5)) {
        throw new ISE("WTF! Couldn't discover workers.");
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
    return new Worker(
        node.getDruidNode().getServiceScheme(),
        node.getDruidNode().getHostAndPortToUse(),
        ((WorkerNodeService) node.getServices().get(WorkerNodeService.DISCOVERY_SERVICE_KEY)).getIp(),
        ((WorkerNodeService) node.getServices().get(WorkerNodeService.DISCOVERY_SERVICE_KEY)).getCapacity(),
        ((WorkerNodeService) node.getServices().get(WorkerNodeService.DISCOVERY_SERVICE_KEY)).getVersion()
    );
  }

  private void addWorker(final Worker worker)
  {
    synchronized (workers) {
      log.info("Worker[%s] reportin' for duty!", worker.getHost());
      cancelWorkerCleanup(worker.getHost());

      WorkerHolder holder = workers.get(worker.getHost());
      if (holder == null) {
        holder = createWorkerHolder(smileMapper, httpClient, config, workersSyncExec, this::taskAddedOrUpdated, worker);
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
      Worker worker
  )
  {
    return new WorkerHolder(smileMapper, httpClient, config, workersSyncExec, listener, worker);
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
          throw Throwables.propagate(e);
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
        new Runnable()
        {
          @Override
          public void run()
          {
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
                  log.info(
                      "Failing task[%s] because worker[%s] disappeared and did not report within cleanup timeout[%s].",
                      workerHostAndPort,
                      taskItem.getTaskId(),
                      config.getTaskCleanupTimeout()
                  );
                  taskComplete(taskItem, null, TaskStatus.failure(taskItem.getTaskId()));
                }
              }
            }
            catch (Exception e) {
              log.makeAlert("Exception while cleaning up worker[%s]", workerHostAndPort).emit();
              throw Throwables.propagate(e);
            }
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
        }
    );
  }

  private void scheduleSyncMonitoring()
  {
    workersSyncExec.scheduleAtFixedRate(
        () -> {
          log.debug("Running the Sync Monitoring.");

          try {
            for (Map.Entry<String, WorkerHolder> e : workers.entrySet()) {
              WorkerHolder workerHolder = e.getValue();
              if (!workerHolder.getUnderlyingSyncer().isOK()) {
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

  /**
   * This method returns the debugging information exposed by {@link HttpRemoteTaskRunnerResource} and meant
   * for that use only. It must not be used for any other purpose.
   */
  public Map<String, Object> getDebugInfo()
  {
    Preconditions.checkArgument(lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS));

    Map<String, Object> result = new HashMap<>(workers.size());
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

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    return lazyWorkers.values().stream().map(holder -> holder.getWorker()).collect(Collectors.toList());
  }

  @Override
  public Collection<Worker> markWorkersLazy(
      Predicate<ImmutableWorkerInfo> isLazyWorker, int maxWorkers
  )
  {
    synchronized (statusLock) {
      Iterator<String> iterator = workers.keySet().iterator();
      while (iterator.hasNext()) {
        String worker = iterator.next();
        WorkerHolder workerHolder = workers.get(worker);
        try {
          if (isWorkerOkForMarkingLazy(workerHolder.getWorker()) && isLazyWorker.apply(workerHolder.toImmutable())) {
            log.info("Adding Worker[%s] to lazySet!", workerHolder.getWorker().getHost());
            lazyWorkers.put(worker, workerHolder);
            if (lazyWorkers.size() == maxWorkers) {
              // only mark excess workers as lazy and allow their cleanup
              break;
            }
          }
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
      return getLazyWorkers();
    }
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
                  .filter(item -> item.getState() == HttpRemoteTaskRunnerWorkItem.State.PENDING)
                  .map(item -> item.getTask())
                  .collect(Collectors.toList());
    }
  }

  @Override
  public Optional<ByteSource> streamTaskLog(String taskId, long offset)
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
      final URL url = WorkerHolder.makeWorkerURL(worker, StringUtils.format("/druid/worker/v1/task/%s/log?offset=%d", taskId, offset));
      return Optional.of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              try {
                return httpClient.go(
                    new Request(HttpMethod.GET, url),
                    new InputStreamResponseHandler()
                ).get();
              }
              catch (InterruptedException e) {
                throw Throwables.propagate(e);
              }
              catch (ExecutionException e) {
                // Unwrap if possible
                Throwables.propagateIfPossible(e.getCause(), IOException.class);
                throw Throwables.propagate(e);
              }
            }
          }
      );
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
        addPendingTaskToExecutor(task.getId());
        return taskRunnerWorkItem.getResult();
      }
    }
  }

  private void addPendingTaskToExecutor(final String taskId)
  {
    pendingTasksExec.execute(
        () -> {
          while (!Thread.interrupted() && lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
            ImmutableWorkerInfo immutableWorker = null;
            HttpRemoteTaskRunnerWorkItem taskItem = null;
            try {
              synchronized (statusLock) {
                taskItem = tasks.get(taskId);

                if (taskItem == null) {
                  log.info(
                      "Task[%s] work item not found. Probably user asked to shutdown before. Not assigning.",
                      taskId
                  );
                  return;
                }

                if (taskItem.getState() != HttpRemoteTaskRunnerWorkItem.State.PENDING) {
                  log.info(
                      "Task[%s] is in state[%s]. Probably some worker already reported it. Not assigning.",
                      taskId,
                      taskItem.getState()
                  );
                  return;
                }

                if (taskItem.getTask() == null) {
                  throw new ISE("WTF! couldn't find Task instance for taskId[%s].", taskId);
                }
                immutableWorker = findWorkerToRunTask(taskItem.getTask());

                if (immutableWorker == null) {
                  // no free worker, wait for some worker to become free
                  statusLock.wait(config.getWaitForWorkerSlot().toStandardDuration().getMillis());
                  continue;
                } else if (workersWithUnacknowledgedTask.putIfAbsent(
                    immutableWorker.getWorker().getHost(),
                    taskId
                ) != null) {
                  // there was a race and someone else took this worker slot, try again
                  continue;
                }
              }

              try {
                // this will send HTTP request to worker for assigning task and hence kept
                // outside the synchronized block.
                if (runTaskOnWorker(taskItem, immutableWorker.getWorker().getHost())) {
                  return;
                }
              }
              finally {
                workersWithUnacknowledgedTask.remove(immutableWorker.getWorker().getHost());
                synchronized (statusLock) {
                  statusLock.notifyAll();
                }
              }
            }
            catch (InterruptedException ex) {
              log.info("Got InterruptedException while assigning task[%s].", taskId);
              Thread.currentThread().interrupt();

              return;
            }
            catch (Throwable th) {
              log.makeAlert(th, "Exception while trying to assign task")
                 .addData("taskId", taskId)
                 .emit();

              if (taskItem != null) {
                taskComplete(taskItem, null, TaskStatus.failure(taskId));
              }

              return;
            }
          }
        }
    );
  }

  @Override
  public void shutdown(String taskId)
  {
    if (!lifecycleLock.awaitStarted(1, TimeUnit.SECONDS)) {
      log.info("This TaskRunner is stopped or not yet started. Ignoring shutdown command for task: %s", taskId);
      return;
    }

    WorkerHolder workerHolderRunningTask = null;
    synchronized (statusLock) {
      HttpRemoteTaskRunnerWorkItem taskRunnerWorkItem = tasks.remove(taskId);
      if (taskRunnerWorkItem != null) {
        if (taskRunnerWorkItem.getState() == HttpRemoteTaskRunnerWorkItem.State.RUNNING) {
          workerHolderRunningTask = workers.get(taskRunnerWorkItem.getWorker().getHost());
          if (workerHolderRunningTask == null) {
            log.info("Can't shutdown! No worker running task[%s]", taskId);
          }
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

    log.info("Stopping...");

    pendingTasksExec.shutdownNow();
    workersSyncExec.shutdownNow();
    cleanupExec.shutdown();

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
                .filter(item -> item.getState() == HttpRemoteTaskRunnerWorkItem.State.PENDING)
                .collect(Collectors.toList());
  }

  @Override
  public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
  {
    synchronized (statusLock) {
      return ImmutableList.copyOf(tasks.values());
    }
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
      switch (workItem.state) {
        case PENDING:
          return RunnerTaskState.PENDING;
        case RUNNING:
          return RunnerTaskState.RUNNING;
        case COMPLETE:
          return RunnerTaskState.NONE;
        default:
          throw new ISE("Unknown state[%s]", workItem.state);
      }
    }
  }

  public List<String> getBlacklistedWorkers()
  {
    return blackListedWorkers.values().stream().map(
        (holder) -> holder.getWorker().getHost()
    ).collect(Collectors.toList());
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.fromNullable(provisioningService.getStats());
  }

  void taskAddedOrUpdated(final TaskAnnouncement announcement, final WorkerHolder workerHolder)
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
                  "WTF! Found unrecognized state[%s] of task[%s] in taskStorage. Notification[%s] from worker[%s] is ignored.",
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
                taskItem.setWorker(worker);
                taskItem.setState(HttpRemoteTaskRunnerWorkItem.State.RUNNING);
                log.info("Task[%s] started RUNNING on worker[%s].", taskId, worker.getHost());
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
                    "WTF! Found unrecognized state[%s] of task[%s]. Notification[%s] from worker[%s] is ignored.",
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
                    "WTF! Found unrecognized state[%s] of task[%s]. Notification[%s] from worker[%s] is ignored.",
                    taskItem.getState(),
                    taskId,
                    announcement,
                    worker.getHost()
                ).emit();
            }
            break;
          default:
            log.makeAlert(
                "WTF! Worker[%s] reported unrecognized state[%s] for task[%s].",
                worker.getHost(),
                announcement.getTaskStatus().getStatusCode(),
                taskId
            ).emit();
        }
      }
    }

    if (isTaskCompleted) {
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

  private static class HttpRemoteTaskRunnerWorkItem extends RemoteTaskRunnerWorkItem
  {
    enum State
    {
      PENDING(0),
      RUNNING(1),
      COMPLETE(2);

      int index;

      State(int index)
      {
        this.index = index;
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

      if (log.isDebugEnabled()) {
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
