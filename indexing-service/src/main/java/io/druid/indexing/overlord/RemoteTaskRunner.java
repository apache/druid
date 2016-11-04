/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.concurrent.Execs;
import io.druid.curator.CuratorUtils;
import io.druid.curator.cache.PathChildrenCacheFactory;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerSelectStrategy;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.tasklogs.TaskLogStreamer;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The RemoteTaskRunner's primary responsibility is to assign tasks to worker nodes.
 * The RemoteTaskRunner uses Zookeeper to keep track of which workers are running which tasks. Tasks are assigned by
 * creating ephemeral nodes in ZK that workers must remove. Workers announce the statuses of the tasks they are running.
 * Once a task completes, it is up to the RTR to remove the task status and run any necessary cleanup.
 * The RemoteTaskRunner is event driven and updates state according to ephemeral node changes in ZK.
 * <p>
 * The RemoteTaskRunner will assign tasks to a node until the node hits capacity. At that point, task assignment will
 * fail. The RemoteTaskRunner depends on another component to create additional worker resources.
 * <p>
 * If a worker node becomes inexplicably disconnected from Zk, the RemoteTaskRunner will fail any tasks associated with the
 * worker after waiting for RemoteTaskRunnerConfig.taskCleanupTimeout for the worker to show up.
 * <p>
 * The RemoteTaskRunner uses ZK for job management and assignment and http for IPC messages.
 */
public class RemoteTaskRunner implements WorkerTaskRunner, TaskLogStreamer
{
  private static final EmittingLogger log = new EmittingLogger(RemoteTaskRunner.class);
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);
  private static final Joiner JOINER = Joiner.on("/");

  private final ObjectMapper jsonMapper;
  private final RemoteTaskRunnerConfig config;
  private final Duration shutdownTimeout;
  private final IndexerZkConfig indexerZkConfig;
  private final CuratorFramework cf;
  private final PathChildrenCacheFactory pathChildrenCacheFactory;
  private final PathChildrenCache workerPathCache;
  private final HttpClient httpClient;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;

  // all workers that exist in ZK
  private final ConcurrentMap<String, ZkWorker> zkWorkers = new ConcurrentHashMap<>();
  // payloads of pending tasks, which we remember just long enough to assign to workers
  private final ConcurrentMap<String, Task> pendingTaskPayloads = new ConcurrentHashMap<>();
  // tasks that have not yet been assigned to a worker
  private final RemoteTaskRunnerWorkQueue pendingTasks = new RemoteTaskRunnerWorkQueue();
  // all tasks that have been assigned to a worker
  private final RemoteTaskRunnerWorkQueue runningTasks = new RemoteTaskRunnerWorkQueue();
  // tasks that are complete but not cleaned up yet
  private final RemoteTaskRunnerWorkQueue completeTasks = new RemoteTaskRunnerWorkQueue();

  private final ExecutorService runPendingTasksExec;

  // Workers that have been marked as lazy. these workers are not running any tasks and can be terminated safely by the scaling policy.
  private final ConcurrentMap<String, ZkWorker> lazyWorkers = new ConcurrentHashMap<>();

  // task runner listeners
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  // workers which were assigned a task and are yet to acknowledge same.
  // Map: workerId -> taskId
  private final ConcurrentMap<String, String> workersWithUnacknowledgedTask = new ConcurrentHashMap<>();
  // Map: taskId -> taskId .tasks which are being tried to be assigned to a worker
  private final ConcurrentMap<String, String> tryAssignTasks = new ConcurrentHashMap<>();

  private final Object statusLock = new Object();

  private volatile boolean started = false;

  private final ListeningScheduledExecutorService cleanupExec;

  private final ConcurrentMap<String, ScheduledFuture> removedWorkerCleanups = new ConcurrentHashMap<>();
  private final ResourceManagementStrategy<WorkerTaskRunner> resourceManagement;

  public RemoteTaskRunner(
      ObjectMapper jsonMapper,
      RemoteTaskRunnerConfig config,
      IndexerZkConfig indexerZkConfig,
      CuratorFramework cf,
      PathChildrenCacheFactory pathChildrenCacheFactory,
      HttpClient httpClient,
      Supplier<WorkerBehaviorConfig> workerConfigRef,
      ScheduledExecutorService cleanupExec,
      ResourceManagementStrategy<WorkerTaskRunner> resourceManagement
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.shutdownTimeout = config.getTaskShutdownLinkTimeout().toStandardDuration(); // Fail fast
    this.indexerZkConfig = indexerZkConfig;
    this.cf = cf;
    this.pathChildrenCacheFactory = pathChildrenCacheFactory;
    this.workerPathCache = pathChildrenCacheFactory.make(cf, indexerZkConfig.getAnnouncementsPath());
    this.httpClient = httpClient;
    this.workerConfigRef = workerConfigRef;
    this.cleanupExec = MoreExecutors.listeningDecorator(cleanupExec);
    this.resourceManagement = resourceManagement;
    this.runPendingTasksExec = Execs.multiThreaded(
        config.getPendingTasksRunnerNumThreads(),
        "rtr-pending-tasks-runner-%d"
    );
  }

  @Override
  @LifecycleStart
  public void start()
  {
    try {
      if (started) {
        return;
      }

      final MutableInt waitingFor = new MutableInt(1);
      final Object waitingForMonitor = new Object();

      // Add listener for creation/deletion of workers
      workerPathCache.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, final PathChildrenCacheEvent event) throws Exception
            {
              final Worker worker;
              switch (event.getType()) {
                case CHILD_ADDED:
                  worker = jsonMapper.readValue(
                      event.getData().getData(),
                      Worker.class
                  );
                  synchronized (waitingForMonitor) {
                    waitingFor.increment();
                  }
                  Futures.addCallback(
                      addWorker(worker),
                      new FutureCallback<ZkWorker>()
                      {
                        @Override
                        public void onSuccess(ZkWorker zkWorker)
                        {
                          synchronized (waitingForMonitor) {
                            waitingFor.decrement();
                            waitingForMonitor.notifyAll();
                          }
                        }

                        @Override
                        public void onFailure(Throwable throwable)
                        {
                          synchronized (waitingForMonitor) {
                            waitingFor.decrement();
                            waitingForMonitor.notifyAll();
                          }
                        }
                      }
                  );
                  break;
                case CHILD_UPDATED:
                  worker = jsonMapper.readValue(
                      event.getData().getData(),
                      Worker.class
                  );
                  updateWorker(worker);
                  break;

                case CHILD_REMOVED:
                  worker = jsonMapper.readValue(
                      event.getData().getData(),
                      Worker.class
                  );
                  removeWorker(worker);
                  break;
                case INITIALIZED:
                  // Schedule cleanup for task status of the workers that might have disconnected while overlord was not running
                  List<String> workers;
                  try {
                    workers = cf.getChildren().forPath(indexerZkConfig.getStatusPath());
                  }
                  catch (KeeperException.NoNodeException e) {
                    // statusPath doesn't exist yet; can occur if no middleManagers have started.
                    workers = ImmutableList.of();
                  }
                  for (String workerId : workers) {
                    final String workerAnnouncePath = JOINER.join(indexerZkConfig.getAnnouncementsPath(), workerId);
                    final String workerStatusPath = JOINER.join(indexerZkConfig.getStatusPath(), workerId);
                    if (!zkWorkers.containsKey(workerId) && cf.checkExists().forPath(workerAnnouncePath) == null) {
                      try {
                        scheduleTasksCleanupForWorker(workerId, cf.getChildren().forPath(workerStatusPath));
                      }
                      catch (Exception e) {
                        log.warn(
                            e,
                            "Could not schedule cleanup for worker[%s] during startup (maybe someone removed the status znode[%s]?). Skipping.",
                            workerId,
                            workerStatusPath
                        );
                      }
                    }
                  }
                  synchronized (waitingForMonitor) {
                    waitingFor.decrement();
                    waitingForMonitor.notifyAll();
                  }
                default:
                  break;
              }
            }
          }
      );
      workerPathCache.start(PathChildrenCache.StartMode.POST_INITIALIZED_EVENT);
      synchronized (waitingForMonitor) {
        while (waitingFor.intValue() > 0) {
          waitingForMonitor.wait();
        }
      }
      resourceManagement.startManagement(this);
      started = true;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    try {
      if (!started) {
        return;
      }
      started = false;

      resourceManagement.stopManagement();

      for (ZkWorker zkWorker : zkWorkers.values()) {
        zkWorker.close();
      }
      workerPathCache.close();

      if (runPendingTasksExec != null) {
        runPendingTasksExec.shutdown();
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
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
      for (Map.Entry<String, RemoteTaskRunnerWorkItem> entry : runningTasks.entrySet()) {
        TaskRunnerUtils.notifyLocationChanged(
            ImmutableList.of(listenerPair),
            entry.getKey(),
            entry.getValue().getLocation()
        );
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
  public Collection<ImmutableWorkerInfo> getWorkers()
  {
    return getImmutableWorkerFromZK(zkWorkers.values());
  }

  @Override
  public Collection<RemoteTaskRunnerWorkItem> getRunningTasks()
  {
    return ImmutableList.copyOf(runningTasks.values());
  }

  @Override
  public Collection<RemoteTaskRunnerWorkItem> getPendingTasks()
  {
    return ImmutableList.copyOf(pendingTasks.values());
  }

  public Collection<Task> getPendingTaskPayloads()
  {
    // return a snapshot of current pending task payloads.
    return ImmutableList.copyOf(pendingTaskPayloads.values());
  }

  public RemoteTaskRunnerConfig getConfig()
  {
    return config;
  }

  @Override
  public Collection<RemoteTaskRunnerWorkItem> getKnownTasks()
  {
    // Racey, since there is a period of time during assignment when a task is neither pending nor running
    return ImmutableList.copyOf(Iterables.concat(pendingTasks.values(), runningTasks.values(), completeTasks.values()));
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.fromNullable(resourceManagement.getStats());
  }

  public ZkWorker findWorkerRunningTask(String taskId)
  {
    for (ZkWorker zkWorker : zkWorkers.values()) {
      if (zkWorker.isRunningTask(taskId)) {
        return zkWorker;
      }
    }
    return null;
  }

  public boolean isWorkerRunningTask(Worker worker, String taskId)
  {
    ZkWorker zkWorker = zkWorkers.get(worker.getHost());
    return (zkWorker != null && zkWorker.isRunningTask(taskId));
  }

  /**
   * A task will be run only if there is no current knowledge in the RemoteTaskRunner of the task.
   *
   * @param task task to run
   */
  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    final RemoteTaskRunnerWorkItem completeTask, runningTask, pendingTask;
    if ((pendingTask = pendingTasks.get(task.getId())) != null) {
      log.info("Assigned a task[%s] that is already pending, not doing anything", task.getId());
      return pendingTask.getResult();
    } else if ((runningTask = runningTasks.get(task.getId())) != null) {
      ZkWorker zkWorker = findWorkerRunningTask(task.getId());
      if (zkWorker == null) {
        log.warn("Told to run task[%s], but no worker has started running it yet.", task.getId());
      } else {
        log.info("Task[%s] already running on %s.", task.getId(), zkWorker.getWorker().getHost());
        TaskAnnouncement announcement = zkWorker.getRunningTasks().get(task.getId());
        if (announcement.getTaskStatus().isComplete()) {
          taskComplete(runningTask, zkWorker, announcement.getTaskStatus());
        }
      }
      return runningTask.getResult();
    } else if ((completeTask = completeTasks.get(task.getId())) != null) {
      return completeTask.getResult();
    } else {
      return addPendingTask(task).getResult();
    }
  }

  /**
   * Finds the worker running the task and forwards the shutdown signal to the worker.
   *
   * @param taskId - task id to shutdown
   */
  @Override
  public void shutdown(final String taskId)
  {
    if (!started) {
      log.info("This TaskRunner is stopped. Ignoring shutdown command for task: %s", taskId);
    } else if (pendingTasks.remove(taskId) != null) {
      pendingTaskPayloads.remove(taskId);
      log.info("Removed task from pending queue: %s", taskId);
    } else if (completeTasks.containsKey(taskId)) {
      cleanup(taskId);
    } else {
      final ZkWorker zkWorker = findWorkerRunningTask(taskId);

      if (zkWorker == null) {
        log.info("Can't shutdown! No worker running task %s", taskId);
        return;
      }
      URL url = null;
      try {
        url = makeWorkerURL(zkWorker.getWorker(), String.format("/task/%s/shutdown", taskId));
        final StatusResponseHolder response = httpClient.go(
            new Request(HttpMethod.POST, url),
            RESPONSE_HANDLER,
            shutdownTimeout
        ).get();

        log.info(
            "Sent shutdown message to worker: %s, status %s, response: %s",
            zkWorker.getWorker().getHost(),
            response.getStatus(),
            response.getContent()
        );

        if (!HttpResponseStatus.OK.equals(response.getStatus())) {
          log.error("Shutdown failed for %s! Are you sure the task was running?", taskId);
        }
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RE(e, "Interrupted posting shutdown to [%s] for task [%s]", url, taskId);
      }
      catch (Exception e) {
        throw new RE(e, "Error in handling post to [%s] for task [%s]", zkWorker.getWorker().getHost(), taskId);
      }
    }
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskId, final long offset)
  {
    final ZkWorker zkWorker = findWorkerRunningTask(taskId);

    if (zkWorker == null) {
      // Worker is not running this task, it might be available in deep storage
      return Optional.absent();
    } else {
      // Worker is still running this task
      final URL url = makeWorkerURL(zkWorker.getWorker(), String.format("/task/%s/log?offset=%d", taskId, offset));
      return Optional.<ByteSource>of(
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

  private URL makeWorkerURL(Worker worker, String path)
  {
    Preconditions.checkArgument(path.startsWith("/"), "path must start with '/': %s", path);

    try {
      return new URL(String.format("http://%s/druid/worker/v1%s", worker.getHost(), path));
    }
    catch (MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Adds a task to the pending queue
   */
  private RemoteTaskRunnerWorkItem addPendingTask(final Task task)
  {
    log.info("Added pending task %s", task.getId());
    final RemoteTaskRunnerWorkItem taskRunnerWorkItem = new RemoteTaskRunnerWorkItem(task.getId(), null, null);
    pendingTaskPayloads.put(task.getId(), task);
    pendingTasks.put(task.getId(), taskRunnerWorkItem);
    runPendingTasks();
    return taskRunnerWorkItem;
  }

  /**
   * This method uses a multi-threaded executor to extract all pending tasks and attempt to run them. Any tasks that
   * are successfully assigned to a worker will be moved from pendingTasks to runningTasks. This method is thread-safe.
   * This method should be run each time there is new worker capacity or if new tasks are assigned.
   */
  private void runPendingTasks()
  {
    runPendingTasksExec.submit(
        new Callable<Void>()
        {
          @Override
          public Void call() throws Exception
          {
            try {
              // make a copy of the pending tasks because tryAssignTask may delete tasks from pending and move them
              // into running status
              List<RemoteTaskRunnerWorkItem> copy = Lists.newArrayList(pendingTasks.values());
              sortByInsertionTime(copy);

              for (RemoteTaskRunnerWorkItem taskRunnerWorkItem : copy) {
                String taskId = taskRunnerWorkItem.getTaskId();
                if (tryAssignTasks.putIfAbsent(taskId, taskId) == null) {
                  try {
                    //this can still be null due to race from explicit task shutdown request
                    //or if another thread steals and completes this task right after this thread makes copy
                    //of pending tasks. See https://github.com/druid-io/druid/issues/2842 .
                    Task task = pendingTaskPayloads.get(taskId);
                    if (task != null && tryAssignTask(task, taskRunnerWorkItem)) {
                      pendingTaskPayloads.remove(taskId);
                    }
                  }
                  catch (Exception e) {
                    log.makeAlert(e, "Exception while trying to assign task")
                       .addData("taskId", taskRunnerWorkItem.getTaskId())
                       .emit();
                    RemoteTaskRunnerWorkItem workItem = pendingTasks.remove(taskId);
                    if (workItem != null) {
                      taskComplete(workItem, null, TaskStatus.failure(taskId));
                    }
                  }
                  finally {
                    tryAssignTasks.remove(taskId);
                  }
                }
              }
            }
            catch (Exception e) {
              log.makeAlert(e, "Exception in running pending tasks").emit();
            }

            return null;
          }
        }
    );
  }

  @VisibleForTesting
  static void sortByInsertionTime(List<RemoteTaskRunnerWorkItem> tasks)
  {
    Collections.sort(tasks, new Comparator<RemoteTaskRunnerWorkItem>()
                     {
                       @Override
                       public int compare(RemoteTaskRunnerWorkItem o1, RemoteTaskRunnerWorkItem o2)
                       {
                         return o1.getQueueInsertionTime().compareTo(o2.getQueueInsertionTime());
                       }
                     }
    );
  }

  /**
   * Removes a task from the complete queue and clears out the ZK status path of the task.
   *
   * @param taskId - the task to cleanup
   */
  private void cleanup(final String taskId)
  {
    if (!started) {
      return;
    }
    final RemoteTaskRunnerWorkItem removed = completeTasks.remove(taskId);
    final Worker worker = removed.getWorker();
    if (removed == null || worker == null) {
      log.makeAlert("WTF?! Asked to cleanup nonexistent task")
         .addData("taskId", taskId)
         .emit();
    } else {
      final String workerId = worker.getHost();
      log.info("Cleaning up task[%s] on worker[%s]", taskId, workerId);
      final String statusPath = JOINER.join(indexerZkConfig.getStatusPath(), workerId, taskId);
      try {
        cf.delete().guaranteed().forPath(statusPath);
      }
      catch (KeeperException.NoNodeException e) {
        log.info("Tried to delete status path[%s] that didn't exist! Must've gone away already?", statusPath);
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  /**
   * Ensures no workers are already running a task before assigning the task to a worker.
   * It is possible that a worker is running a task that the RTR has no knowledge of. This occurs when the RTR
   * needs to bootstrap after a restart.
   *
   * @param taskRunnerWorkItem - the task to assign
   *
   * @return true iff the task is now assigned
   */
  private boolean tryAssignTask(final Task task, final RemoteTaskRunnerWorkItem taskRunnerWorkItem) throws Exception
  {
    Preconditions.checkNotNull(task, "task");
    Preconditions.checkNotNull(taskRunnerWorkItem, "taskRunnerWorkItem");
    Preconditions.checkArgument(task.getId().equals(taskRunnerWorkItem.getTaskId()), "task id != workItem id");

    if (runningTasks.containsKey(task.getId()) || findWorkerRunningTask(task.getId()) != null) {
      log.info("Task[%s] already running.", task.getId());
      return true;
    } else {
      // Nothing running this task, announce it in ZK for a worker to run it
      WorkerBehaviorConfig workerConfig = workerConfigRef.get();
      WorkerSelectStrategy strategy;
      if (workerConfig == null || workerConfig.getSelectStrategy() == null) {
        log.warn("No worker selections strategy set. Using default.");
        strategy = WorkerBehaviorConfig.DEFAULT_STRATEGY;
      } else {
        strategy = workerConfig.getSelectStrategy();
      }

      ZkWorker assignedWorker = null;
      Optional<ImmutableWorkerInfo> immutableZkWorker = null;
      try {
        synchronized (workersWithUnacknowledgedTask) {
          immutableZkWorker = strategy.findWorkerForTask(
              config,
              ImmutableMap.copyOf(
                  Maps.transformEntries(
                      Maps.filterEntries(
                          zkWorkers, new Predicate<Map.Entry<String, ZkWorker>>()
                          {
                            @Override
                            public boolean apply(Map.Entry<String, ZkWorker> input)
                            {
                              return !lazyWorkers.containsKey(input.getKey()) &&
                                     !workersWithUnacknowledgedTask.containsKey(input.getKey());
                            }
                          }
                      ),
                      new Maps.EntryTransformer<String, ZkWorker, ImmutableWorkerInfo>()
                      {
                        @Override
                        public ImmutableWorkerInfo transformEntry(
                            String key, ZkWorker value
                        )
                        {
                          return value.toImmutable();
                        }
                      }
                  )
              ),
              task
          );

          if (immutableZkWorker.isPresent() &&
              workersWithUnacknowledgedTask.putIfAbsent(immutableZkWorker.get().getWorker().getHost(), task.getId())
                == null) {
            assignedWorker = zkWorkers.get(immutableZkWorker.get().getWorker().getHost());
          }
        }

        if (assignedWorker != null) {
          return announceTask(task, assignedWorker, taskRunnerWorkItem);
        } else {
          log.debug(
              "Unsuccessful task-assign attempt for task [%s] on workers [%s]. Workers to ack tasks are [%s].",
              task.getId(),
              zkWorkers.values(),
              workersWithUnacknowledgedTask
          );
        }

        return false;
      }
      finally {
        if (assignedWorker != null) {
          workersWithUnacknowledgedTask.remove(assignedWorker.getWorker().getHost());
          //if this attempt won the race to run the task then other task might be able to use this worker now after task ack.
          runPendingTasks();
        }
      }
    }
  }

  /**
   * Creates a ZK entry under a specific path associated with a worker. The worker is responsible for
   * removing the task ZK entry and creating a task status ZK entry.
   *
   * @param theZkWorker        The worker the task is assigned to
   * @param taskRunnerWorkItem The task to be assigned
   *
   * @return boolean indicating whether the task was successfully assigned or not
   */
  private boolean announceTask(
      final Task task,
      final ZkWorker theZkWorker,
      final RemoteTaskRunnerWorkItem taskRunnerWorkItem
  ) throws Exception
  {
    Preconditions.checkArgument(task.getId().equals(taskRunnerWorkItem.getTaskId()), "task id != workItem id");
    final String worker = theZkWorker.getWorker().getHost();
    synchronized (statusLock) {
      if (!zkWorkers.containsKey(worker) || lazyWorkers.containsKey(worker)) {
        // the worker might got killed or has been marked as lazy.
        log.info("Not assigning task to already removed worker[%s]", worker);
        return false;
      }
      log.info("Coordinator asking Worker[%s] to add task[%s]", worker, task.getId());

      CuratorUtils.createIfNotExists(
          cf,
          JOINER.join(indexerZkConfig.getTasksPath(), worker, task.getId()),
          CreateMode.EPHEMERAL,
          jsonMapper.writeValueAsBytes(task),
          config.getMaxZnodeBytes()
      );

      RemoteTaskRunnerWorkItem workItem = pendingTasks.remove(task.getId());
      if (workItem == null) {
        log.makeAlert("WTF?! Got a null work item from pending tasks?! How can this be?!")
           .addData("taskId", task.getId())
           .emit();
        return false;
      }

      RemoteTaskRunnerWorkItem newWorkItem = workItem.withWorker(theZkWorker.getWorker(), null);
      runningTasks.put(task.getId(), newWorkItem);
      log.info("Task %s switched from pending to running (on [%s])", task.getId(), newWorkItem.getWorker().getHost());
      TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), TaskStatus.running(task.getId()));

      // Syncing state with Zookeeper - don't assign new tasks until the task we just assigned is actually running
      // on a worker - this avoids overflowing a worker with tasks
      Stopwatch timeoutStopwatch = Stopwatch.createStarted();
      while (!isWorkerRunningTask(theZkWorker.getWorker(), task.getId())) {
        final long waitMs = config.getTaskAssignmentTimeout().toStandardDuration().getMillis();
        statusLock.wait(waitMs);
        long elapsed = timeoutStopwatch.elapsed(TimeUnit.MILLISECONDS);
        if (elapsed >= waitMs) {
          log.makeAlert(
              "Task assignment timed out on worker [%s], never ran task [%s]! Timeout: (%s >= %s)!",
              worker,
              task.getId(),
              elapsed,
              config.getTaskAssignmentTimeout()
          );
          taskComplete(taskRunnerWorkItem, theZkWorker, TaskStatus.failure(task.getId()));
          break;
        }
      }
      return true;
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

  /**
   * When a new worker appears, listeners are registered for status changes associated with tasks assigned to
   * the worker. Status changes indicate the creation or completion of a task.
   * The RemoteTaskRunner updates state according to these changes.
   *
   * @param worker contains metadata for a worker that has appeared in ZK
   *
   * @return future that will contain a fully initialized worker
   */
  private ListenableFuture<ZkWorker> addWorker(final Worker worker)
  {
    log.info("Worker[%s] reportin' for duty!", worker.getHost());

    try {
      cancelWorkerCleanup(worker.getHost());

      final String workerStatusPath = JOINER.join(indexerZkConfig.getStatusPath(), worker.getHost());
      final PathChildrenCache statusCache = pathChildrenCacheFactory.make(cf, workerStatusPath);
      final SettableFuture<ZkWorker> retVal = SettableFuture.create();
      final ZkWorker zkWorker = new ZkWorker(
          worker,
          statusCache,
          jsonMapper
      );

      // Add status listener to the watcher for status changes
      zkWorker.addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
              final String taskId;
              final RemoteTaskRunnerWorkItem taskRunnerWorkItem;
              synchronized (statusLock) {
                try {
                  switch (event.getType()) {
                    case CHILD_ADDED:
                    case CHILD_UPDATED:
                      taskId = ZKPaths.getNodeFromPath(event.getData().getPath());
                      final TaskAnnouncement announcement = jsonMapper.readValue(
                          event.getData().getData(), TaskAnnouncement.class
                      );

                      log.info(
                          "Worker[%s] wrote %s status for task [%s] on [%s]",
                          zkWorker.getWorker().getHost(),
                          announcement.getTaskStatus().getStatusCode(),
                          taskId,
                          announcement.getTaskLocation()
                      );

                      // Synchronizing state with ZK
                      statusLock.notifyAll();

                      final RemoteTaskRunnerWorkItem tmp;
                      if ((tmp = runningTasks.get(taskId)) != null) {
                        taskRunnerWorkItem = tmp;
                      } else {
                        final RemoteTaskRunnerWorkItem newTaskRunnerWorkItem = new RemoteTaskRunnerWorkItem(
                            taskId,
                            zkWorker.getWorker(),
                            TaskLocation.unknown()
                        );
                        final RemoteTaskRunnerWorkItem existingItem = runningTasks.putIfAbsent(
                            taskId,
                            newTaskRunnerWorkItem
                        );
                        if (existingItem == null) {
                          log.warn(
                              "Worker[%s] announced a status for a task I didn't know about, adding to runningTasks: %s",
                              zkWorker.getWorker().getHost(),
                              taskId
                          );
                          taskRunnerWorkItem = newTaskRunnerWorkItem;
                        } else {
                          taskRunnerWorkItem = existingItem;
                        }
                      }

                      if (!announcement.getTaskLocation().equals(taskRunnerWorkItem.getLocation())) {
                        taskRunnerWorkItem.setLocation(announcement.getTaskLocation());
                        TaskRunnerUtils.notifyLocationChanged(listeners, taskId, announcement.getTaskLocation());
                      }

                      if (announcement.getTaskStatus().isComplete()) {
                        taskComplete(taskRunnerWorkItem, zkWorker, announcement.getTaskStatus());
                        runPendingTasks();
                      }
                      break;
                    case CHILD_REMOVED:
                      taskId = ZKPaths.getNodeFromPath(event.getData().getPath());
                      taskRunnerWorkItem = runningTasks.remove(taskId);
                      if (taskRunnerWorkItem != null) {
                        log.info("Task[%s] just disappeared!", taskId);
                        taskRunnerWorkItem.setResult(TaskStatus.failure(taskId));
                        TaskRunnerUtils.notifyStatusChanged(listeners, taskId, TaskStatus.failure(taskId));
                      } else {
                        log.info("Task[%s] went bye bye.", taskId);
                      }
                      break;
                    case INITIALIZED:
                      if (zkWorkers.putIfAbsent(worker.getHost(), zkWorker) == null) {
                        retVal.set(zkWorker);
                      } else {
                        final String message = String.format(
                            "WTF?! Tried to add already-existing worker[%s]",
                            worker.getHost()
                        );
                        log.makeAlert(message)
                           .addData("workerHost", worker.getHost())
                           .addData("workerIp", worker.getIp())
                           .emit();
                        retVal.setException(new IllegalStateException(message));
                      }
                      runPendingTasks();
                  }
                }
                catch (Exception e) {
                  log.makeAlert(e, "Failed to handle new worker status")
                     .addData("worker", zkWorker.getWorker().getHost())
                     .addData("znode", event.getData().getPath())
                     .emit();
                }
              }
            }
          }
      );
      zkWorker.start();
      return retVal;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * We allow workers to change their own capacities and versions. They cannot change their own hosts or ips without
   * dropping themselves and re-announcing.
   */
  private void updateWorker(final Worker worker)
  {
    final ZkWorker zkWorker = zkWorkers.get(worker.getHost());
    if (zkWorker != null) {
      log.info("Worker[%s] updated its announcement from[%s] to[%s].", worker.getHost(), zkWorker.getWorker(), worker);
      zkWorker.setWorker(worker);
    } else {
      log.warn(
          "WTF, worker[%s] updated its announcement but we didn't have a ZkWorker for it. Ignoring.",
          worker.getHost()
      );
    }
  }

  /**
   * When a ephemeral worker node disappears from ZK, incomplete running tasks will be retried by
   * the logic in the status listener. We still have to make sure there are no tasks assigned
   * to the worker but not yet running.
   *
   * @param worker - the removed worker
   */
  private void removeWorker(final Worker worker)
  {
    log.info("Kaboom! Worker[%s] removed!", worker.getHost());

    final ZkWorker zkWorker = zkWorkers.get(worker.getHost());
    if (zkWorker != null) {
      try {
        scheduleTasksCleanupForWorker(worker.getHost(), getAssignedTasks(worker));
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      finally {
        try {
          zkWorker.close();
        }
        catch (Exception e) {
          log.error(e, "Exception closing worker[%s]!", worker.getHost());
        }
        zkWorkers.remove(worker.getHost());
      }
    }
    lazyWorkers.remove(worker.getHost());
  }

  /**
   * Schedule a task that will, at some point in the future, clean up znodes and issue failures for "tasksToFail"
   * if they are being run by "worker".
   */
  private void scheduleTasksCleanupForWorker(final String worker, final List<String> tasksToFail)
  {
    // This method is only called from the PathChildrenCache event handler, so this may look like a race,
    // but is actually not.
    cancelWorkerCleanup(worker);

    final ListenableScheduledFuture<?> cleanupTask = cleanupExec.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            log.info("Running scheduled cleanup for Worker[%s]", worker);
            try {
              for (String assignedTask : tasksToFail) {
                String taskPath = JOINER.join(indexerZkConfig.getTasksPath(), worker, assignedTask);
                String statusPath = JOINER.join(indexerZkConfig.getStatusPath(), worker, assignedTask);
                if (cf.checkExists().forPath(taskPath) != null) {
                  cf.delete().guaranteed().forPath(taskPath);
                }

                if (cf.checkExists().forPath(statusPath) != null) {
                  cf.delete().guaranteed().forPath(statusPath);
                }

                log.info("Failing task[%s]", assignedTask);
                RemoteTaskRunnerWorkItem taskRunnerWorkItem = runningTasks.remove(assignedTask);
                if (taskRunnerWorkItem != null) {
                  taskRunnerWorkItem.setResult(TaskStatus.failure(assignedTask));
                  TaskRunnerUtils.notifyStatusChanged(listeners, assignedTask, TaskStatus.failure(assignedTask));
                } else {
                  log.warn("RemoteTaskRunner has no knowledge of task[%s]", assignedTask);
                }
              }

              // worker is gone, remove worker task status announcements path.
              String workerStatusPath = JOINER.join(indexerZkConfig.getStatusPath(), worker);
              if (cf.checkExists().forPath(workerStatusPath) != null) {
                cf.delete().guaranteed().forPath(JOINER.join(indexerZkConfig.getStatusPath(), worker));
              }
            }
            catch (Exception e) {
              log.makeAlert("Exception while cleaning up worker[%s]", worker).emit();
              throw Throwables.propagate(e);
            }
          }
        },
        config.getTaskCleanupTimeout().toStandardDuration().getMillis(),
        TimeUnit.MILLISECONDS
    );

    removedWorkerCleanups.put(worker, cleanupTask);

    // Remove this entry from removedWorkerCleanups when done, if it's actually the one in there.
    Futures.addCallback(
        cleanupTask,
        new FutureCallback<Object>()
        {
          @Override
          public void onSuccess(Object result)
          {
            removedWorkerCleanups.remove(worker, cleanupTask);
          }

          @Override
          public void onFailure(Throwable t)
          {
            removedWorkerCleanups.remove(worker, cleanupTask);
          }
        }
    );
  }

  private void taskComplete(
      RemoteTaskRunnerWorkItem taskRunnerWorkItem,
      ZkWorker zkWorker,
      TaskStatus taskStatus
  )
  {
    Preconditions.checkNotNull(taskRunnerWorkItem, "taskRunnerWorkItem");
    Preconditions.checkNotNull(taskStatus, "taskStatus");
    if (zkWorker != null) {
      log.info(
          "Worker[%s] completed task[%s] with status[%s]",
          zkWorker.getWorker().getHost(),
          taskStatus.getId(),
          taskStatus.getStatusCode()
      );
      // Worker is done with this task
      zkWorker.setLastCompletedTaskTime(new DateTime());
    } else {
      log.info("Workerless task[%s] completed with status[%s]", taskStatus.getId(), taskStatus.getStatusCode());
    }

    // Move from running -> complete
    completeTasks.put(taskStatus.getId(), taskRunnerWorkItem);
    runningTasks.remove(taskStatus.getId());

    // Notify interested parties
    taskRunnerWorkItem.setResult(taskStatus);
    TaskRunnerUtils.notifyStatusChanged(listeners, taskStatus.getId(), taskStatus);
  }

  @Override
  public Collection<Worker> markWorkersLazy(Predicate<ImmutableWorkerInfo> isLazyWorker, int maxWorkers)
  {
    // status lock is used to prevent any tasks being assigned to the worker while we mark it lazy
    synchronized (statusLock) {
      Iterator<String> iterator = zkWorkers.keySet().iterator();
      while (iterator.hasNext()) {
        String worker = iterator.next();
        ZkWorker zkWorker = zkWorkers.get(worker);
        try {
          if (getAssignedTasks(zkWorker.getWorker()).isEmpty() && isLazyWorker.apply(zkWorker.toImmutable())) {
            log.info("Adding Worker[%s] to lazySet!", zkWorker.getWorker().getHost());
            lazyWorkers.put(worker, zkWorker);
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
      return ImmutableList.copyOf(getWorkerFromZK(lazyWorkers.values()));
    }
  }

  protected List<String> getAssignedTasks(Worker worker) throws Exception
  {
    final List<String> assignedTasks = Lists.newArrayList(
        cf.getChildren().forPath(JOINER.join(indexerZkConfig.getTasksPath(), worker.getHost()))
    );

    for (Map.Entry<String, RemoteTaskRunnerWorkItem> entry : runningTasks.entrySet()) {
      if (entry.getValue() == null) {
        log.error(
            "Huh? null work item for [%s]", entry.getKey()
        );
      } else if (entry.getValue().getWorker() == null) {
        log.error("Huh? no worker for [%s]", entry.getKey());
      } else if (entry.getValue().getWorker().getHost().equalsIgnoreCase(worker.getHost())) {
        log.info("[%s]: Found [%s] running", worker.getHost(), entry.getKey());
        assignedTasks.add(entry.getKey());
      }
    }
    log.info("[%s]: Found %d tasks assigned", worker.getHost(), assignedTasks.size());
    return assignedTasks;
  }

  @Override
  public Collection<Worker> getLazyWorkers()
  {
    return ImmutableList.copyOf(getWorkerFromZK(lazyWorkers.values()));
  }

  private static ImmutableList<ImmutableWorkerInfo> getImmutableWorkerFromZK(Collection<ZkWorker> workers)
  {
    return ImmutableList.copyOf(
        Collections2.transform(
            workers,
            new Function<ZkWorker, ImmutableWorkerInfo>()
            {
              @Override
              public ImmutableWorkerInfo apply(ZkWorker input)
              {
                return input.toImmutable();
              }
            }
        )
    );
  }

  public static Collection<Worker> getWorkerFromZK(Collection<ZkWorker> workers)
  {
    return Collections2.transform(
        workers,
        new Function<ZkWorker, Worker>()
        {
          @Override
          public Worker apply(ZkWorker input)
          {
            return input.getWorker();
          }
        }
    );
  }

  @VisibleForTesting
  ConcurrentMap<String, ScheduledFuture> getRemovedWorkerCleanups()
  {
    return removedWorkerCleanups;
  }

  @VisibleForTesting
  RemoteTaskRunnerConfig getRemoteTaskRunnerConfig()
  {
    return config;
  }

  @VisibleForTesting
  Map<String, String> getWorkersWithUnacknowledgedTask()
  {
    return workersWithUnacknowledgedTask;
  }
}
