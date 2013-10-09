/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.InputStreamResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.curator.cache.PathChildrenCacheFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerSetupData;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.tasklogs.TaskLogStreamer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The RemoteTaskRunner's primary responsibility is to assign tasks to worker nodes.
 * The RemoteTaskRunner uses Zookeeper to keep track of which workers are running which tasks. Tasks are assigned by
 * creating ephemeral nodes in ZK that workers must remove. Workers announce the statuses of the tasks they are running.
 * Once a task completes, it is up to the RTR to remove the task status and run any necessary cleanup.
 * The RemoteTaskRunner is event driven and updates state according to ephemeral node changes in ZK.
 * <p/>
 * The RemoteTaskRunner will assign tasks to a node until the node hits capacity. At that point, task assignment will
 * fail. The RemoteTaskRunner depends on another component to create additional worker resources.
 * For example, {@link io.druid.indexing.overlord.scaling.ResourceManagementScheduler} can take care of these duties.
 * <p/>
 * If a worker node becomes inexplicably disconnected from Zk, the RemoteTaskRunner will fail any tasks associated with the worker.
 * <p/>
 * The RemoteTaskRunner uses ZK for job management and assignment and http for IPC messages.
 */
public class RemoteTaskRunner implements TaskRunner, TaskLogStreamer
{
  private static final EmittingLogger log = new EmittingLogger(RemoteTaskRunner.class);
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);
  private static final Joiner JOINER = Joiner.on("/");

  private final ObjectMapper jsonMapper;
  private final RemoteTaskRunnerConfig config;
  private final ZkPathsConfig zkPaths;
  private final CuratorFramework cf;
  private final PathChildrenCacheFactory pathChildrenCacheFactory;
  private final PathChildrenCache workerPathCache;
  private final Supplier<WorkerSetupData> workerSetupData;
  private final HttpClient httpClient;

  // all workers that exist in ZK
  private final Map<String, ZkWorker> zkWorkers = new ConcurrentHashMap<String, ZkWorker>();
  // all tasks that have been assigned to a worker
  private final RemoteTaskRunnerWorkQueue runningTasks = new RemoteTaskRunnerWorkQueue();
  // tasks that have not yet run
  private final RemoteTaskRunnerWorkQueue pendingTasks = new RemoteTaskRunnerWorkQueue();

  private final ExecutorService runPendingTasksExec = Executors.newSingleThreadExecutor();

  private final Object statusLock = new Object();

  private volatile boolean started = false;

  public RemoteTaskRunner(
      ObjectMapper jsonMapper,
      RemoteTaskRunnerConfig config,
      ZkPathsConfig zkPaths,
      CuratorFramework cf,
      PathChildrenCacheFactory pathChildrenCacheFactory,
      Supplier<WorkerSetupData> workerSetupData,
      HttpClient httpClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.zkPaths = zkPaths;
    this.cf = cf;
    this.pathChildrenCacheFactory = pathChildrenCacheFactory;
    this.workerPathCache = pathChildrenCacheFactory.make(cf, zkPaths.getIndexerAnnouncementPath());
    this.workerSetupData = workerSetupData;
    this.httpClient = httpClient;
  }

  @LifecycleStart
  public void start()
  {
    try {
      if (started) {
        return;
      }

      // Add listener for creation/deletion of workers
      workerPathCache.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, final PathChildrenCacheEvent event) throws Exception
            {
              Worker worker;
              switch (event.getType()) {
                case CHILD_ADDED:
                  worker = jsonMapper.readValue(
                      event.getData().getData(),
                      Worker.class
                  );
                  addWorker(worker, PathChildrenCache.StartMode.NORMAL);
                  break;
                case CHILD_REMOVED:
                  worker = jsonMapper.readValue(
                      event.getData().getData(),
                      Worker.class
                  );
                  removeWorker(worker);
                  break;
                default:
                  break;
              }
            }
          }
      );
      workerPathCache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);

      for (ChildData childData : workerPathCache.getCurrentData()) {
        final Worker worker = jsonMapper.readValue(
            childData.getData(),
            Worker.class
        );
        addWorker(worker, PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
      }

      started = true;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @LifecycleStop
  public void stop()
  {
    try {
      if (!started) {
        return;
      }

      for (ZkWorker zkWorker : zkWorkers.values()) {
        zkWorker.close();
      }
      workerPathCache.close();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      started = false;
    }
  }

  @Override
  public Collection<ZkWorker> getWorkers()
  {
    return zkWorkers.values();
  }

  @Override
  public Collection<RemoteTaskRunnerWorkItem> getRunningTasks()
  {
    return runningTasks.values();
  }

  @Override
  public Collection<RemoteTaskRunnerWorkItem> getPendingTasks()
  {
    return pendingTasks.values();
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

  public boolean isWorkerRunningTask(Worker worker, Task task)
  {
    ZkWorker zkWorker = zkWorkers.get(worker.getHost());

    return (zkWorker != null && zkWorker.isRunningTask(task.getId()));
  }

  @Override
  public void bootstrap(List<Task> tasks)
  {
    try {
      if (!started) {
        throw new ISE("Must start RTR first before calling bootstrap!");
      }

      Map<String, Worker> existingTasks = Maps.newHashMap();
      for (ZkWorker zkWorker : zkWorkers.values()) {
        for (String runningTask : zkWorker.getRunningTasks().keySet()) {
          existingTasks.put(runningTask, zkWorker.getWorker());
        }
      }

      for (Task task : tasks) {
        Worker worker = existingTasks.get(task.getId());
        if (worker != null) {
          log.info("Bootstrap found [%s] running on [%s].", task.getId(), worker.getHost());
          runningTasks.put(
              task.getId(),
              new RemoteTaskRunnerWorkItem(
                  task,
                  SettableFuture.<TaskStatus>create(),
                  worker
              )
          );
        }
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * A task will be run only if there is no current knowledge in the RemoteTaskRunner of the task.
   *
   * @param task task to run
   */
  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    RemoteTaskRunnerWorkItem runningTask = runningTasks.get(task.getId());
    if (runningTask != null) {
      ZkWorker zkWorker = findWorkerRunningTask(task.getId());
      if (zkWorker == null) {
        log.warn("Told to run task[%s], but no worker has started running it yet.", task.getId());
      } else {
        log.info("Task[%s] already running on %s.", task.getId(), zkWorker.getWorker().getHost());
        TaskAnnouncement announcement = zkWorker.getRunningTasks().get(task.getId());
        if (announcement.getTaskStatus().isComplete()) {
          taskComplete(runningTask, zkWorker, task.getId(), announcement.getTaskStatus());
        }
      }

      return runningTask.getResult();
    }

    RemoteTaskRunnerWorkItem pendingTask = pendingTasks.get(task.getId());
    if (pendingTask != null) {
      log.info("Assigned a task[%s] that is already pending, not doing anything", task.getId());
      return pendingTask.getResult();
    }

    RemoteTaskRunnerWorkItem taskRunnerWorkItem = new RemoteTaskRunnerWorkItem(
        task,
        SettableFuture.<TaskStatus>create(),
        null
    );
    addPendingTask(taskRunnerWorkItem);
    return taskRunnerWorkItem.getResult();
  }

  /**
   * Finds the worker running the task and forwards the shutdown signal to the worker.
   *
   * @param taskId - task id to shutdown
   */
  @Override
  public void shutdown(String taskId)
  {
    if (pendingTasks.containsKey(taskId)) {
      pendingTasks.remove(taskId);
      return;
    }

    final ZkWorker zkWorker = findWorkerRunningTask(taskId);

    if (zkWorker == null) {
      log.info("Can't shutdown! No worker running task %s", taskId);
      return;
    }

    try {
      final URL url = makeWorkerURL(zkWorker.getWorker(), String.format("/task/%s/shutdown", taskId));
      final StatusResponseHolder response = httpClient.post(url)
                                                      .go(RESPONSE_HANDLER)
                                                      .get();

      log.info(
          "Sent shutdown message to worker: %s, status %s, response: %s",
          zkWorker.getWorker().getHost(),
          response.getStatus(),
          response.getContent()
      );

      if (!response.getStatus().equals(HttpResponseStatus.ACCEPTED)) {
        log.error("Shutdown failed for %s! Are you sure the task was running?", taskId);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Optional<InputSupplier<InputStream>> streamTaskLog(final String taskId, final long offset)
  {
    final ZkWorker zkWorker = findWorkerRunningTask(taskId);

    if (zkWorker == null) {
      // Worker is not running this task, it might be available in deep storage
      return Optional.absent();
    } else {
      // Worker is still running this task
      final URL url = makeWorkerURL(zkWorker.getWorker(), String.format("/task/%s/log?offset=%d", taskId, offset));
      return Optional.<InputSupplier<InputStream>>of(
          new InputSupplier<InputStream>()
          {
            @Override
            public InputStream getInput() throws IOException
            {
              try {
                return httpClient.get(url)
                                 .go(new InputStreamResponseHandler())
                                 .get();
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
  private void addPendingTask(final RemoteTaskRunnerWorkItem taskRunnerWorkItem)
  {
    log.info("Added pending task %s", taskRunnerWorkItem.getTask().getId());

    pendingTasks.put(taskRunnerWorkItem.getTask().getId(), taskRunnerWorkItem);
    runPendingTasks();
  }

  /**
   * This method uses a single threaded executor to extract all pending tasks and attempt to run them. Any tasks that
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
              // make a copy of the pending tasks because assignTask may delete tasks from pending and move them
              // into running status
              List<RemoteTaskRunnerWorkItem> copy = Lists.newArrayList(pendingTasks.values());
              for (RemoteTaskRunnerWorkItem taskWrapper : copy) {
                assignTask(taskWrapper);
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

  /**
   * Removes a task from the running queue and clears out the ZK status path of the task.
   *
   * @param workerId - the worker that was previously running the task
   * @param taskId   - the task to cleanup
   */
  private void cleanup(final String workerId, final String taskId)
  {
    log.info("Cleaning up [%s]", taskId);
    runningTasks.remove(taskId);
    final String statusPath = JOINER.join(zkPaths.getIndexerStatusPath(), workerId, taskId);
    try {
      cf.delete().guaranteed().forPath(statusPath);
    }
    catch (Exception e) {
      log.info("Tried to delete status path[%s] that didn't exist! Must've gone away already?", statusPath);
    }
  }

  /**
   * Ensures no workers are already running a task before assigning the task to a worker.
   * It is possible that a worker is running a task that the RTR has no knowledge of. This occurs when the RTR
   * needs to bootstrap after a restart.
   *
   * @param taskRunnerWorkItem - the task to assign
   */
  private void assignTask(RemoteTaskRunnerWorkItem taskRunnerWorkItem)
  {
    try {
      final String taskId = taskRunnerWorkItem.getTask().getId();

      if (runningTasks.containsKey(taskId) || findWorkerRunningTask(taskId) != null) {
        log.info("Task[%s] already running.", taskId);
      } else {
        // Nothing running this task, announce it in ZK for a worker to run it
        ZkWorker zkWorker = findWorkerForTask(taskRunnerWorkItem.getTask());
        if (zkWorker != null) {
          announceTask(zkWorker.getWorker(), taskRunnerWorkItem);
        }
      }
    }
    catch (Exception e) {
      log.makeAlert("Exception while trying to run task")
         .addData("taskId", taskRunnerWorkItem.getTask().getId())
         .emit();
    }
  }

  /**
   * Creates a ZK entry under a specific path associated with a worker. The worker is responsible for
   * removing the task ZK entry and creating a task status ZK entry.
   *
   * @param theWorker          The worker the task is assigned to
   * @param taskRunnerWorkItem The task to be assigned
   */
  private void announceTask(Worker theWorker, RemoteTaskRunnerWorkItem taskRunnerWorkItem) throws Exception
  {
    final Task task = taskRunnerWorkItem.getTask();

    log.info("Coordinator asking Worker[%s] to add task[%s]", theWorker.getHost(), task.getId());

    byte[] rawBytes = jsonMapper.writeValueAsBytes(task);
    if (rawBytes.length > config.getMaxZnodeBytes()) {
      throw new ISE("Length of raw bytes for task too large[%,d > %,d]", rawBytes.length, config.getMaxZnodeBytes());
    }

    String taskPath = JOINER.join(zkPaths.getIndexerTaskPath(), theWorker.getHost(), task.getId());

    if (cf.checkExists().forPath(taskPath) == null) {
      cf.create()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(
            taskPath, rawBytes
        );
    }

    RemoteTaskRunnerWorkItem workItem = pendingTasks.remove(task.getId());
    if (workItem == null) {
      log.makeAlert("WTF?! Got a null work item from pending tasks?! How can this be?!")
         .addData("taskId", task.getId())
         .emit();
      return;
    }

    RemoteTaskRunnerWorkItem newWorkItem = workItem.withWorker(theWorker);
    runningTasks.put(task.getId(), newWorkItem);
    log.info("Task %s switched from pending to running (on [%s])", task.getId(), newWorkItem.getWorker().getHost());

    // Syncing state with Zookeeper - don't assign new tasks until the task we just assigned is actually running
    // on a worker - this avoids overflowing a worker with tasks
    Stopwatch timeoutStopwatch = new Stopwatch();
    timeoutStopwatch.start();
    synchronized (statusLock) {
      while (!isWorkerRunningTask(theWorker, task)) {
        final long waitMs = config.getTaskAssignmentTimeout().toStandardDuration().getMillis();
        statusLock.wait(waitMs);
        long elapsed = timeoutStopwatch.elapsed(TimeUnit.MILLISECONDS);
        if (elapsed >= waitMs) {
          log.error(
              "Something went wrong! [%s] never ran task [%s]! Timeout: (%s >= %s)!",
              theWorker.getHost(),
              task.getId(),
              elapsed,
              config.getTaskAssignmentTimeout()
          );

          taskRunnerWorkItem.setResult(TaskStatus.failure(taskRunnerWorkItem.getTask().getId()));
          break;
        }
      }
    }
  }

  /**
   * When a new worker appears, listeners are registered for status changes associated with tasks assigned to
   * the worker. Status changes indicate the creation or completion of a task.
   * The RemoteTaskRunner updates state according to these changes.
   *
   * @param worker - contains metadata for a worker that has appeared in ZK
   */
  private ZkWorker addWorker(final Worker worker, PathChildrenCache.StartMode startMode)
  {
    log.info("Worker[%s] reportin' for duty!", worker.getHost());

    try {
      final String workerStatusPath = JOINER.join(zkPaths.getIndexerStatusPath(), worker.getHost());
      final PathChildrenCache statusCache = pathChildrenCacheFactory.make(cf, workerStatusPath);
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
              String taskId;
              RemoteTaskRunnerWorkItem taskRunnerWorkItem;
              synchronized (statusLock) {
                try {
                  switch (event.getType()) {
                    case CHILD_ADDED:
                    case CHILD_UPDATED:
                      taskId = ZKPaths.getNodeFromPath(event.getData().getPath());
                      final TaskStatus taskStatus = jsonMapper.readValue(
                          event.getData().getData(), TaskStatus.class
                      );

                      log.info(
                          "Worker[%s] wrote %s status for task: %s",
                          zkWorker.getWorker().getHost(),
                          taskStatus.getStatusCode(),
                          taskId
                      );

                      // Synchronizing state with ZK
                      statusLock.notify();

                      taskRunnerWorkItem = runningTasks.get(taskId);
                      if (taskRunnerWorkItem == null) {
                        log.warn(
                            "WTF?! Worker[%s] announcing a status for a task I didn't know about: %s",
                            zkWorker.getWorker().getHost(),
                            taskId
                        );
                      }

                      if (taskStatus.isComplete()) {
                        taskComplete(taskRunnerWorkItem, zkWorker, taskId, taskStatus);
                        runPendingTasks();
                      }
                      break;
                    case CHILD_REMOVED:
                      taskId = ZKPaths.getNodeFromPath(event.getData().getPath());
                      taskRunnerWorkItem = runningTasks.remove(taskId);
                      if (taskRunnerWorkItem != null) {
                        log.info("Task[%s] just disappeared!", taskId);
                        taskRunnerWorkItem.setResult(TaskStatus.failure(taskRunnerWorkItem.getTask().getId()));
                      } else {
                        log.info("Task[%s] went bye bye.", taskId);
                      }
                      break;
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

      zkWorker.start(startMode);
      zkWorkers.put(worker.getHost(), zkWorker);

      runPendingTasks();

      return zkWorker;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
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
        List<String> tasksToFail = Lists.newArrayList(
            cf.getChildren().forPath(JOINER.join(zkPaths.getIndexerTaskPath(), worker.getHost()))
        );
        log.info("[%s]: Found %d tasks assigned", worker.getHost(), tasksToFail.size());

        for (Map.Entry<String, RemoteTaskRunnerWorkItem> entry : runningTasks.entrySet()) {
          if (entry.getValue() == null) {
            log.error("Huh? null work item for [%s]", entry.getKey());
          } else if (entry.getValue().getWorker() == null) {
            log.error("Huh? no worker for [%s]", entry.getKey());
          } else if (entry.getValue().getWorker().getHost().equalsIgnoreCase(worker.getHost())) {
            log.info("[%s]: Found [%s] running", worker.getHost(), entry.getKey());
            tasksToFail.add(entry.getKey());
          }
        }

        for (String assignedTask : tasksToFail) {
          RemoteTaskRunnerWorkItem taskRunnerWorkItem = runningTasks.remove(assignedTask);
          if (taskRunnerWorkItem != null) {
            String taskPath = JOINER.join(zkPaths.getIndexerTaskPath(), worker.getHost(), assignedTask);
            if (cf.checkExists().forPath(taskPath) != null) {
              cf.delete().guaranteed().forPath(taskPath);
            }

            log.info("Failing task[%s]", assignedTask);
            taskRunnerWorkItem.setResult(TaskStatus.failure(taskRunnerWorkItem.getTask().getId()));
          } else {
            log.warn("RemoteTaskRunner has no knowledge of task[%s]", assignedTask);
          }
        }
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
  }

  private ZkWorker findWorkerForTask(final Task task)
  {
    TreeSet<ZkWorker> sortedWorkers = Sets.newTreeSet(
        new Comparator<ZkWorker>()
        {
          @Override
          public int compare(
              ZkWorker zkWorker, ZkWorker zkWorker2
          )
          {
            int retVal = -Ints.compare(zkWorker.getCurrCapacityUsed(), zkWorker2.getCurrCapacityUsed());
            if (retVal == 0) {
              retVal = zkWorker.getWorker().getHost().compareTo(zkWorker2.getWorker().getHost());
            }

            return retVal;
          }
        }
    );
    sortedWorkers.addAll(zkWorkers.values());
    final String configMinWorkerVer = workerSetupData.get().getMinVersion();
    final String minWorkerVer = configMinWorkerVer == null ? config.getWorkerVersion() : configMinWorkerVer;

    for (ZkWorker zkWorker : sortedWorkers) {
      if (zkWorker.canRunTask(task) && zkWorker.isValidVersion(minWorkerVer)) {
        return zkWorker;
      }
    }
    log.debug("Worker nodes %s do not have capacity to run any more tasks!", zkWorkers.values());
    return null;
  }

  private void taskComplete(
      RemoteTaskRunnerWorkItem taskRunnerWorkItem,
      ZkWorker zkWorker,
      String taskId,
      TaskStatus taskStatus
  )
  {
    if (taskRunnerWorkItem != null) {
      final ListenableFuture<TaskStatus> result = taskRunnerWorkItem.getResult();
      if (result != null) {
        ((SettableFuture<TaskStatus>) result).set(taskStatus);
      }
    }

    // Worker is done with this task
    zkWorker.setLastCompletedTaskTime(new DateTime());
    cleanup(zkWorker.getWorker().getHost(), taskId);
  }
}