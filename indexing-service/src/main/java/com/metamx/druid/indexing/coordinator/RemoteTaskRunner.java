/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.common.tasklogs.TaskLogProvider;
import com.metamx.druid.indexing.coordinator.config.RemoteTaskRunnerConfig;
import com.metamx.druid.indexing.coordinator.setup.WorkerSetupData;
import com.metamx.druid.indexing.worker.Worker;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.InputStreamResponseHandler;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import com.metamx.http.client.response.ToStringResponseHandler;
import org.apache.curator.framework.CuratorFramework;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The RemoteTaskRunner's primary responsibility is to assign tasks to worker nodes and manage retries in failure
 * scenarios. The RemoteTaskRunner keeps track of which workers are running which tasks and manages coordinator and
 * worker interactions over Zookeeper. The RemoteTaskRunner is event driven and updates state according to ephemeral
 * node changes in ZK.
 * <p/>
 * The RemoteTaskRunner will assign tasks to a node until the node hits capacity. At that point, task assignment will
 * fail. The RemoteTaskRunner depends on another component to create additional worker resources.
 * For example, {@link com.metamx.druid.indexing.coordinator.scaling.ResourceManagementScheduler} can take care of these duties.
 * <p/>
 * If a worker node becomes inexplicably disconnected from Zk, the RemoteTaskRunner will automatically retry any tasks
 * that were associated with the node.
 * <p/>
 * The RemoteTaskRunner uses ZK for job management and assignment and http for IPC messages.
 */
public class RemoteTaskRunner implements TaskRunner, TaskLogProvider
{
  private static final EmittingLogger log = new EmittingLogger(RemoteTaskRunner.class);
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);
  private static final Joiner JOINER = Joiner.on("/");

  private final ObjectMapper jsonMapper;
  private final RemoteTaskRunnerConfig config;
  private final CuratorFramework cf;
  private final PathChildrenCache workerPathCache;
  private final AtomicReference<WorkerSetupData> workerSetupData;
  private final HttpClient httpClient;

  // all workers that exist in ZK
  private final Map<String, ZkWorker> zkWorkers = new ConcurrentHashMap<String, ZkWorker>();
  // all tasks that have been assigned to a worker
  private final TaskRunnerWorkQueue runningTasks = new TaskRunnerWorkQueue();
  // tasks that have not yet run
  private final TaskRunnerWorkQueue pendingTasks = new TaskRunnerWorkQueue();

  private final ExecutorService runPendingTasksExec = Executors.newSingleThreadExecutor();

  private final Object statusLock = new Object();

  private volatile boolean started = false;

  public RemoteTaskRunner(
      ObjectMapper jsonMapper,
      RemoteTaskRunnerConfig config,
      CuratorFramework cf,
      PathChildrenCache workerPathCache,
      AtomicReference<WorkerSetupData> workerSetupData,
      HttpClient httpClient
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.cf = cf;
    this.workerPathCache = workerPathCache;
    this.workerSetupData = workerSetupData;
    this.httpClient = httpClient;
  }

  @LifecycleStart
  public void start()
  {
    if (started) {
      return;
    }

    started = true;
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
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    return runningTasks.values();
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
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
      Map<String, ZkWorker> existingTasks = Maps.newHashMap();
      Set<String> existingWorkers = Sets.newHashSet(cf.getChildren().forPath(config.getIndexerAnnouncementPath()));
      for (String existingWorker : existingWorkers) {
        Worker worker = jsonMapper.readValue(
            cf.getData()
              .forPath(
                  JOINER.join(
                      config.getIndexerAnnouncementPath(),
                      existingWorker
                  )
              ), Worker.class
        );
        ZkWorker zkWorker = addWorker(worker);
        List<String> runningTasks = cf.getChildren()
                                      .forPath(JOINER.join(config.getIndexerStatusPath(), existingWorker));
        for (String runningTask : runningTasks) {
          existingTasks.put(runningTask, zkWorker);
        }
      }

      // initialize data structures
      for (Task task : tasks) {
        TaskRunnerWorkItem taskRunnerWorkItem = new TaskRunnerWorkItem(
            task,
            SettableFuture.<TaskStatus>create(),
            new DateTime()
        );

        ZkWorker zkWorker = existingTasks.remove(task.getId());
        if (zkWorker != null) {
          runningTasks.put(task.getId(), taskRunnerWorkItem);
          zkWorker.addRunningTask(task);
        } else {
          addPendingTask(taskRunnerWorkItem);
        }
      }

      // shutdown any tasks that we don't know about
      for (String existingTask : existingTasks.keySet()) {
        shutdown(existingTask);
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    init();
  }

  /**
   * A task will be run only if there is no current knowledge in the RemoteTaskRunner of the task.
   *
   * @param task task to run
   */
  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    if (runningTasks.containsKey(task.getId()) || pendingTasks.containsKey(task.getId())) {
      throw new ISE("Assigned a task[%s] that is already running or pending, WTF is happening?!", task.getId());
    }
    TaskRunnerWorkItem taskRunnerWorkItem = new TaskRunnerWorkItem(
        task, SettableFuture.<TaskStatus>create(), new DateTime()
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
        throw new ISE("Shutdown failed");
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
   *
   * @param taskRunnerWorkItem
   */
  private void addPendingTask(final TaskRunnerWorkItem taskRunnerWorkItem)
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
              List<TaskRunnerWorkItem> copy = Lists.newArrayList(pendingTasks.values());
              for (TaskRunnerWorkItem taskWrapper : copy) {
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
    runningTasks.remove(taskId);
    final String statusPath = JOINER.join(config.getIndexerStatusPath(), workerId, taskId);
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
  private void assignTask(TaskRunnerWorkItem taskRunnerWorkItem)
  {
    try {
      final String taskId = taskRunnerWorkItem.getTask().getId();

      if (runningTasks.containsKey(taskId)) {
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
      throw Throwables.propagate(e);
    }
  }

  /**
   * Creates a ZK entry under a specific path associated with a worker. The worker is responsible for
   * removing the task ZK entry and creating a task status ZK entry.
   *
   * @param theWorker          The worker the task is assigned to
   * @param taskRunnerWorkItem The task to be assigned
   */
  private void announceTask(Worker theWorker, TaskRunnerWorkItem taskRunnerWorkItem) throws Exception
  {
    final Task task = taskRunnerWorkItem.getTask();

    log.info("Coordinator asking Worker[%s] to add task[%s]", theWorker.getHost(), task.getId());

    byte[] rawBytes = jsonMapper.writeValueAsBytes(task);
    if (rawBytes.length > config.getMaxNumBytes()) {
      throw new ISE("Length of raw bytes for task too large[%,d > %,d]", rawBytes.length, config.getMaxNumBytes());
    }

    String taskPath = JOINER.join(config.getIndexerTaskPath(), theWorker.getHost(), task.getId());

    if (cf.checkExists().forPath(taskPath) == null) {
      cf.create()
        .withMode(CreateMode.EPHEMERAL)
        .forPath(
            taskPath, rawBytes
        );
    }

    runningTasks.put(task.getId(), pendingTasks.remove(task.getId()));
    log.info("Task %s switched from pending to running", task.getId());

    // Syncing state with Zookeeper - don't assign new tasks until the task we just assigned is actually running
    // on a worker - this avoids overflowing a worker with tasks
    Stopwatch timeoutStopwatch = new Stopwatch();
    timeoutStopwatch.start();
    synchronized (statusLock) {
      while (!isWorkerRunningTask(theWorker, task)) {
        statusLock.wait(config.getTaskAssignmentTimeoutDuration().getMillis());
        if (timeoutStopwatch.elapsed(TimeUnit.MILLISECONDS) >= config.getTaskAssignmentTimeoutDuration().getMillis()) {
          log.error(
              "Something went wrong! %s never ran task %s after %s!",
              theWorker.getHost(),
              task.getId(),
              config.getTaskAssignmentTimeoutDuration()
          );

          failTask(taskRunnerWorkItem);

          break;
        }
      }
    }
  }

  private void init()
  {
    try {
      // Add listener for creation/deletion of workers
      workerPathCache.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, final PathChildrenCacheEvent event) throws Exception
            {
              if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                final Worker worker = jsonMapper.readValue(
                    event.getData().getData(),
                    Worker.class
                );
                log.info("Worker[%s] reportin' for duty!", worker.getHost());
                ZkWorker zkWorker = addWorker(worker);
                initWorker(zkWorker);
                runPendingTasks();
              } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                final Worker worker = jsonMapper.readValue(
                    event.getData().getData(),
                    Worker.class
                );
                log.info("Kaboom! Worker[%s] removed!", worker.getHost());
                removeWorker(worker);
              }
            }
          }
      );
      workerPathCache.start();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * When a new worker appears, listeners are registered for status changes associated with tasks assigned to
   * the worker. Status changes indicate the creation or completion of a task.
   * The RemoteTaskRunner updates state according to these changes.
   *
   * @param worker - contains metadata for a worker that has appeared in ZK
   */
  private ZkWorker addWorker(final Worker worker)
  {
    try {
      final String workerStatusPath = JOINER.join(config.getIndexerStatusPath(), worker.getHost());
      final PathChildrenCache statusCache = new PathChildrenCache(cf, workerStatusPath, true);
      final ZkWorker zkWorker = new ZkWorker(
          worker,
          statusCache
      );

      zkWorkers.put(worker.getHost(), zkWorker);

      return zkWorker;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private void initWorker(final ZkWorker zkWorker)
  {
    try {
      // Add status listener to the watcher for status changes
      zkWorker.addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
              synchronized (statusLock) {
                try {
                  if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED) ||
                      event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    final String taskId = ZKPaths.getNodeFromPath(event.getData().getPath());
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

                    final TaskRunnerWorkItem taskRunnerWorkItem = runningTasks.get(taskId);
                    if (taskRunnerWorkItem == null) {
                      log.warn(
                          "WTF?! Worker[%s] announcing a status for a task I didn't know about: %s",
                          zkWorker.getWorker().getHost(),
                          taskId
                      );
                    } else {
                      zkWorker.addRunningTask(taskRunnerWorkItem.getTask());
                    }

                    if (taskStatus.isComplete()) {
                      if (taskRunnerWorkItem != null) {
                        final ListenableFuture<TaskStatus> result = taskRunnerWorkItem.getResult();
                        if (result != null) {
                          ((SettableFuture<TaskStatus>) result).set(taskStatus);
                        }

                        zkWorker.removeRunningTask(taskRunnerWorkItem.getTask());
                      }

                      // Worker is done with this task
                      zkWorker.setLastCompletedTaskTime(new DateTime());
                      cleanup(zkWorker.getWorker().getHost(), taskId);
                      runPendingTasks();
                    }
                  } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                    final String taskId = ZKPaths.getNodeFromPath(event.getData().getPath());
                    TaskRunnerWorkItem taskRunnerWorkItem = runningTasks.get(taskId);
                    if (taskRunnerWorkItem != null) {
                      log.info("Task %s just disappeared!", taskId);
                      failTask(taskRunnerWorkItem);
                    }
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
    ZkWorker zkWorker = zkWorkers.get(worker.getHost());
    if (zkWorker != null) {
      try {
        Set<String> tasksToRetry = Sets.newHashSet(
            cf.getChildren()
              .forPath(JOINER.join(config.getIndexerTaskPath(), worker.getHost()))
        );
        tasksToRetry.addAll(
            cf.getChildren()
              .forPath(JOINER.join(config.getIndexerStatusPath(), worker.getHost()))
        );
        log.info("%s has %d tasks to retry", worker.getHost(), tasksToRetry.size());

        for (String taskId : tasksToRetry) {
          TaskRunnerWorkItem taskRunnerWorkItem = runningTasks.get(taskId);
          if (taskRunnerWorkItem != null) {
            String taskPath = JOINER.join(config.getIndexerTaskPath(), worker.getHost(), taskId);
            if (cf.checkExists().forPath(taskPath) != null) {
              cf.delete().guaranteed().forPath(taskPath);
            }
            failTask(taskRunnerWorkItem);
          } else {
            log.warn("RemoteTaskRunner has no knowledge of task %s", taskId);
          }
        }

        zkWorker.close();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      finally {
        zkWorkers.remove(worker.getHost());
      }
    }
  }

  private ZkWorker findWorkerForTask(final Task task)
  {
    for (ZkWorker zkWorker : zkWorkers.values()) {
      if (zkWorker.canRunTask(task) &&
          zkWorker.getWorker().getVersion().compareTo(workerSetupData.get().getMinVersion()) >= 0) {
        return zkWorker;
      }
    }
    log.debug("Worker nodes %s do not have capacity to run any more tasks!", zkWorkers.values());
    return null;
  }

  private void failTask(TaskRunnerWorkItem taskRunnerWorkItem)
  {
    final ListenableFuture<TaskStatus> result = taskRunnerWorkItem.getResult();
    ((SettableFuture<TaskStatus>) result).set(TaskStatus.failure(taskRunnerWorkItem.getTask().getId()));
  }
}