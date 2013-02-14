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

package com.metamx.druid.merger.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskHolder;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.RemoteTaskRunnerConfig;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.emitter.EmittingLogger;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The RemoteTaskRunner's primary responsibility is to assign tasks to worker nodes and manage retries in failure
 * scenarios. The RemoteTaskRunner keeps track of which workers are running which tasks and manages coordinator and
 * worker interactions over Zookeeper. The RemoteTaskRunner is event driven and updates state according to ephemeral
 * node changes in ZK.
 * <p/>
 * The RemoteTaskRunner will assign tasks to a node until the node hits capacity. At that point, task assignment will
 * fail. The RemoteTaskRunner depends on another component to create additional worker resources.
 * For example, {@link com.metamx.druid.merger.coordinator.scaling.ResourceManagementScheduler} can take care of these duties.
 * <p/>
 * If a worker node becomes inexplicably disconnected from Zk, the RemoteTaskRunner will automatically retry any tasks
 * that were associated with the node.
 */
public class RemoteTaskRunner implements TaskRunner
{
  private static final EmittingLogger log = new EmittingLogger(RemoteTaskRunner.class);
  private static final Joiner JOINER = Joiner.on("/");

  private final ObjectMapper jsonMapper;
  private final RemoteTaskRunnerConfig config;
  private final CuratorFramework cf;
  private final PathChildrenCache workerPathCache;
  private final ScheduledExecutorService scheduledExec;
  private final RetryPolicyFactory retryPolicyFactory;
  private final WorkerSetupManager workerSetupManager;

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
      ScheduledExecutorService scheduledExec,
      RetryPolicyFactory retryPolicyFactory,
      WorkerSetupManager workerSetupManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.cf = cf;
    this.workerPathCache = workerPathCache;
    this.scheduledExec = scheduledExec;
    this.retryPolicyFactory = retryPolicyFactory;
    this.workerSetupManager = workerSetupManager;
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
              if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                final Worker worker = jsonMapper.readValue(
                    event.getData().getData(),
                    Worker.class
                );
                log.info("New worker[%s] found!", worker.getHost());
                addWorker(worker);
              } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                final Worker worker = jsonMapper.readValue(
                    event.getData().getData(),
                    Worker.class
                );
                log.info("Worker[%s] removed!", worker.getHost());
                removeWorker(worker);
              }
            }
          }
      );
      workerPathCache.start();

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
      if (zkWorker.getRunningTasks().contains(taskId)) {
        return zkWorker;
      }
    }
    return null;
  }

  /**
   * A task will be run only if there is no current knowledge in the RemoteTaskRunner of the task.
   *
   * @param task     task to run
   * @param context  task context to run under
   * @param callback callback to be called exactly once
   */
  @Override
  public void run(Task task, TaskContext context, TaskCallback callback)
  {
    if (runningTasks.containsKey(task.getId()) || pendingTasks.containsKey(task.getId())) {
      throw new ISE("Assigned a task[%s] that is already running or pending, WTF is happening?!", task.getId());
    }
    TaskRunnerWorkItem taskRunnerWorkItem = new TaskRunnerWorkItem(
        task, context, callback, retryPolicyFactory.makeRetryPolicy(), new DateTime()
    );
    addPendingTask(taskRunnerWorkItem);
  }

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
    Future future = runPendingTasksExec.submit(
        new Callable<Void>()
        {
          @Override
          public Void call() throws Exception
          {
            // make a copy of the pending tasks because assignTask may delete tasks from pending and move them
            // into running status
            List<TaskRunnerWorkItem> copy = Lists.newArrayList(pendingTasks.values());
            for (TaskRunnerWorkItem taskWrapper : copy) {
              assignTask(taskWrapper);
            }

            return null;
          }
        }
    );

    try {
      future.get();
    }
    catch (InterruptedException e) {
      log.error(e, "InterruptedException in runPendingTasks()");
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      log.error(e, "ExecutionException in runPendingTasks()");
      throw Throwables.propagate(e.getCause());
    }
  }

  /**
   * Retries a task by inserting it back into the pending queue after a given delay.
   * This method will also clean up any status paths that were associated with the task.
   *
   * @param taskRunnerWorkItem - the task to retry
   * @param workerId           - the worker that was previously running this task
   */
  private void retryTask(final TaskRunnerWorkItem taskRunnerWorkItem, final String workerId)
  {
    final String taskId = taskRunnerWorkItem.getTask().getId();
    log.info("Retry scheduled in %s for %s", taskRunnerWorkItem.getRetryPolicy().getRetryDelay(), taskId);
    scheduledExec.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            cleanup(workerId, taskId);
            addPendingTask(taskRunnerWorkItem);
          }
        },
        taskRunnerWorkItem.getRetryPolicy().getAndIncrementRetryDelay().getMillis(),
        TimeUnit.MILLISECONDS
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
    final String statusPath = JOINER.join(config.getStatusPath(), workerId, taskId);
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
      ZkWorker zkWorker = findWorkerRunningTask(taskId);

      // If a worker is already running this task, we don't need to announce it
      if (zkWorker != null) {
        final Worker worker = zkWorker.getWorker();
        log.info("Worker[%s] is already running task[%s].", worker.getHost(), taskId);
        runningTasks.put(taskId, pendingTasks.remove(taskId));
        log.info("Task %s switched from pending to running", taskId);
      } else {
        // Nothing running this task, announce it in ZK for a worker to run it
        zkWorker = findWorkerForTask();
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
    final TaskContext taskContext = taskRunnerWorkItem.getTaskContext();

    log.info("Coordinator asking Worker[%s] to add task[%s]", theWorker.getHost(), task.getId());

    byte[] rawBytes = jsonMapper.writeValueAsBytes(new TaskHolder(task, taskContext));
    if (rawBytes.length > config.getMaxNumBytes()) {
      throw new ISE("Length of raw bytes for task too large[%,d > %,d]", rawBytes.length, config.getMaxNumBytes());
    }

    cf.create()
      .withMode(CreateMode.EPHEMERAL)
      .forPath(
          JOINER.join(
              config.getTaskPath(),
              theWorker.getHost(),
              task.getId()
          ),
          jsonMapper.writeValueAsBytes(new TaskHolder(task, taskContext))
      );

    runningTasks.put(task.getId(), pendingTasks.remove(task.getId()));
    log.info("Task %s switched from pending to running", task.getId());

    // Syncing state with Zookeeper - don't assign new tasks until the task we just assigned is actually running
    // on a worker - this avoids overflowing a worker with tasks
    synchronized (statusLock) {
      while (findWorkerRunningTask(task.getId()) == null) {
        statusLock.wait(config.getTaskAssignmentTimeoutDuration().getMillis());
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
  private void addWorker(final Worker worker)
  {
    try {
      final String workerStatusPath = JOINER.join(config.getStatusPath(), worker.getHost());
      final PathChildrenCache statusCache = new PathChildrenCache(cf, workerStatusPath, true);
      final ZkWorker zkWorker = new ZkWorker(
          worker,
          statusCache,
          jsonMapper
      );

      // Add status listener to the watcher for status changes
      statusCache.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {

              try {
                if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED) ||
                    event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                  final String taskId = ZKPaths.getNodeFromPath(event.getData().getPath());
                  final TaskStatus taskStatus;

                  // This can fail if a worker writes a bogus status. Retry if so.
                  try {
                    taskStatus = jsonMapper.readValue(
                        event.getData().getData(), TaskStatus.class
                    );

                    if (!taskStatus.getId().equals(taskId)) {
                      // Sanity check
                      throw new ISE(
                          "Worker[%s] status id does not match payload id: %s != %s",
                          worker.getHost(),
                          taskId,
                          taskStatus.getId()
                      );
                    }
                  }
                  catch (Exception e) {
                    log.warn(e, "Worker[%s] wrote bogus status for task: %s", worker.getHost(), taskId);
                    retryTask(runningTasks.get(taskId), worker.getHost());
                    throw Throwables.propagate(e);
                  }

                  log.info(
                      "Worker[%s] wrote %s status for task: %s",
                      worker.getHost(),
                      taskStatus.getStatusCode(),
                      taskId
                  );

                  // Synchronizing state with ZK
                  synchronized (statusLock) {
                    statusLock.notify();
                  }

                  final TaskRunnerWorkItem taskRunnerWorkItem = runningTasks.get(taskId);
                  if (taskRunnerWorkItem == null) {
                    log.warn(
                        "WTF?! Worker[%s] announcing a status for a task I didn't know about: %s",
                        worker.getHost(),
                        taskId
                    );
                  }

                  if (taskStatus.isComplete()) {
                    if (taskRunnerWorkItem != null) {
                      final TaskCallback callback = taskRunnerWorkItem.getCallback();
                      if (callback != null) {
                        callback.notify(taskStatus);
                      }
                    }

                    // Worker is done with this task
                    zkWorker.setLastCompletedTaskTime(new DateTime());
                    cleanup(worker.getHost(), taskId);
                    runPendingTasks();
                  }
                } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                  final String taskId = ZKPaths.getNodeFromPath(event.getData().getPath());
                  if (runningTasks.containsKey(taskId)) {
                    log.info("Task %s just disappeared!", taskId);
                    retryTask(runningTasks.get(taskId), worker.getHost());
                  }
                }
              }
              catch (Exception e) {
                log.makeAlert(e, "Failed to handle new worker status")
                   .addData("worker", worker.getHost())
                   .addData("znode", event.getData().getPath())
                   .emit();
              }
            }
          }
      );
      zkWorkers.put(worker.getHost(), zkWorker);
      statusCache.start();

      runPendingTasks();
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
        Set<String> tasksPending = Sets.newHashSet(
            cf.getChildren()
              .forPath(JOINER.join(config.getTaskPath(), worker.getHost()))
        );
        log.info("%s had %d tasks pending", worker.getHost(), tasksPending.size());

        for (String taskId : tasksPending) {
          TaskRunnerWorkItem taskRunnerWorkItem = pendingTasks.get(taskId);
          if (taskRunnerWorkItem != null) {
            cf.delete().guaranteed().forPath(JOINER.join(config.getTaskPath(), worker.getHost(), taskId));
            retryTask(taskRunnerWorkItem, worker.getHost());
          } else {
            log.warn("RemoteTaskRunner has no knowledge of pending task %s", taskId);
          }
        }

        zkWorker.getStatusCache().close();
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
      finally {
        zkWorkers.remove(worker.getHost());
      }
    }
  }

  private ZkWorker findWorkerForTask()
  {
    try {
      final MinMaxPriorityQueue<ZkWorker> workerQueue = MinMaxPriorityQueue.<ZkWorker>orderedBy(
          new Comparator<ZkWorker>()
          {
            @Override
            public int compare(ZkWorker w1, ZkWorker w2)
            {
              return -Ints.compare(w1.getRunningTasks().size(), w2.getRunningTasks().size());
            }
          }
      ).create(
          FunctionalIterable.create(zkWorkers.values()).filter(
              new Predicate<ZkWorker>()
              {
                @Override
                public boolean apply(ZkWorker input)
                {
                  return (!input.isAtCapacity() &&
                          input.getWorker()
                               .getVersion()
                               .compareTo(workerSetupManager.getWorkerSetupData().getMinVersion()) >= 0);
                }
              }
          )
      );

      if (workerQueue.isEmpty()) {
        log.info("Worker nodes %s do not have capacity to run any more tasks!", zkWorkers.values());
        return null;
      }

      return workerQueue.peek();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
