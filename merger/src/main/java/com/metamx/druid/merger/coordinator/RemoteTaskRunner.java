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
import com.netflix.curator.framework.recipes.cache.ChildData;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
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
 * For example, {@link com.metamx.druid.merger.coordinator.scaling.ResourceManagmentScheduler} can take care of these duties.
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
  private final Map<String, WorkerWrapper> zkWorkers = new ConcurrentHashMap<String, WorkerWrapper>();
  // all tasks that have been assigned to a worker
  private final ConcurrentSkipListMap<String, TaskWrapper> runningTasks = new ConcurrentSkipListMap<String, TaskWrapper>();
  // tasks that have not yet run
  private final ConcurrentSkipListMap<String, TaskWrapper> pendingTasks = new ConcurrentSkipListMap<String, TaskWrapper>();

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

      for (WorkerWrapper workerWrapper : zkWorkers.values()) {
        workerWrapper.close();
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
  public Collection<WorkerWrapper> getWorkers()
  {
    return zkWorkers.values();
  }

  @Override
  public Collection<TaskWrapper> getRunningTasks()
  {
    return runningTasks.values();
  }

  @Override
  public Collection<TaskWrapper> getPendingTasks()
  {
    return pendingTasks.values();
  }

  public boolean isTaskRunning(String taskId)
  {
    return runningTasks.containsKey(taskId);
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
      throw new ISE("Assigned a task[%s] that is already running, WTF is happening?!", task.getId());
    }
    TaskWrapper taskWrapper = new TaskWrapper(
        task, context, callback, retryPolicyFactory.makeRetryPolicy(), System.currentTimeMillis()
    );
    addPendingTask(taskWrapper);
  }

  private void addPendingTask(final TaskWrapper taskWrapper)
  {
    pendingTasks.put(taskWrapper.getTask().getId(), taskWrapper);
    runPendingTasks();
  }

  /**
   * This method uses a single threaded executor to extract all pending tasks and attempt to run them. Any tasks that
   * are successfully started by a worker will be moved from pendingTasks to runningTasks. This method is thread-safe.
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
            // make a copy of the pending tasks because assignTask may delete tasks from pending and move them into moving
            List<TaskWrapper> copy = Lists.newArrayList(pendingTasks.values());
            for (TaskWrapper taskWrapper : copy) {
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
      throw Throwables.propagate(e);
    }
    catch (ExecutionException e) {
      throw Throwables.propagate(e.getCause());
    }
  }


  private void retryTask(final TaskWrapper taskWrapper, final String workerId, final String taskId)
  {
    scheduledExec.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            runningTasks.remove(taskId);
            cleanup(workerId, taskId);
            addPendingTask(taskWrapper);
          }
        },
        taskWrapper.getRetryPolicy().getAndIncrementRetryDelay().getMillis(),
        TimeUnit.MILLISECONDS
    );
  }

  private void cleanup(final String workerId, final String taskId)
  {
    try {
      final String statusPath = JOINER.join(config.getStatusPath(), workerId, taskId);
      cf.delete().guaranteed().forPath(statusPath);
    }
    catch (Exception e) {
      log.warn("Tried to delete a status path that didn't exist! Must've gone away already?");
    }

    try {
      final String taskPath = JOINER.join(config.getTaskPath(), workerId, taskId);
      cf.delete().guaranteed().forPath(taskPath);
    }
    catch (Exception e) {
      log.warn("Tried to delete a task path that didn't exist! Must've gone away already?");
    }
  }

  /**
   * Ensures no workers are already running a task before assigning the task to a worker.
   * It is possible that a worker is running a task that the RTR has no knowledge of. This is common when the RTR
   * needs to bootstrap after a restart.
   *
   * @param taskWrapper - a wrapper containing task metadata
   */
  private void assignTask(TaskWrapper taskWrapper)
  {
    try {
      WorkerWrapper workerWrapper = findWorkerRunningTask(taskWrapper);

      // If a worker is already running this task, we don't need to announce it
      if (workerWrapper != null) {
        final Worker worker = workerWrapper.getWorker();

        log.info("Worker[%s] is already running task[%s].", worker.getHost(), taskWrapper.getTask().getId());
        runningTasks.put(taskWrapper.getTask().getId(), pendingTasks.remove(taskWrapper.getTask().getId()));

        final ChildData statusData = workerWrapper.getStatusCache()
                                                  .getCurrentData(
                                                      JOINER.join(
                                                          config.getStatusPath(),
                                                          worker.getHost(),
                                                          taskWrapper.getTask().getId()
                                                      )
                                                  );

        final TaskStatus taskStatus = jsonMapper.readValue(
            statusData.getData(),
            TaskStatus.class
        );

        TaskCallback callback = taskWrapper.getCallback();
        if (callback != null) {
          callback.notify(taskStatus);
        }

        if (taskStatus.isComplete()) {
          cleanup(worker.getHost(), taskWrapper.getTask().getId());
        }
      } else {
        // Announce the task or retry if there is not enough capacity
        workerWrapper = findWorkerForTask();
        if (workerWrapper != null) {
          announceTask(workerWrapper.getWorker(), taskWrapper);
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
   * @param theWorker   The worker the task is assigned to
   * @param taskWrapper The task to be assigned
   */
  private void announceTask(Worker theWorker, TaskWrapper taskWrapper) throws Exception
  {

    final Task task = taskWrapper.getTask();
    final TaskContext taskContext = taskWrapper.getTaskContext();

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

    synchronized (statusLock) {
      // Syncing state with Zookeeper - wait for task to go into running queue
      while (pendingTasks.containsKey(task.getId())) {
        statusLock.wait(config.getTaskAssignmentTimeoutDuration().getMillis());
      }
    }
  }

  /**
   * When a new worker appears, listeners are registered for status changes.
   * Status changes indicate the creation or completion of task.
   * The RemoteTaskRunner updates state according to these changes.
   *
   * @param worker - contains metadata for a worker that has appeared in ZK
   */
  private void addWorker(final Worker worker)
  {
    try {
      final String workerStatusPath = JOINER.join(config.getStatusPath(), worker.getHost());
      final PathChildrenCache statusCache = new PathChildrenCache(cf, workerStatusPath, true);
      final WorkerWrapper workerWrapper = new WorkerWrapper(
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
                    retryTask(runningTasks.get(taskId), worker.getHost(), taskId);
                    throw Throwables.propagate(e);
                  }

                  log.info(
                      "Worker[%s] wrote %s status for task: %s",
                      worker.getHost(),
                      taskStatus.getStatusCode(),
                      taskId
                  );

                  if (pendingTasks.containsKey(taskId)) {
                    runningTasks.put(taskId, pendingTasks.remove(taskId));
                  }

                  synchronized (statusLock) {
                    statusLock.notify();
                  }

                  final TaskWrapper taskWrapper = runningTasks.get(taskId);
                  if (taskWrapper == null) {
                    log.warn(
                        "WTF?! Worker[%s] announcing a status for a task I didn't know about: %s",
                        worker.getHost(),
                        taskId
                    );
                  }

                  if (taskStatus.isComplete()) {
                    if (taskWrapper != null) {
                      final TaskCallback callback = taskWrapper.getCallback();

                      // Cleanup
                      if (callback != null) {
                        callback.notify(taskStatus);
                      }
                    }

                    // Worker is done with this task
                    workerWrapper.setLastCompletedTaskTime(new DateTime());
                    cf.delete().guaranteed().inBackground().forPath(event.getData().getPath());
                    runningTasks.remove(taskId);
                    runPendingTasks();
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
      zkWorkers.put(worker.getHost(), workerWrapper);
      statusCache.start();

      runPendingTasks();
    }

    catch (
        Exception e
        )

    {
      throw Throwables.propagate(e);
    }

  }

  /**
   * When a ephemeral worker node disappears from ZK, we have to make sure there are no tasks still assigned
   * to the worker. If tasks remain, they are retried.
   *
   * @param worker - the removed worker
   */
  private void removeWorker(final Worker worker)
  {
    WorkerWrapper workerWrapper = zkWorkers.get(worker.getHost());
    if (workerWrapper != null) {
      try {
        Set<String> tasksToRetry = Sets.newHashSet(workerWrapper.getRunningTasks());
        tasksToRetry.addAll(cf.getChildren().forPath(JOINER.join(config.getTaskPath(), worker.getHost())));

        for (String taskId : tasksToRetry) {
          TaskWrapper taskWrapper = runningTasks.get(taskId);
          if (taskWrapper != null) {
            retryTask(runningTasks.get(taskId), worker.getHost(), taskId);
          }
        }

        workerWrapper.getStatusCache().close();
      }
      catch (Exception e) {
        log.error(e, "Failed to cleanly remove worker[%s]");
      }
    }
    zkWorkers.remove(worker.getHost());
  }

  private WorkerWrapper findWorkerForTask()
  {
    try {
      final MinMaxPriorityQueue<WorkerWrapper> workerQueue = MinMaxPriorityQueue.<WorkerWrapper>orderedBy(
          new Comparator<WorkerWrapper>()
          {
            @Override
            public int compare(WorkerWrapper w1, WorkerWrapper w2)
            {
              return -Ints.compare(w1.getRunningTasks().size(), w2.getRunningTasks().size());
            }
          }
      ).create(
          FunctionalIterable.create(zkWorkers.values()).filter(
              new Predicate<WorkerWrapper>()
              {
                @Override
                public boolean apply(WorkerWrapper input)
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

  private WorkerWrapper findWorkerRunningTask(TaskWrapper taskWrapper)
  {
    for (WorkerWrapper workerWrapper : zkWorkers.values()) {
      if (workerWrapper.getRunningTasks().contains(taskWrapper.getTask().getId())) {
        return workerWrapper;
      }
    }
    return null;
  }
}
