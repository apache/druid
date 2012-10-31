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
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.PeriodGranularity;
import com.metamx.druid.merger.common.TaskHolder;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.RemoteTaskRunnerConfig;
import com.metamx.druid.merger.coordinator.scaling.ScalingStrategy;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.emitter.EmittingLogger;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.io.IOException;
import java.util.Comparator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * The RemoteTaskRunner encapsulates all interactions with Zookeeper and keeps track of which workers
 * are running which tasks. The RemoteTaskRunner is event driven and updates state according to ephemeral node
 * changes in ZK.
 * <p/>
 * The RemoteTaskRunner will assign tasks to a node until the node hits capacity. RemoteTaskRunners have scaling
 * strategies to help them decide when to create or delete new resources. When tasks are assigned to the remote
 * task runner and no workers have capacity to handle the task, provisioning will be done according to the strategy.
 * The remote task runner periodically runs a check to see if any worker nodes have not had any work for a
 * specified period of time. If so, the worker node will be terminated.
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
  private final PathChildrenCache workerListener;
  private final ScheduledExecutorService scheduledExec;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ConcurrentHashMap<String, WorkerWrapper> zkWorkers; // all workers that exist in ZK
  private final ConcurrentHashMap<String, TaskWrapper> tasks; // all tasks that are assigned or need to be assigned
  private final ScalingStrategy strategy;

  private final Object statusLock = new Object();

  private volatile boolean started = false;

  public RemoteTaskRunner(
      ObjectMapper jsonMapper,
      RemoteTaskRunnerConfig config,
      CuratorFramework cf,
      PathChildrenCache workerListener,
      ScheduledExecutorService scheduledExec,
      RetryPolicyFactory retryPolicyFactory,
      ConcurrentHashMap<String, WorkerWrapper> zkWorkers,
      ConcurrentHashMap<String, TaskWrapper> tasks,
      ScalingStrategy strategy
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.cf = cf;
    this.workerListener = workerListener;
    this.scheduledExec = scheduledExec;
    this.retryPolicyFactory = retryPolicyFactory;
    this.zkWorkers = zkWorkers;
    this.tasks = tasks;
    this.strategy = strategy;
  }

  @LifecycleStart
  public void start()
  {
    try {
      workerListener.start();
      workerListener.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, final PathChildrenCacheEvent event) throws Exception
            {
              if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                final Worker worker = jsonMapper.readValue(
                    cf.getData().forPath(event.getData().getPath()),
                    Worker.class
                );

                log.info("New worker[%s] found!", worker.getHost());
                addWorker(worker);
              } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                // Get the worker host from the path
                String workerHost = event.getData().getPath().substring(event.getData().getPath().lastIndexOf("/") + 1);

                log.info("Worker[%s] removed!", workerHost);
                removeWorker(workerHost);
              }
            }
          }
      );

      // Schedule termination of worker nodes periodically
      Period period = new Period(config.getTerminateResourcesPeriodMs());
      PeriodGranularity granularity = new PeriodGranularity(period, null, null);
      final long truncatedNow = granularity.truncate(new DateTime().getMillis());

      ScheduledExecutors.scheduleAtFixedRate(
          scheduledExec,
          new Duration(
              System.currentTimeMillis(),
              granularity.next(truncatedNow) - config.getTerminateResourcesWindowMs()
          ),
          new Duration(config.getTerminateResourcesPeriodMs()),
          new Runnable()
          {
            @Override
            public void run()
            {
              strategy.terminateIfNeeded(zkWorkers);
            }
          }
      );

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
      for (WorkerWrapper workerWrapper : zkWorkers.values()) {
        workerWrapper.getWatcher().close();
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      started = false;
    }
  }

  public boolean hasStarted()
  {
    return started;
  }

  @Override
  public void run(Task task, TaskContext context, TaskCallback callback)
  {
    assignTask(
        new TaskWrapper(
            task, context, callback, retryPolicyFactory.makeRetryPolicy()
        )
    );
  }

  private boolean assignTask(TaskWrapper taskWrapper)
  {
    // If the task already exists, we don't need to announce it
    try {
      WorkerWrapper workerWrapper;
      if ((workerWrapper = findWorkerRunningTask(taskWrapper)) != null) {
        final Worker worker = workerWrapper.getWorker();

        log.info("Worker[%s] is already running task{%s].", worker.getHost(), taskWrapper.getTask().getId());

        TaskStatus taskStatus = jsonMapper.readValue(
            cf.getData().forPath(JOINER.join(config.getStatusPath(), worker.getHost(), taskWrapper.getTask().getId())),
            TaskStatus.class
        );

        if (taskStatus.isComplete()) {
          TaskCallback callback = taskWrapper.getCallback();
          if (callback != null) {
            callback.notify(taskStatus);
          }
          new CleanupPaths(worker.getHost(), taskWrapper.getTask().getId()).run();
        } else {
          tasks.put(taskWrapper.getTask().getId(), taskWrapper);
        }
        return true;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    // Announce the task
    WorkerWrapper workerWrapper = getWorkerForTask();
    if (workerWrapper != null) {
      announceTask(workerWrapper.getWorker(), taskWrapper);
      return true;
    }

    return false;
  }

  /**
   * Retries a task that has failed.
   *
   * @param pre         - A runnable that is executed before the retry occurs
   * @param taskWrapper - a container for task properties
   */
  private void retryTask(
      final Runnable pre,
      final TaskWrapper taskWrapper
  )
  {
    final Task task = taskWrapper.getTask();
    final RetryPolicy retryPolicy = taskWrapper.getRetryPolicy();

    log.info("Registering retry for failed task[%s]", task.getId());

    if (retryPolicy.hasExceededRetryThreshold()) {
      log.makeAlert("Task [%s] has failed[%d] times, giving up!", task.getId(), retryPolicy.getNumRetries())
         .emit();
      return;
    }

    scheduledExec.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              if (pre != null) {
                pre.run();
              }

              if (tasks.containsKey(task.getId())) {
                log.info("Retry[%d] for task[%s]", retryPolicy.getNumRetries(), task.getId());
                if (!assignTask(taskWrapper)) {
                  throw new ISE("Unable to find worker to send retry request to for task[%s]", task.getId());
                }
              }
            }
            catch (Exception e) {
              retryTask(null, taskWrapper);
            }
          }
        },
        retryPolicy.getAndIncrementRetryDelay(),
        TimeUnit.MILLISECONDS
    );
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
      final String workerStatus = JOINER.join(config.getStatusPath(), worker.getHost());
      final ConcurrentSkipListSet<String> runningTasks = new ConcurrentSkipListSet<String>(
          cf.getChildren().forPath(workerStatus)
      );
      final PathChildrenCache watcher = new PathChildrenCache(cf, workerStatus, false);
      final WorkerWrapper workerWrapper = new WorkerWrapper(
          worker,
          runningTasks,
          watcher
      );

      // Add status listener to the watcher for status changes
      watcher.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
            {
              synchronized (statusLock) {
                String taskId = null;
                try {
                  if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                    String statusPath = event.getData().getPath();
                    TaskStatus taskStatus = jsonMapper.readValue(
                        cf.getData().forPath(statusPath), TaskStatus.class
                    );
                    taskId = taskStatus.getId();

                    log.info("New status[%s] appeared!", taskId);
                    runningTasks.add(taskId);
                    statusLock.notify();
                  } else if (event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                    String statusPath = event.getData().getPath();
                    TaskStatus taskStatus = jsonMapper.readValue(
                        cf.getData().forPath(statusPath), TaskStatus.class
                    );
                    taskId = taskStatus.getId();

                    log.info("Task[%s] updated status[%s]!", taskId, taskStatus.getStatusCode());

                    if (taskStatus.isComplete()) {
                      workerWrapper.setLastCompletedTaskTime(new DateTime());
                      TaskWrapper taskWrapper = tasks.get(taskId);

                      if (taskWrapper == null) {
                        log.warn("A task completed that I didn't know about? WTF?!");
                      } else {
                        TaskCallback callback = taskWrapper.getCallback();

                        // Cleanup
                        if (callback != null) {
                          callback.notify(taskStatus);
                        }
                        tasks.remove(taskId);
                        runningTasks.remove(taskId);
                        cf.delete().guaranteed().forPath(statusPath);
                      }
                    }
                  }
                }
                catch (Exception e) {
                  retryTask(new CleanupPaths(worker.getHost(), taskId), tasks.get(taskId));
                }
              }
            }
          }
      );
      zkWorkers.put(worker.getHost(), workerWrapper);
      watcher.start();
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

  /**
   * When a ephemeral worker node disappears from ZK, we have to make sure there are no tasks still assigned
   * to the worker. If tasks remain, they are retried.
   *
   * @param workerId - id of the removed worker
   */
  private void removeWorker(final String workerId)
  {
    WorkerWrapper workerWrapper = zkWorkers.get(workerId);
    if (workerWrapper != null) {
      for (String taskId : workerWrapper.getRunningTasks()) {
        TaskWrapper taskWrapper = tasks.get(taskId);
        if (taskWrapper != null) {
          retryTask(new CleanupPaths(workerId, taskId), tasks.get(taskId));
        }
        workerWrapper.removeTask(taskId);
      }

      try {
        workerWrapper.getWatcher().close();
      }
      catch (IOException e) {
        log.error("Failed to close watcher associated with worker[%s]", workerWrapper.getWorker().getHost());
      }
    }
    zkWorkers.remove(workerId);
  }

  private WorkerWrapper getWorkerForTask()
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
                          input.getWorker().getVersion().compareTo(config.getMinWorkerVersion()) >= 0);
                }
              }
          )
      );

      if (workerQueue.isEmpty()) {
        log.makeAlert("There are no worker nodes with capacity to run task!").emit();
        strategy.provision(zkWorkers);
        return null;
      }

      return workerQueue.peek();
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
  private void announceTask(Worker theWorker, TaskWrapper taskWrapper)
  {
    synchronized (statusLock) {
      final Task task = taskWrapper.getTask();
      final TaskContext taskContext = taskWrapper.getTaskContext();
      try {
        log.info("Coordinator asking Worker[%s] to add task[%s]", theWorker.getHost(), task.getId());

        tasks.put(task.getId(), taskWrapper);

        cf.create()
          .creatingParentsIfNeeded()
          .withMode(CreateMode.EPHEMERAL)
          .forPath(
              JOINER.join(
                  config.getTaskPath(),
                  theWorker.getHost(),
                  task.getId()
              ),
              jsonMapper.writeValueAsBytes(new TaskHolder(task, taskContext))
          );

        while (findWorkerRunningTask(taskWrapper) == null) {
          statusLock.wait();
        }
      }
      catch (Exception e) {
        log.error(e, "Exception creating task[%s] for worker node[%s]", task.getId(), theWorker.getHost());
        throw Throwables.propagate(e);
      }
    }
  }

  private class CleanupPaths implements Runnable
  {
    private final String workerId;
    private final String taskId;

    private CleanupPaths(String workerId, String taskId)
    {
      this.workerId = workerId;
      this.taskId = taskId;
    }

    @Override
    public void run()
    {
      try {
        final String statusPath = JOINER.join(config.getStatusPath(), workerId, taskId);
        final String taskPath = JOINER.join(config.getTaskPath(), workerId, taskId);
        cf.delete().guaranteed().forPath(statusPath);
        cf.delete().guaranteed().forPath(taskPath);
      }
      catch (Exception e) {
        log.warn("Tried to delete a path that didn't exist! Must've gone away already!");
      }
    }
  }
}
