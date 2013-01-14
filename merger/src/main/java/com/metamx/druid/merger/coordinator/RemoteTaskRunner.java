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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
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
import com.metamx.druid.merger.coordinator.scaling.AutoScalingData;
import com.metamx.druid.merger.coordinator.scaling.ScalingStrategy;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.emitter.EmittingLogger;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import com.netflix.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Period;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private final PathChildrenCache workerPathCache;
  private final ScheduledExecutorService scheduledExec;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ScalingStrategy strategy;
  private final WorkerSetupManager workerSetupManager;

  // all workers that exist in ZK
  private final Map<String, WorkerWrapper> zkWorkers = new ConcurrentHashMap<String, WorkerWrapper>();
  // all tasks that are assigned or need to be assigned
  private final Map<String, TaskWrapper> tasks = new ConcurrentHashMap<String, TaskWrapper>();

  private final ConcurrentSkipListSet<String> currentlyProvisioning = new ConcurrentSkipListSet<String>();
  private final ConcurrentSkipListSet<String> currentlyTerminating = new ConcurrentSkipListSet<String>();
  private final Object statusLock = new Object();

  private volatile DateTime lastProvisionTime = new DateTime();
  private volatile DateTime lastTerminateTime = new DateTime();
  private volatile boolean started = false;

  public RemoteTaskRunner(
      ObjectMapper jsonMapper,
      RemoteTaskRunnerConfig config,
      CuratorFramework cf,
      PathChildrenCache workerPathCache,
      ScheduledExecutorService scheduledExec,
      RetryPolicyFactory retryPolicyFactory,
      ScalingStrategy strategy,
      WorkerSetupManager workerSetupManager
  )
  {
    this.jsonMapper = jsonMapper;
    this.config = config;
    this.cf = cf;
    this.workerPathCache = workerPathCache;
    this.scheduledExec = scheduledExec;
    this.retryPolicyFactory = retryPolicyFactory;
    this.strategy = strategy;
    this.workerSetupManager = workerSetupManager;
  }

  @LifecycleStart
  public void start()
  {
    try {
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

      // Schedule termination of worker nodes periodically
      Period period = new Period(config.getTerminateResourcesDuration());
      PeriodGranularity granularity = new PeriodGranularity(period, config.getTerminateResourcesOriginDateTime(), null);
      final long startTime = granularity.next(granularity.truncate(new DateTime().getMillis()));

      ScheduledExecutors.scheduleAtFixedRate(
          scheduledExec,
          new Duration(
              System.currentTimeMillis(),
              startTime
          ),
          config.getTerminateResourcesDuration(),
          new Runnable()
          {
            @Override
            public void run()
            {
              if (currentlyTerminating.isEmpty()) {
                if (zkWorkers.size() <= workerSetupManager.getWorkerSetupData().getMinNumWorkers()) {
                  return;
                }

                List<WorkerWrapper> thoseLazyWorkers = Lists.newArrayList(
                    FunctionalIterable
                        .create(zkWorkers.values())
                        .filter(
                            new Predicate<WorkerWrapper>()
                            {
                              @Override
                              public boolean apply(WorkerWrapper input)
                              {
                                return input.getRunningTasks().isEmpty()
                                       && System.currentTimeMillis() - input.getLastCompletedTaskTime().getMillis()
                                          > config.getMaxWorkerIdleTimeMillisBeforeDeletion();
                              }
                            }
                        )
                );

                AutoScalingData terminated = strategy.terminate(
                    Lists.transform(
                        thoseLazyWorkers,
                        new Function<WorkerWrapper, String>()
                        {
                          @Override
                          public String apply(WorkerWrapper input)
                          {
                            return input.getWorker().getIp();
                          }
                        }
                    )
                );

                if (terminated != null) {
                  currentlyTerminating.addAll(terminated.getNodeIds());
                  lastTerminateTime = new DateTime();
                }
              } else {
                Duration durSinceLastTerminate = new Duration(new DateTime(), lastTerminateTime);
                if (durSinceLastTerminate.isLongerThan(config.getMaxScalingDuration())) {
                  log.makeAlert("Worker node termination taking too long")
                     .addData("millisSinceLastTerminate", durSinceLastTerminate.getMillis())
                     .addData("terminatingCount", currentlyTerminating.size())
                     .emit();
                }

                log.info(
                    "%s still terminating. Wait for all nodes to terminate before trying again.",
                    currentlyTerminating
                );
              }
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

  public boolean hasStarted()
  {
    return started;
  }

  public int getNumWorkers()
  {
    return zkWorkers.size();
  }

  @Override
  public void run(Task task, TaskContext context, TaskCallback callback)
  {
    if (tasks.containsKey(task.getId())) {
      throw new ISE("Assigned a task[%s] that already exists, WTF is happening?!", task.getId());
    }
    TaskWrapper taskWrapper = new TaskWrapper(
        task, context, callback, retryPolicyFactory.makeRetryPolicy()
    );
    tasks.put(taskWrapper.getTask().getId(), taskWrapper);
    assignTask(taskWrapper);
  }

  private void assignTask(TaskWrapper taskWrapper)
  {
    WorkerWrapper workerWrapper = findWorkerRunningTask(taskWrapper);

    // If the task already exists, we don't need to announce it
    if (workerWrapper != null) {
      final Worker worker = workerWrapper.getWorker();
      try {
        log.info("Worker[%s] is already running task[%s].", worker.getHost(), taskWrapper.getTask().getId());

        TaskStatus taskStatus = jsonMapper.readValue(
            workerWrapper.getStatusCache()
                         .getCurrentData(
                             JOINER.join(config.getStatusPath(), worker.getHost(), taskWrapper.getTask().getId())
                         )
                         .getData(),
            TaskStatus.class
        );

        if (taskStatus.isComplete()) {
          TaskCallback callback = taskWrapper.getCallback();
          if (callback != null) {
            callback.notify(taskStatus);
          }
          new CleanupPaths(worker.getHost(), taskWrapper.getTask().getId()).run();
        }
      }
      catch (Exception e) {
        log.error(e, "Task exists, but hit exception!");
        retryTask(new CleanupPaths(worker.getHost(), taskWrapper.getTask().getId()), taskWrapper);
      }
    } else {
      // Announce the task or retry if there is not enough capacity
      workerWrapper = findWorkerForTask();
      if (workerWrapper != null) {
        announceTask(workerWrapper.getWorker(), taskWrapper);
      } else {
        retryTask(null, taskWrapper);
      }
    }
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
      log.makeAlert("Task exceeded maximum retry count")
         .addData("task", task.getId())
         .addData("retryCount", retryPolicy.getNumRetries())
         .emit();
      return;
    }

    scheduledExec.schedule(
        new Runnable()
        {
          @Override
          public void run()
          {
            if (pre != null) {
              pre.run();
            }

            if (tasks.containsKey(task.getId())) {
              log.info("Retry[%d] for task[%s]", retryPolicy.getNumRetries(), task.getId());
              assignTask(taskWrapper);
            }
          }
        },
        retryPolicy.getAndIncrementRetryDelay().getMillis(),
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
      currentlyProvisioning.removeAll(strategy.ipLookup(Arrays.<String>asList(worker.getIp())));

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
              synchronized (statusLock) {
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
                      retryTask(new CleanupPaths(worker.getHost(), taskId), tasks.get(taskId));
                      throw Throwables.propagate(e);
                    }

                    log.info(
                        "Worker[%s] wrote %s status for task: %s",
                        worker.getHost(),
                        taskStatus.getStatusCode(),
                        taskId
                    );

                    statusLock.notify();

                    if (taskStatus.isComplete()) {
                      // Worker is done with this task
                      workerWrapper.setLastCompletedTaskTime(new DateTime());
                      final TaskWrapper taskWrapper = tasks.get(taskId);

                      if (taskWrapper == null) {
                        log.warn(
                            "WTF?! Worker[%s] completed a task I didn't know about: %s",
                            worker.getHost(),
                            taskId
                        );
                      } else {
                        final TaskCallback callback = taskWrapper.getCallback();

                        // Cleanup
                        if (callback != null) {
                          callback.notify(taskStatus);
                        }
                        tasks.remove(taskId);
                        cf.delete().guaranteed().inBackground().forPath(event.getData().getPath());
                      }
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
          }
      );
      zkWorkers.put(worker.getHost(), workerWrapper);
      statusCache.start();
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
   * @param worker - the removed worker
   */
  private void removeWorker(final Worker worker)
  {
    currentlyTerminating.remove(worker.getHost());

    WorkerWrapper workerWrapper = zkWorkers.get(worker.getHost());
    if (workerWrapper != null) {
      try {
        Set<String> tasksToRetry = Sets.newHashSet(workerWrapper.getRunningTasks());
        tasksToRetry.addAll(cf.getChildren().forPath(JOINER.join(config.getTaskPath(), worker.getHost())));

        for (String taskId : tasksToRetry) {
          TaskWrapper taskWrapper = tasks.get(taskId);
          if (taskWrapper != null) {
            retryTask(new CleanupPaths(worker.getHost(), taskId), tasks.get(taskId));
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
        log.info("Worker nodes do not have capacity to run any more tasks!");

        if (currentlyProvisioning.isEmpty()) {
          AutoScalingData provisioned = strategy.provision();
          if (provisioned != null) {
            currentlyProvisioning.addAll(provisioned.getNodeIds());
            lastProvisionTime = new DateTime();
          }
        } else {
          Duration durSinceLastProvision = new Duration(new DateTime(), lastProvisionTime);
          if (durSinceLastProvision.isLongerThan(config.getMaxScalingDuration())) {
            log.makeAlert("Worker node provisioning taking too long")
               .addData("millisSinceLastProvision", durSinceLastProvision.getMillis())
               .addData("provisioningCount", currentlyProvisioning.size())
               .emit();
          }

          log.info(
              "%s still provisioning. Wait for all provisioned nodes to complete before requesting new worker.",
              currentlyProvisioning
          );
        }
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

        // Syncing state with Zookeeper
        while (findWorkerRunningTask(taskWrapper) == null) {
          statusLock.wait(config.getTaskAssignmentTimeoutDuration().getMillis());
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
  }
}
