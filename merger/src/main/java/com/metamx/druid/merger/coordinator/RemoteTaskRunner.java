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
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.merger.common.TaskHolder;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.emitter.EmittingLogger;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 */
public class RemoteTaskRunner implements TaskRunner
{
  private static final EmittingLogger log = new EmittingLogger(RemoteTaskRunner.class);
  private static final Joiner JOINER = Joiner.on("/");

  private final ObjectMapper jsonMapper;
  private final TaskInventoryManager taskInventoryManager;
  private final IndexerZkConfig config;
  private final CuratorFramework cf;
  private final ScheduledExecutorService scheduledExec;
  private final RetryPolicyFactory retryPolicyFactory;

  private final ConcurrentHashMap<String, PathChildrenCache> monitors = new ConcurrentHashMap<String, PathChildrenCache>();

  public RemoteTaskRunner(
      ObjectMapper jsonMapper,
      TaskInventoryManager taskInventoryManager,
      IndexerZkConfig config,
      CuratorFramework cf,
      ScheduledExecutorService scheduledExec,
      RetryPolicyFactory retryPolicyFactory
  )
  {
    this.jsonMapper = jsonMapper;
    this.taskInventoryManager = taskInventoryManager;
    this.config = config;
    this.cf = cf;
    this.scheduledExec = scheduledExec;
    this.retryPolicyFactory = retryPolicyFactory;
  }

  @LifecycleStop
  public void stop()
  {
    scheduledExec.shutdownNow();
  }

  @Override
  public void run(final Task task, final TaskContext taskContext, final TaskCallback callback)
  {
    run(task, taskContext, callback, retryPolicyFactory.makeRetryPolicy());
  }

  private void run(
      final Task task,
      final TaskContext taskContext,
      final TaskCallback callback,
      final RetryPolicy retryPolicy
  )
  {
    try {
      // If a worker is already running this task, check the status
      Map<String, Worker> allRunningTasks = Maps.newHashMap();
      for (Worker worker : taskInventoryManager.getInventory()) {
        for (String taskId : worker.getTasks().keySet()) {
          allRunningTasks.put(taskId, worker);
        }
      }

      Worker workerRunningThisTask = allRunningTasks.get(task.getId());
      if (workerRunningThisTask != null) {
        // If the status is complete, just run the callback, otherwise monitor for the completion of the task
        if (!verifyStatusComplete(jsonMapper, workerRunningThisTask, task, callback)) {
          monitorStatus(jsonMapper, workerRunningThisTask, task, taskContext, callback, retryPolicy);
        }
        return;
      }

      // Run the task if it does not currently exist
      Worker theWorker = getLeastCapacityWorker();
      monitorStatus(jsonMapper, theWorker, task, taskContext, callback, retryPolicy);
      announceTask(theWorker, task, taskContext);
    }
    catch (Exception e) {
      log.error(e, "Failed to dispatch task. Retrying");
      retryTask(task, taskContext, callback, retryPolicy);
    }
  }

  private void retryTask(
      final Task task,
      final TaskContext taskContext,
      final TaskCallback callback,
      final RetryPolicy retryPolicy
  )
  {
    if (retryPolicy.hasExceededRetryThreshold()) {
      log.makeAlert("Task [%s] has failed[%d] times, giving up!", task.getId(), retryPolicy.getNumRetries())
         .emit();
      callback.notify(TaskStatus.failure(task.getId()));

      return;
    }

    scheduledExec.schedule(
        new Callable<Object>()
        {
          @Override
          public Object call() throws Exception
          {
            retryPolicy.runRunnables();

            log.info("Retry[%d] for task[%s]", retryPolicy.getNumRetries(), task.getId());
            run(task, taskContext, callback, retryPolicy);
            return null;
          }
        },
        retryPolicy.getAndIncrementRetryDelay(),
        TimeUnit.MILLISECONDS
    );
  }

  private Worker getLeastCapacityWorker()
  {
    final MinMaxPriorityQueue<Worker> workerQueue = MinMaxPriorityQueue.<Worker>orderedBy(
        new Comparator<Worker>()
        {
          @Override
          public int compare(Worker w1, Worker w2)
          {
            return Ints.compare(w1.getTasks().size(), w2.getTasks().size());
          }
        }
    ).create(taskInventoryManager.getInventory());

    if (workerQueue.isEmpty()) {
      log.error("No worker nodes found!");
      throw new RuntimeException();
    }

    return workerQueue.peek();
  }

  private boolean verifyStatusComplete(
      final ObjectMapper jsonMapper,
      final Worker worker,
      final Task task,
      final TaskCallback callback
  )
  {
    try {
      final String taskPath = JOINER.join(config.getTaskPath(), worker.getHost(), task.getId());
      final String statusPath = JOINER.join(config.getStatusPath(), worker.getHost(), task.getId());

      TaskStatus taskStatus = jsonMapper.readValue(
          cf.getData().forPath(statusPath), TaskStatus.class
      );

      if (taskStatus.isComplete()) {
        if (callback != null) {
          callback.notify(taskStatus);
        }

        cf.delete().guaranteed().forPath(statusPath);
        cf.delete().guaranteed().forPath(taskPath);

        return true;
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    return false;
  }

  /**
   * Creates a monitor for status updates and deletes. Worker nodes announce a status when they start a task and update
   * it again upon completing the task. If a status is deleted, this means the worker node has died before completing
   * its status update.
   */
  private void monitorStatus(
      final ObjectMapper jsonMapper,
      final Worker worker,
      final Task task,
      final TaskContext taskContext,
      final TaskCallback callback,
      final RetryPolicy retryPolicy
  ) throws Exception
  {
    final String taskPath = JOINER.join(config.getTaskPath(), worker.getHost(), task.getId());
    final String statusPath = JOINER.join(config.getStatusPath(), worker.getHost(), task.getId());

    PathChildrenCache monitor = monitors.get(worker.getHost());
    if (monitor == null) {
      monitor = new PathChildrenCache(
          cf,
          JOINER.join(config.getStatusPath(), worker.getHost()),
          false
      );
      monitor.start();
    }

    final PathChildrenCache statusMonitor = monitor;
    statusMonitor.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent)
              throws Exception
          {
            try {
              if (pathChildrenCacheEvent.getData().getPath().equals(statusPath)) {
                if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                  throw new ISE("Worker[%s] dropped Task[%s]!", worker.getHost(), task.getId());
                }

                TaskStatus taskStatus = jsonMapper.readValue(
                    cf.getData().forPath(statusPath), TaskStatus.class
                );

                if (taskStatus.isComplete()) {
                  if (callback != null) {
                    callback.notify(taskStatus);
                  }

                  cf.delete().guaranteed().forPath(statusPath);
                  cf.delete().guaranteed().forPath(taskPath);
                  statusMonitor.close();
                }
              }
            }
            catch (Exception e) {
              log.error(e, "Exception while cleaning up task[%s]. Retrying", task.getId());

              retryPolicy.registerRunnable(
                  new Runnable()
                  {
                    @Override
                    public void run()
                    {
                      try {
                        if (cf.checkExists().forPath(statusPath) != null) {
                          cf.delete().guaranteed().forPath(statusPath);
                        }
                        if (cf.checkExists().forPath(taskPath) != null) {
                          cf.delete().guaranteed().forPath(taskPath);
                        }
                        statusMonitor.close();
                      }
                      catch (Exception e) {
                        throw Throwables.propagate(e);
                      }
                    }
                  }
              );

              retryTask(task, taskContext, callback, retryPolicy);
            }
          }
        }
    );
  }

  private void announceTask(Worker theWorker, Task task, TaskContext taskContext)
  {
    try {
      log.info(
          "Coordinator asking Worker[%s] to add"
          + " task[%s]", theWorker.getHost(), task.getId()
      );

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
    }
    catch (Exception e) {
      log.error(e, "Exception creating task[%s] for worker node[%s]", task.getId(), theWorker.getHost());
      throw Throwables.propagate(e);
    }
  }
}
