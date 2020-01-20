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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.curator.CuratorUtils;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;

/**
 * This class is deprecated and required only to support {@link org.apache.druid.indexing.overlord.RemoteTaskRunner}.
 * {@link org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner} should be used instead.
 *
 * The monitor watches ZK at a specified path for new tasks to appear. Upon starting the monitor, a listener will be
 * created that waits for new tasks. Tasks are executed as soon as they are seen.
 */
@Deprecated
public class WorkerTaskMonitor extends WorkerTaskManager
{
  private static final EmittingLogger log = new EmittingLogger(WorkerTaskMonitor.class);

  private final ObjectMapper jsonMapper;
  private final PathChildrenCache pathChildrenCache;
  private final CuratorFramework cf;
  private final WorkerCuratorCoordinator workerCuratorCoordinator;

  private final Object lifecycleLock = new Object();
  private volatile boolean started = false;

  @Inject
  public WorkerTaskMonitor(
      ObjectMapper jsonMapper,
      TaskRunner taskRunner,
      TaskConfig taskConfig,
      CuratorFramework cf,
      WorkerCuratorCoordinator workerCuratorCoordinator,
      @IndexingService DruidLeaderClient overlordClient
  )
  {
    super(jsonMapper, taskRunner, taskConfig, overlordClient);

    this.jsonMapper = jsonMapper;
    this.pathChildrenCache = new PathChildrenCache(
        cf,
        workerCuratorCoordinator.getTaskPathForWorker(),
        false,
        true,
        Execs.makeThreadFactory("TaskMonitorCache-%s")
    );
    this.cf = cf;
    this.workerCuratorCoordinator = workerCuratorCoordinator;
  }

  /**
   * Register a monitor for new tasks. When new tasks appear, the worker node announces a status to indicate it has
   * started the task. When the task is complete, the worker node updates the status.
   */
  @LifecycleStart
  @Override
  public void start() throws Exception
  {
    super.start();

    synchronized (lifecycleLock) {
      Preconditions.checkState(!started, "already started");
      started = true;

      try {
        cleanupStaleAnnouncements();
        registerRunListener();
        pathChildrenCache.start();

        log.debug("Started WorkerTaskMonitor.");
        started = true;
      }
      catch (InterruptedException e) {
        throw e;
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception starting WorkerTaskMonitor")
           .emit();
        throw e;
      }
    }
  }

  private void cleanupStaleAnnouncements() throws Exception
  {
    synchronized (lock) {
      // cleanup any old running task announcements which are invalid after restart
      for (TaskAnnouncement announcement : workerCuratorCoordinator.getAnnouncements()) {
        if (announcement.getTaskStatus().isRunnable()) {
          TaskStatus completionStatus = null;
          TaskAnnouncement completedAnnouncement = completedTasks.get(announcement.getTaskId());
          if (completedAnnouncement != null) {
            completionStatus = completedAnnouncement.getTaskStatus();
          } else if (!runningTasks.containsKey(announcement.getTaskStatus().getId())) {
            completionStatus = TaskStatus.failure(announcement.getTaskStatus().getId());
          }

          if (completionStatus != null) {
            log.info(
                "Cleaning up stale announcement for task [%s]. New status is [%s].",
                announcement.getTaskStatus().getId(),
                completionStatus.getStatusCode()
            );
            workerCuratorCoordinator.updateTaskStatusAnnouncement(
                TaskAnnouncement.create(
                    announcement.getTaskStatus().getId(),
                    announcement.getTaskType(),
                    announcement.getTaskResource(),
                    completionStatus,
                    TaskLocation.unknown(),
                    announcement.getTaskDataSource()
                )
            );
          }
        }
      }
    }
  }

  private void registerRunListener()
  {
    pathChildrenCache.getListenable().addListener(
        new PathChildrenCacheListener()
        {
          @Override
          public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent event)
              throws Exception
          {
            if (CuratorUtils.isChildAdded(event)) {
              final Task task = jsonMapper.readValue(
                  cf.getData().forPath(event.getData().getPath()),
                  Task.class
              );

              assignTask(task);
            }
          }
        }
    );
  }

  @LifecycleStop
  @Override
  public void stop() throws Exception
  {
    super.stop();

    synchronized (lifecycleLock) {
      Preconditions.checkState(started, "not started");

      try {
        started = false;
        pathChildrenCache.close();

        log.debug("Stopped WorkerTaskMonitor.");
      }
      catch (Exception e) {
        log.makeAlert(e, "Exception stopping WorkerTaskMonitor")
           .emit();
      }
    }
  }

  @Override
  protected void taskStarted(String taskId)
  {
    try {
      workerCuratorCoordinator.removeTaskRunZnode(taskId);
    }
    catch (Exception ex) {
      log.error(ex, "Unknown exception while deleting task[%s] znode.", taskId);
    }
  }

  @Override
  protected void taskAnnouncementChanged(TaskAnnouncement announcement)
  {
    try {
      workerCuratorCoordinator.updateTaskStatusAnnouncement(announcement);
    }
    catch (Exception ex) {
      log.makeAlert(ex, "Failed to update task announcement")
         .addData("task", announcement.getTaskId())
         .emit();
    }
  }
}
