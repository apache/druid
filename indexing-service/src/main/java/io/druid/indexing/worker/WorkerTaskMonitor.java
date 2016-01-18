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

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.worker.config.WorkerConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.listen.Listenable;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * The monitor watches ZK at a specified path for new tasks to appear. Upon starting the monitor, a listener will be
 * created that waits for new tasks. Tasks are executed as soon as they are seen.
 * <p/>
 * The monitor implements {@link io.druid.query.QuerySegmentWalker} so tasks can offer up queryable data. This is useful for
 * realtime index tasks.
 */
public class WorkerTaskMonitor
{
  private static final EmittingLogger log = new EmittingLogger(WorkerTaskMonitor.class);

  private final ObjectMapper jsonMapper;
  private final PathChildrenCache pathChildrenCache;
  private final CuratorFramework cf;
  private final WorkerCuratorCoordinator workerCuratorCoordinator;
  private final TaskRunner taskRunner;
  private final ExecutorService exec;
  private final List<Task> running = new CopyOnWriteArrayList<Task>();

  @Inject
  public WorkerTaskMonitor(
      ObjectMapper jsonMapper,
      CuratorFramework cf,
      WorkerCuratorCoordinator workerCuratorCoordinator,
      TaskRunner taskRunner,
      WorkerConfig workerConfig
  )
  {
    this.jsonMapper = jsonMapper;
    this.pathChildrenCache = new PathChildrenCache(
        cf, workerCuratorCoordinator.getTaskPathForWorker(), false, true, Execs.makeThreadFactory("TaskMonitorCache-%s")
    );
    this.cf = cf;
    this.workerCuratorCoordinator = workerCuratorCoordinator;
    this.taskRunner = taskRunner;

    this.exec = Execs.multiThreaded(workerConfig.getCapacity(), "WorkerTaskMonitor-%d");
  }

  /**
   * Register a monitor for new tasks. When new tasks appear, the worker node announces a status to indicate it has
   * started the task. When the task is complete, the worker node updates the status. It is up to the coordinator to
   * determine how many tasks are sent to each worker node and cleanup tasks and statuses in ZK accordingly.
   */
  @LifecycleStart
  public void start()
  {
    try {
      // restore restorable tasks
      final List<Pair<Task, ListenableFuture<TaskStatus>>> restored = taskRunner.restore();
      for (Pair<Task, ListenableFuture<TaskStatus>> pair : restored) {
        submitTaskRunnable(pair.lhs, pair.rhs);
      }

      // cleanup any old running task announcements which are invalid after restart
      for (TaskAnnouncement announcement : workerCuratorCoordinator.getAnnouncements()) {
        if (!isTaskRunning(announcement.getTaskStatus().getId()) && announcement.getTaskStatus().isRunnable()) {
          workerCuratorCoordinator.updateAnnouncement(
              TaskAnnouncement.create(
                  announcement.getTaskStatus().getId(),
                  announcement.getTaskResource(),
                  TaskStatus.failure(announcement.getTaskStatus().getId())
              )
          );
        }
      }

      pathChildrenCache.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent)
                throws Exception
            {
              if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                final Task task = jsonMapper.readValue(
                    cf.getData().forPath(pathChildrenCacheEvent.getData().getPath()),
                    Task.class
                );

                submitTaskRunnable(task, null);
              }
            }
          }
      );
      pathChildrenCache.start();
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception starting WorkerTaskMonitor")
         .addData("exception", e.toString())
         .emit();
    }
  }

  private void submitTaskRunnable(final Task task, final ListenableFuture<TaskStatus> taskStatusAlreadySubmitted)
  {
    if (isTaskRunning(task.getId())) {
      log.warn(
          "I can't build it. There's something in the way. Got task %s that I am already running...",
          task.getId()
      );
      workerCuratorCoordinator.unannounceTask(task.getId());
      return;
    }

    log.info("Submitting runnable for task[%s]", task.getId());

    running.add(task);

    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            final long startTime = System.currentTimeMillis();

            TaskStatus taskStatus;

            try {
              workerCuratorCoordinator.updateAnnouncement(
                  TaskAnnouncement.create(
                      task,
                      TaskStatus.running(task.getId())
                  )
              );

              if (taskStatusAlreadySubmitted != null) {
                log.info("Affirmative. Connecting to already-running task [%s]", task.getId());
                taskStatus = taskStatusAlreadySubmitted.get();
              } else {
                log.info("Affirmative. Running task [%s]", task.getId());
                workerCuratorCoordinator.unannounceTask(task.getId());
                taskStatus = taskRunner.run(task).get();
              }
            }
            catch (InterruptedException e) {
              log.debug(e, "Interrupted while running task[%s], exiting.", task.getId());
              return;
            }
            catch (Exception e) {
              log.makeAlert(e, "I can't build there. Failed to run task")
                 .addData("task", task.getId())
                 .emit();
              taskStatus = TaskStatus.failure(task.getId());
            }
            finally {
              running.remove(task);
            }

            taskStatus = taskStatus.withDuration(System.currentTimeMillis() - startTime);

            try {
              workerCuratorCoordinator.updateAnnouncement(TaskAnnouncement.create(task, taskStatus));
              log.info(
                  "Job's finished. Completed [%s] with status [%s]",
                  task.getId(),
                  taskStatus.getStatusCode()
              );
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to update task status")
                 .addData("task", task.getId())
                 .emit();
            }
          }
        }
    );
  }

  private boolean isTaskRunning(final String taskId)
  {
    for (final Task runningTask : running) {
      if (runningTask.getId().equals(taskId)) {
        return true;
      }
    }

    return false;
  }

  @LifecycleStop
  public void stop()
  {
    try {
      pathChildrenCache.close();
      exec.shutdownNow();
      taskRunner.stop();
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception stopping WorkerTaskMonitor")
         .addData("exception", e.toString())
         .emit();
    }
  }
}
