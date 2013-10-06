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

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.worker.config.WorkerConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;

import java.util.List;
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

                if (isTaskRunning(task)) {
                  log.warn(
                      "I can't build it. There's something in the way. Got task %s that I am already running...",
                      task.getId()
                  );
                  workerCuratorCoordinator.unannounceTask(task.getId());
                  return;
                }

                exec.submit(
                    new Runnable()
                    {
                      @Override
                      public void run()
                      {
                        final long startTime = System.currentTimeMillis();

                        log.info("Affirmative. Running task [%s]", task.getId());
                        running.add(task);

                        TaskStatus taskStatus;
                        try {
                          workerCuratorCoordinator.unannounceTask(task.getId());
                          workerCuratorCoordinator.announceTastAnnouncement(
                              TaskAnnouncement.create(
                                  task,
                                  TaskStatus.running(task.getId())
                              )
                          );
                          taskStatus = taskRunner.run(task).get();
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

  private boolean isTaskRunning(final Task task)
  {
    for (final Task runningTask : running) {
      if (runningTask.getId().equals(task.getId())) {
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
      exec.shutdown();
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception stopping WorkerTaskMonitor")
         .addData("exception", e.toString())
         .emit();
    }
  }
}
