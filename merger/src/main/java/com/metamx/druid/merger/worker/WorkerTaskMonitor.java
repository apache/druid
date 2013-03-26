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

package com.metamx.druid.merger.worker;

import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.Query;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.TaskToolboxFactory;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.query.NoopQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import com.metamx.druid.query.segment.SegmentDescriptor;
import com.metamx.emitter.EmittingLogger;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheEvent;
import com.netflix.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;

import java.io.File;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;

/**
 * The monitor watches ZK at a specified path for new tasks to appear. Upon starting the monitor, a listener will be
 * created that waits for new tasks. Tasks are executed as soon as they are seen.
 *
 * The monitor implements {@link QuerySegmentWalker} so tasks can offer up queryable data. This is useful for
 * realtime index tasks.
 */
public class WorkerTaskMonitor implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(WorkerTaskMonitor.class);

  private final PathChildrenCache pathChildrenCache;
  private final CuratorFramework cf;
  private final WorkerCuratorCoordinator workerCuratorCoordinator;
  private final TaskToolboxFactory toolboxFactory;
  private final ExecutorService exec;
  private final List<Task> running = new CopyOnWriteArrayList<Task>();

  public WorkerTaskMonitor(
      PathChildrenCache pathChildrenCache,
      CuratorFramework cf,
      WorkerCuratorCoordinator workerCuratorCoordinator,
      TaskToolboxFactory toolboxFactory,
      ExecutorService exec
  )
  {
    this.pathChildrenCache = pathChildrenCache;
    this.cf = cf;
    this.workerCuratorCoordinator = workerCuratorCoordinator;
    this.toolboxFactory = toolboxFactory;
    this.exec = exec;
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
      pathChildrenCache.start();
      pathChildrenCache.getListenable().addListener(
          new PathChildrenCacheListener()
          {
            @Override
            public void childEvent(CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent)
                throws Exception
            {
              if (pathChildrenCacheEvent.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                final Task task = toolboxFactory.getObjectMapper().readValue(
                    cf.getData().forPath(pathChildrenCacheEvent.getData().getPath()),
                    Task.class
                );
                final TaskToolbox toolbox = toolboxFactory.build(task);

                if (isTaskRunning(task)) {
                  log.warn("Got task %s that I am already running...", task.getId());
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
                        final File taskDir = toolbox.getTaskDir();

                        log.info("Running task [%s]", task.getId());
                        running.add(task);

                        TaskStatus taskStatus;
                        try {
                          workerCuratorCoordinator.unannounceTask(task.getId());
                          workerCuratorCoordinator.announceStatus(TaskStatus.running(task.getId()));
                          taskStatus = task.run(toolbox);
                        }
                        catch (Exception e) {
                          log.makeAlert(e, "Failed to run task")
                             .addData("task", task.getId())
                             .emit();
                          taskStatus = TaskStatus.failure(task.getId());
                        } finally {
                          running.remove(task);
                        }

                        taskStatus = taskStatus.withDuration(System.currentTimeMillis() - startTime);

                        try {
                          workerCuratorCoordinator.updateStatus(taskStatus);
                          log.info("Completed task [%s] with status [%s]", task.getId(), taskStatus.getStatusCode());
                        }
                        catch (Exception e) {
                          log.makeAlert(e, "Failed to update task status")
                             .addData("task", task.getId())
                             .emit();
                        }

                        try {
                          if (taskDir.exists()) {
                            log.info("Removing task directory: %s", taskDir);
                            FileUtils.deleteDirectory(taskDir);
                          }
                        }
                        catch (Exception e) {
                          log.makeAlert(e, "Failed to delete task directory")
                             .addData("taskDir", taskDir.toString())
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

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return getQueryRunnerImpl(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return getQueryRunnerImpl(query);
  }

  private <T> QueryRunner<T> getQueryRunnerImpl(Query<T> query) {
    QueryRunner<T> queryRunner = null;

    for (final Task task : running) {
      if (task.getDataSource().equals(query.getDataSource())) {
        final QueryRunner<T> taskQueryRunner = task.getQueryRunner(query);

        if (taskQueryRunner != null) {
          if (queryRunner == null) {
            queryRunner = taskQueryRunner;
          } else {
            log.makeAlert("Found too many query runners for datasource")
               .addData("dataSource", query.getDataSource())
               .emit();
          }
        }
      }
    }

    return queryRunner == null ? new NoopQueryRunner<T>() : queryRunner;
  }
}
