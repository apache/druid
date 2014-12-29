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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.concurrent.ScheduledExecutors;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.indexing.overlord.autoscaling.ResourceManagementScheduler;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerFactory;
import io.druid.server.DruidNode;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Encapsulates the indexer leadership lifecycle.
 */
public class TaskMaster
{
  private final LeaderSelector leaderSelector;
  private final ReentrantLock giant = new ReentrantLock();
  private final Condition mayBeStopped = giant.newCondition();
  private final TaskActionClientFactory taskActionClientFactory;

  private final AtomicReference<Lifecycle> leaderLifecycleRef = new AtomicReference<>(null);

  private volatile boolean leading = false;
  private volatile TaskRunner taskRunner;
  private volatile TaskQueue taskQueue;
  private volatile ResourceManagementScheduler resourceManagementScheduler;

  private static final EmittingLogger log = new EmittingLogger(TaskMaster.class);

  @Inject
  public TaskMaster(
      final TaskQueueConfig taskQueueConfig,
      final TaskLockbox taskLockbox,
      final TaskStorage taskStorage,
      final TaskActionClientFactory taskActionClientFactory,
      @Self final DruidNode node,
      final IndexerZkConfig zkPaths,
      final TaskRunnerFactory runnerFactory,
      final ResourceManagementSchedulerFactory managementSchedulerFactory,
      final CuratorFramework curator,
      final ServiceAnnouncer serviceAnnouncer,
      final ServiceEmitter emitter
  )
  {
    this.taskActionClientFactory = taskActionClientFactory;
    this.leaderSelector = new LeaderSelector(
        curator,
        zkPaths.getLeaderLatchPath(),
        new LeaderSelectorListener()
        {
          @Override
          public void takeLeadership(CuratorFramework client) throws Exception
          {
            giant.lock();

            try {
              // Make sure the previous leadership cycle is really, really over.
              stopLeading();

              // I AM THE MASTER OF THE UNIVERSE.
              log.info("By the power of Grayskull, I have the power!");
              taskLockbox.syncFromStorage();
              taskRunner = runnerFactory.build();
              taskQueue = new TaskQueue(
                  taskQueueConfig,
                  taskStorage,
                  taskRunner,
                  taskActionClientFactory,
                  taskLockbox,
                  emitter
              );

              // Sensible order to start stuff:
              final Lifecycle leaderLifecycle = new Lifecycle();
              if (leaderLifecycleRef.getAndSet(leaderLifecycle) != null) {
                log.makeAlert("TaskMaster set a new Lifecycle without the old one being cleared!  Race condition")
                   .emit();
              }
              leaderLifecycle.addManagedInstance(taskRunner);
              if (taskRunner instanceof RemoteTaskRunner) {
                final ScheduledExecutorFactory executorFactory = ScheduledExecutors.createFactory(leaderLifecycle);
                resourceManagementScheduler = managementSchedulerFactory.build(
                    (RemoteTaskRunner) taskRunner,
                    executorFactory
                );
                leaderLifecycle.addManagedInstance(resourceManagementScheduler);
              }
              leaderLifecycle.addManagedInstance(taskQueue);
              leaderLifecycle.addHandler(
                  new Lifecycle.Handler()
                  {
                    @Override
                    public void start() throws Exception
                    {
                      serviceAnnouncer.announce(node);
                    }

                    @Override
                    public void stop()
                    {
                      serviceAnnouncer.unannounce(node);
                    }
                  }
              );
              try {
                leaderLifecycle.start();
                leading = true;
                while (leading && !Thread.currentThread().isInterrupted()) {
                  mayBeStopped.await();
                }
              }
              catch (InterruptedException e) {
                // Suppress so we can bow out gracefully
              }
              finally {
                log.info("Bowing out!");
                stopLeading();
              }
            }
            catch (Exception e) {
              log.makeAlert(e, "Failed to lead").emit();
              throw Throwables.propagate(e);
            }
            finally {
              giant.unlock();
            }
          }

          @Override
          public void stateChanged(CuratorFramework client, ConnectionState newState)
          {
            if (newState == ConnectionState.LOST || newState == ConnectionState.SUSPENDED) {
              // disconnected from zk. assume leadership is gone
              stopLeading();
            }
          }
        }
    );

    leaderSelector.setId(node.getHostAndPort());
    leaderSelector.autoRequeue();
  }

  /**
   * Starts waiting for leadership. Should only be called once throughout the life of the program.
   */
  @LifecycleStart
  public void start()
  {
    giant.lock();

    try {
      leaderSelector.start();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Stops forever (not just this particular leadership session). Should only be called once throughout the life of
   * the program.
   */
  @LifecycleStop
  public void stop()
  {
    giant.lock();

    try {
      leaderSelector.close();
      stopLeading();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Relinquish leadership. May be called multiple times, even when not currently the leader.
   */
  private void stopLeading()
  {
    giant.lock();

    try {
      if (leading) {
        leading = false;
        mayBeStopped.signalAll();
        final Lifecycle leaderLifecycle = leaderLifecycleRef.getAndSet(null);
        if (leaderLifecycle != null) {
          leaderLifecycle.stop();
        }
      }
    }
    finally {
      giant.unlock();
    }
  }

  public boolean isLeading()
  {
    return leading;
  }

  public String getLeader()
  {
    try {
      return leaderSelector.getLeader().getId();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public Optional<TaskRunner> getTaskRunner()
  {
    if (leading) {
      return Optional.of(taskRunner);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskQueue> getTaskQueue()
  {
    if (leading) {
      return Optional.of(taskQueue);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskActionClient> getTaskActionClient(Task task)
  {
    if (leading) {
      return Optional.of(taskActionClientFactory.create(task));
    } else {
      return Optional.absent();
    }
  }

  public Optional<ResourceManagementScheduler> getResourceManagementScheduler()
  {
    if (leading) {
      return Optional.fromNullable(resourceManagementScheduler);
    } else {
      return Optional.absent();
    }
  }
}
