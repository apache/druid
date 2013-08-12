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

package com.metamx.druid.indexing.coordinator;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.curator.discovery.ServiceAnnouncer;
import com.metamx.druid.indexing.common.actions.TaskActionClient;
import com.metamx.druid.indexing.common.actions.TaskActionClientFactory;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.indexing.coordinator.exec.TaskConsumer;
import com.metamx.druid.indexing.coordinator.scaling.ResourceManagementScheduler;
import com.metamx.druid.indexing.coordinator.scaling.ResourceManagementSchedulerFactory;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.state.ConnectionState;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Encapsulates the indexer leadership lifecycle.
 */
public class TaskMasterLifecycle
{
  private final LeaderSelector leaderSelector;
  private final ReentrantLock giant = new ReentrantLock();
  private final Condition mayBeStopped = giant.newCondition();
  private final TaskQueue taskQueue;
  private final TaskActionClientFactory taskActionClientFactory;

  private volatile boolean leading = false;
  private volatile TaskRunner taskRunner;
  private volatile ResourceManagementScheduler resourceManagementScheduler;

  private static final EmittingLogger log = new EmittingLogger(TaskMasterLifecycle.class);

  public TaskMasterLifecycle(
      final TaskQueue taskQueue,
      final TaskActionClientFactory taskActionClientFactory,
      final IndexerCoordinatorConfig indexerCoordinatorConfig,
      final ServiceDiscoveryConfig serviceDiscoveryConfig,
      final TaskRunnerFactory runnerFactory,
      final ResourceManagementSchedulerFactory managementSchedulerFactory,
      final CuratorFramework curator,
      final ServiceAnnouncer serviceAnnouncer,
      final ServiceEmitter emitter
  )
  {
    this.taskQueue = taskQueue;
    this.taskActionClientFactory = taskActionClientFactory;

    this.leaderSelector = new LeaderSelector(
        curator, indexerCoordinatorConfig.getIndexerLeaderLatchPath(), new LeaderSelectorListener()
    {
      @Override
      public void takeLeadership(CuratorFramework client) throws Exception
      {
        giant.lock();

        try {
          log.info("By the power of Grayskull, I have the power!");

          taskRunner = runnerFactory.build();
          final TaskConsumer taskConsumer = new TaskConsumer(
              taskQueue,
              taskRunner,
              taskActionClientFactory,
              emitter
          );

          // Bootstrap task queue and task lockbox (load state stuff from the database)
          taskQueue.bootstrap();

          // Sensible order to start stuff:
          final Lifecycle leaderLifecycle = new Lifecycle();
          leaderLifecycle.addManagedInstance(taskRunner);
          leaderLifecycle.addHandler(
              new Lifecycle.Handler()
              {
                @Override
                public void start() throws Exception
                {
                  taskRunner.bootstrap(taskQueue.snapshot());
                }

                @Override
                public void stop()
                {

                }
              }
          );
          leaderLifecycle.addManagedInstance(taskQueue);
          Initialization.announceDefaultService(serviceDiscoveryConfig, serviceAnnouncer, leaderLifecycle);
          leaderLifecycle.addManagedInstance(taskConsumer);

          if ("remote".equalsIgnoreCase(indexerCoordinatorConfig.getRunnerImpl())) {
            if (!(taskRunner instanceof RemoteTaskRunner)) {
              throw new ISE("WTF?! We configured a remote runner and got %s", taskRunner.getClass());
            }
            resourceManagementScheduler = managementSchedulerFactory.build((RemoteTaskRunner) taskRunner);
            leaderLifecycle.addManagedInstance(resourceManagementScheduler);
          }

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
            leaderLifecycle.stop();
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

    leaderSelector.setId(indexerCoordinatorConfig.getServerName());
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
      return Optional.of(resourceManagementScheduler);
    } else {
      return Optional.absent();
    }
  }
}
