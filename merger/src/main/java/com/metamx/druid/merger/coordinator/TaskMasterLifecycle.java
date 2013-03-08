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

import com.google.common.base.Throwables;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.initialization.Initialization;
import com.metamx.druid.initialization.ServiceDiscoveryConfig;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.TaskToolboxFactory;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.merger.coordinator.exec.TaskConsumer;
import com.metamx.druid.merger.coordinator.scaling.ResourceManagementScheduler;
import com.metamx.druid.merger.coordinator.scaling.ResourceManagementSchedulerFactory;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.recipes.leader.LeaderSelector;
import com.netflix.curator.framework.recipes.leader.LeaderSelectorListener;
import com.netflix.curator.framework.state.ConnectionState;

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
  private final TaskToolboxFactory taskToolboxFactory;

  private volatile boolean leading = false;
  private volatile TaskRunner taskRunner;
  private volatile ResourceManagementScheduler resourceManagementScheduler;

  private static final EmittingLogger log = new EmittingLogger(TaskMasterLifecycle.class);

  public TaskMasterLifecycle(
      final TaskQueue taskQueue,
      final TaskToolboxFactory taskToolboxFactory,
      final IndexerCoordinatorConfig indexerCoordinatorConfig,
      final ServiceDiscoveryConfig serviceDiscoveryConfig,
      final TaskRunnerFactory runnerFactory,
      final ResourceManagementSchedulerFactory managementSchedulerFactory,
      final CuratorFramework curator,
      final ServiceEmitter emitter
  )
  {
    this.taskQueue = taskQueue;
    this.taskToolboxFactory = taskToolboxFactory;

    this.leaderSelector = new LeaderSelector(
        curator, indexerCoordinatorConfig.getLeaderLatchPath(), new LeaderSelectorListener()
    {
      @Override
      public void takeLeadership(CuratorFramework client) throws Exception
      {
        giant.lock();

        try {
          log.info("By the power of Grayskull, I have the power!");

          taskRunner = runnerFactory.build();
          resourceManagementScheduler = managementSchedulerFactory.build(taskRunner);
          final TaskConsumer taskConsumer = new TaskConsumer(
              taskQueue,
              taskRunner,
              taskToolboxFactory,
              emitter
          );

          // Bootstrap task queue and task lockbox (load state stuff from the database)
          taskQueue.bootstrap();

          // Sensible order to start stuff:
          final Lifecycle leaderLifecycle = new Lifecycle();
          leaderLifecycle.addManagedInstance(taskQueue);
          leaderLifecycle.addManagedInstance(taskRunner);
          Initialization.makeServiceDiscoveryClient(curator, serviceDiscoveryConfig, leaderLifecycle);
          leaderLifecycle.addManagedInstance(taskConsumer);
          leaderLifecycle.addManagedInstance(resourceManagementScheduler);

          leading = true;

          try {
            leaderLifecycle.start();

            while (leading) {
              mayBeStopped.await();
            }
          }
          finally {
            log.info("Bowing out!");
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

  public TaskRunner getTaskRunner()
  {
    return taskRunner;
  }

  public TaskQueue getTaskQueue()
  {
    return taskQueue;
  }

  public TaskToolbox getTaskToolbox()
  {
    return taskToolbox;
  }
}
