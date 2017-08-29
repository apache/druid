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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.indexing.overlord.helpers.OverlordHelperManager;
import io.druid.indexing.overlord.supervisor.SupervisorManager;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.server.DruidNode;
import io.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListener;
import org.apache.curator.framework.recipes.leader.Participant;
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
  private final ReentrantLock giant = new ReentrantLock(true);
  private final Condition mayBeStopped = giant.newCondition();
  private final TaskActionClientFactory taskActionClientFactory;
  private final SupervisorManager supervisorManager;

  private final AtomicReference<Lifecycle> leaderLifecycleRef = new AtomicReference<>(null);

  private volatile boolean leading = false;
  private volatile TaskRunner taskRunner;
  private volatile TaskQueue taskQueue;

  private static final EmittingLogger log = new EmittingLogger(TaskMaster.class);

  @Inject
  public TaskMaster(
      final TaskQueueConfig taskQueueConfig,
      final TaskLockbox taskLockbox,
      final TaskStorage taskStorage,
      final TaskActionClientFactory taskActionClientFactory,
      @Self final DruidNode selfNode,
      final IndexerZkConfig zkPaths,
      final TaskRunnerFactory runnerFactory,
      final CuratorFramework curator,
      final ServiceAnnouncer serviceAnnouncer,
      final CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig,
      final ServiceEmitter emitter,
      final SupervisorManager supervisorManager,
      final OverlordHelperManager overlordHelperManager
  )
  {
    this.supervisorManager = supervisorManager;
    this.taskActionClientFactory = taskActionClientFactory;

    final DruidNode node = coordinatorOverlordServiceConfig.getOverlordService() == null ? selfNode :
                           selfNode.withService(coordinatorOverlordServiceConfig.getOverlordService());

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
              leaderLifecycle.addManagedInstance(taskQueue);
              leaderLifecycle.addManagedInstance(supervisorManager);
              leaderLifecycle.addManagedInstance(overlordHelperManager);

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
                log.debug("Interrupted while waiting");
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

    leaderSelector.setId(node.getHostAndPortToUse());
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

  public boolean isLeader()
  {
    return leading;
  }

  public String getCurrentLeader()
  {
    try {
      final Participant leader = leaderSelector.getLeader();
      if (leader != null && leader.isLeader()) {
        return leader.getId();
      } else {
        return null;
      }
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

  public Optional<ScalingStats> getScalingStats()
  {
    if (leading) {
      return taskRunner.getScalingStats();
    } else {
      return Optional.absent();
    }
  }

  public Optional<SupervisorManager> getSupervisorManager()
  {
    if (leading) {
      return Optional.of(supervisorManager);
    } else {
      return Optional.absent();
    }
  }
}
