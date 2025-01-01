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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.client.indexing.IndexingService;
import org.apache.druid.curator.discovery.ServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.actions.SegmentAllocationQueue;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.TaskContextEnricher;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.duty.OverlordDutyExecutor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Encapsulates the leadership lifecycle of the Druid Overlord service.
 * No classes other than Resource endpoints should have this class as a dependency.
 * To query the current state of the Overlord, use {@link TaskMaster} instead.
 */
public class DruidOverlord
{
  private static final EmittingLogger log = new EmittingLogger(DruidOverlord.class);

  private final DruidLeaderSelector overlordLeaderSelector;
  private final DruidLeaderSelector.Listener leadershipListener;

  private final ReentrantLock giant = new ReentrantLock(true);

  private final AtomicReference<Lifecycle> leaderLifecycleRef = new AtomicReference<>(null);

  /**
   * Indicates that all services have been started and the node can now announce
   * itself with {@link ServiceAnnouncer#announce}. This must be set to false
   * as soon as {@link DruidLeaderSelector.Listener#stopBeingLeader()} is
   * called.
   */
  private volatile boolean initialized;

  @Inject
  public DruidOverlord(
      final TaskMaster taskMaster,
      final TaskLockConfig taskLockConfig,
      final TaskQueueConfig taskQueueConfig,
      final DefaultTaskConfig defaultTaskConfig,
      final TaskLockbox taskLockbox,
      final TaskStorage taskStorage,
      final TaskActionClientFactory taskActionClientFactory,
      @Self final DruidNode selfNode,
      final TaskRunnerFactory runnerFactory,
      final ServiceAnnouncer serviceAnnouncer,
      final CoordinatorOverlordServiceConfig coordinatorOverlordServiceConfig,
      final ServiceEmitter emitter,
      final SupervisorManager supervisorManager,
      final OverlordDutyExecutor overlordDutyExecutor,
      @IndexingService final DruidLeaderSelector overlordLeaderSelector,
      final SegmentAllocationQueue segmentAllocationQueue,
      final CompactionScheduler compactionScheduler,
      final ObjectMapper mapper,
      final TaskContextEnricher taskContextEnricher
  )
  {
    this.overlordLeaderSelector = overlordLeaderSelector;

    final DruidNode node = coordinatorOverlordServiceConfig.getOverlordService() == null ? selfNode :
                           selfNode.withService(coordinatorOverlordServiceConfig.getOverlordService());

    this.leadershipListener = new DruidLeaderSelector.Listener()
    {
      @Override
      public void becomeLeader()
      {
        giant.lock();

        // I AM THE MASTER OF THE UNIVERSE.
        log.info("By the power of Grayskull, I have the power. I am the leader");

        try {
          final TaskRunner taskRunner = runnerFactory.build();
          final TaskQueue taskQueue = new TaskQueue(
              taskLockConfig,
              taskQueueConfig,
              defaultTaskConfig,
              taskStorage,
              taskRunner,
              taskActionClientFactory,
              taskLockbox,
              emitter,
              mapper,
              taskContextEnricher
          );

          // Sensible order to start stuff:
          final Lifecycle leaderLifecycle = new Lifecycle("task-master");
          if (leaderLifecycleRef.getAndSet(leaderLifecycle) != null) {
            log.makeAlert("TaskMaster set a new Lifecycle without the old one being cleared!  Race condition")
               .emit();
          }

          // First add "half leader" services: everything required for APIs except the supervisor manager.
          // Then, become "half leader" so those APIs light up and supervisor initialization can proceed.
          leaderLifecycle.addManagedInstance(taskRunner);
          leaderLifecycle.addManagedInstance(taskQueue);
          leaderLifecycle.addHandler(
              new Lifecycle.Handler() {
                @Override
                public void start()
                {
                  segmentAllocationQueue.becomeLeader();
                  taskMaster.becomeHalfLeader(taskRunner, taskQueue);
                }

                @Override
                public void stop()
                {
                  taskMaster.stopBeingLeader();
                  segmentAllocationQueue.stopBeingLeader();
                }
              }
          );
          leaderLifecycle.addManagedInstance(supervisorManager);
          leaderLifecycle.addManagedInstance(overlordDutyExecutor);
          leaderLifecycle.addHandler(
              new Lifecycle.Handler()
              {
                @Override
                public void start()
                {
                  taskMaster.becomeFullLeader();
                  compactionScheduler.start();

                  // Announce the node only after all the services have been initialized
                  initialized = true;
                  serviceAnnouncer.announce(node);
                }

                @Override
                public void stop()
                {
                  serviceAnnouncer.unannounce(node);
                  compactionScheduler.stop();
                  taskMaster.downgradeToHalfLeader();
                }
              }
          );

          leaderLifecycle.start();
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
        finally {
          giant.unlock();
        }
      }

      @Override
      public void stopBeingLeader()
      {
        giant.lock();
        try {
          initialized = false;
          log.info("I am no longer the leader...");
          final Lifecycle leaderLifecycle = leaderLifecycleRef.getAndSet(null);

          if (leaderLifecycle != null) {
            leaderLifecycle.stop();
          }
        }
        finally {
          giant.unlock();
        }
      }
    };
  }

  /**
   * Starts waiting for leadership.
   * Should be called only once throughout the life of the service.
   */
  @LifecycleStart
  public void start()
  {
    giant.lock();

    try {
      overlordLeaderSelector.registerListener(leadershipListener);
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * Stops forever (not just this particular leadership session).
   * Should be called only once throughout the life of the service.
   */
  @LifecycleStop
  public void stop()
  {
    giant.lock();

    try {
      gracefulStopLeaderLifecycle();
      overlordLeaderSelector.unregisterListener();
    }
    finally {
      giant.unlock();
    }
  }

  /**
   * @return true if it's the leader and all its services have been initialized.
   */
  public boolean isLeader()
  {
    return overlordLeaderSelector.isLeader() && initialized;
  }

  public String getCurrentLeader()
  {
    return overlordLeaderSelector.getCurrentLeader();
  }

  public Optional<String> getRedirectLocation()
  {
    String leader = overlordLeaderSelector.getCurrentLeader();
    // do not redirect when
    // leader is not elected
    // leader is the current node
    if (leader == null || leader.isEmpty() || overlordLeaderSelector.isLeader()) {
      return Optional.absent();
    } else {
      return Optional.of(leader);
    }
  }

  private void gracefulStopLeaderLifecycle()
  {
    try {
      if (isLeader()) {
        leadershipListener.stopBeingLeader();
      }
    }
    catch (Exception ex) {
      // fail silently since we are stopping anyway
    }
  }

}
