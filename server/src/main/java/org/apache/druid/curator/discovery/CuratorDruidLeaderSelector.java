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

package org.apache.druid.curator.discovery;

import com.google.common.base.Preconditions;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.DruidNode;
import org.apache.druid.utils.CloseableUtils;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class CuratorDruidLeaderSelector implements DruidLeaderSelector
{
  private static final EmittingLogger log = new EmittingLogger(CuratorDruidLeaderSelector.class);

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final String selfId;
  private final CuratorFramework curator;
  private final String latchPath;

  private ExecutorService listenerExecutor;

  private DruidLeaderSelector.Listener listener = null;
  private final AtomicReference<LeaderLatch> leaderLatch = new AtomicReference<>();
  private final AtomicInteger latchIdCounter = new AtomicInteger(0);

  private volatile boolean leader = false;
  private volatile int term = 0;

  public CuratorDruidLeaderSelector(CuratorFramework curator, @Self DruidNode self, String latchPath)
  {
    this.curator = curator;
    this.selfId = self.getServiceScheme() + "://" + self.getHostAndPortToUse();
    this.latchPath = latchPath;

    // Creating a LeaderLatch here allows us to query for the current leader. We will not be considered for leadership
    // election until LeaderLatch.start() is called in registerListener(). This allows clients to observe the current
    // leader without being involved in the election.
    this.leaderLatch.set(createNewLeaderLatch());
  }

  private LeaderLatch createNewLeaderLatch()
  {
    return new LeaderLatch(curator, latchPath, selfId);
  }

  private LeaderLatch createNewLeaderLatchWithListener()
  {
    final LeaderLatch newLeaderLatch = createNewLeaderLatch();
    final int latchId = latchIdCounter.incrementAndGet();

    newLeaderLatch.addListener(
        new LeaderLatchListener()
        {
          @Override
          public void isLeader()
          {
            try {
              if (newLeaderLatch.getState() == LeaderLatch.State.CLOSED) {
                log.warn("[%s][%d] Ignoring request to become leader as latch is already closed.", selfId, latchId);
                return;
              } else if (leader) {
                log.warn("[%s][%d] I'm being asked to become leader. But I am already the leader. Ignored event.", selfId, latchId);
                return;
              } else {
                log.info("[%s][%d] I am now the leader. Latch state[%s]", selfId, latchId, newLeaderLatch.getState());
              }

              leader = true;
              term++;
              listener.becomeLeader();
            }
            catch (Exception ex) {
              log.makeAlert(ex, "[%s] listener becomeLeader() failed. Unable to become leader", selfId).emit();
              giveUpLeadershipAndRecreateLatch(-1);
            }
          }

          @Override
          public void notLeader()
          {
            try {
              if (!leader) {
                log.warn("[%s][%d] I'm being asked to stop being leader. But I am not the leader. Ignored event.", selfId, latchId);
                return;
              } else {
                log.info("[%s][%d] Giving up leadership", selfId, latchId);
              }

              leader = false;
              listener.stopBeingLeader();

              giveUpLeadershipAndRecreateLatch(latchId);
            }
            catch (Exception ex) {
              log.makeAlert(ex, "[%s] listener.stopBeingLeader() failed. Unable to stopBeingLeader", selfId).emit();
            }
          }
        },
        listenerExecutor
    );

    return leaderLatch.getAndSet(newLeaderLatch);
  }

  /**
   * Recreates the leader latch and allows other nodes to become leader.
   */
  private void giveUpLeadershipAndRecreateLatch(int latchId)
  {
    // give others a chance to become leader.
    log.info("[%s][%d] Recreating leader latch to allow other nodes to become leader.", selfId, latchId);
    final LeaderLatch oldLatch = createNewLeaderLatchWithListener();
    CloseableUtils.closeAndSuppressExceptions(
        oldLatch,
        e -> log.warn("[%s] Could not close old leader latch; continuing with new one anyway.", selfId)
    );

    leader = false;
    try {
      // Small delay before starting the latch so that others waiting are chosen to become leader.
      Thread.sleep(20000);
      log.info("[%s][%d] Now starting the latch", selfId, latchId);
      leaderLatch.get().start();
    }
    catch (Exception e) {
      // An exception can be caught here if interrupted while sleeping or if start() fails.
      // If that happens, the node will zombie out as it won't be looking for the latch anymore.
      log.makeAlert(e, "[%s] I am a zombie", selfId).emit();
    }
  }

  @Nullable
  @Override
  public String getCurrentLeader()
  {
    try {
      final LeaderLatch latch = leaderLatch.get();

      Participant participant = latch.getLeader();
      if (participant.isLeader()) {
        return participant.getId();
      }

      return null;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isLeader()
  {
    return leader;
  }

  @Override
  public int localTerm()
  {
    return term;
  }

  @Override
  public void registerListener(DruidLeaderSelector.Listener listener)
  {
    Preconditions.checkArgument(listener != null, "listener is null.");

    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }
    try {
      this.listener = listener;
      this.listenerExecutor = Execs.singleThreaded(
          StringUtils.format(
              "LeaderSelector[%s]",
              StringUtils.encodeForFormat(latchPath)
          )
      );

      createNewLeaderLatchWithListener();
      leaderLatch.get().start();

      lifecycleLock.started();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @Override
  public void unregisterListener()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    CloseableUtils.closeAndSuppressExceptions(leaderLatch.get(), e -> log.warn(e, "Failed to close LeaderLatch."));
    listenerExecutor.shutdownNow();
  }
}
