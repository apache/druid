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

package io.druid.curator.discovery;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.concurrent.LifecycleLock;
import io.druid.discovery.DruidLeaderSelector;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.server.DruidNode;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.leader.Participant;

import javax.annotation.Nullable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
public class CuratorDruidLeaderSelector implements DruidLeaderSelector
{
  private static final EmittingLogger log = new EmittingLogger(CuratorDruidLeaderSelector.class);

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private final DruidNode self;
  private final CuratorFramework curator;
  private final String latchPath;

  private ExecutorService listenerExecutor;

  private DruidLeaderSelector.Listener listener = null;
  private final AtomicReference<LeaderLatch> leaderLatch = new AtomicReference<>();

  private volatile boolean leader = false;
  private volatile int term = 0;

  public CuratorDruidLeaderSelector(CuratorFramework curator, @Self DruidNode self, String latchPath)
  {
    this.curator = curator;
    this.self = self;
    this.latchPath = latchPath;
  }

  private LeaderLatch createNewLeaderLatch()
  {
    final LeaderLatch newLeaderLatch = new LeaderLatch(
        curator, latchPath, self.getServiceScheme() + "://" + self.getHostAndPortToUse()
    );

    newLeaderLatch.addListener(
        new LeaderLatchListener()
        {
          @Override
          public void isLeader()
          {
            try {
              if (leader) {
                log.warn("I'm being asked to become leader. But I am already the leader. Ignored event.");
                return;
              }

              leader = true;
              term++;
              listener.becomeLeader();
            }
            catch (Exception ex) {
              log.makeAlert(ex, "listener becomeLeader() failed. Unable to become leader").emit();

              // give others a chance to become leader.
              final LeaderLatch oldLatch = createNewLeaderLatch();
              CloseQuietly.close(oldLatch);
              leader = false;
              try {
                //Small delay before starting the latch so that others waiting are chosen to become leader.
                Thread.sleep(ThreadLocalRandom.current().nextInt(1000, 5000));
                leaderLatch.get().start();
              }
              catch (Exception e) {
                // If an exception gets thrown out here, then the node will zombie out 'cause it won't be looking for
                // the latch anymore.  I don't believe it's actually possible for an Exception to throw out here, but
                // Curator likes to have "throws Exception" on methods so it might happen...
                log.makeAlert(e, "I am a zombie").emit();
              }
            }
          }

          @Override
          public void notLeader()
          {
            try {
              if (!leader) {
                log.warn("I'm being asked to stop being leader. But I am not the leader. Ignored event.");
                return;
              }

              leader = false;
              listener.stopBeingLeader();
            }
            catch (Exception ex) {
              log.makeAlert(ex, "listener.stopBeingLeader() failed. Unable to stopBeingLeader").emit();
            }
          }
        },
        listenerExecutor
    );

    return leaderLatch.getAndSet(newLeaderLatch);
  }

  @Nullable
  @Override
  public String getCurrentLeader()
  {
    if (!lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
      throw new ISE("not started");
    }

    try {
      final LeaderLatch latch = leaderLatch.get();

      Participant participant = latch.getLeader();
      if (participant.isLeader()) {
        return participant.getId();
      }

      return null;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
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
      this.listenerExecutor = Execs.singleThreaded(StringUtils.format("LeaderSelector[%s]", latchPath));

      createNewLeaderLatch();
      leaderLatch.get().start();

      lifecycleLock.started();
    }
    catch (Exception ex) {
      throw Throwables.propagate(ex);
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
    CloseQuietly.close(leaderLatch.get());
    listenerExecutor.shutdownNow();
  }
}
