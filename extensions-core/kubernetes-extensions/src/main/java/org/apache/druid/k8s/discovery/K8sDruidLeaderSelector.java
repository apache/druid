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

package org.apache.druid.k8s.discovery;

import org.apache.druid.annotations.SuppressFBWarnings;
import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.DruidNode;

import javax.annotation.Nullable;

public class K8sDruidLeaderSelector implements DruidLeaderSelector
{
  private static final EmittingLogger LOGGER = new EmittingLogger(K8sDruidLeaderSelector.class);

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  private DruidLeaderSelector.Listener listener = null;
  private final LeaderElectorAsyncWrapper leaderLatch;

  private volatile boolean leader = false;

  @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT", justification = "incremented but in single thread")
  private volatile int term = 0;

  public K8sDruidLeaderSelector(@Self DruidNode self, String lockResourceName, String lockResourceNamespace, K8sDiscoveryConfig discoveryConfig, K8sLeaderElectorFactory k8sLeaderElectorFactory)
  {
    this.leaderLatch = new LeaderElectorAsyncWrapper(
        self.getServiceScheme() + "://" + self.getHostAndPortToUse(),
        lockResourceName,
        lockResourceNamespace,
        discoveryConfig,
        k8sLeaderElectorFactory
    );
  }

  private void startLeaderElector(LeaderElectorAsyncWrapper leaderElector)
  {
    leaderElector.run(
        () -> {
          try {
            if (leader) {
              LOGGER.warn("I'm being asked to become leader. But I am already the leader. Ignored event.");
              return;
            }

            leader = true;
            term++;
            listener.becomeLeader();
          }
          catch (Throwable ex) {
            LOGGER.makeAlert(ex, "listener becomeLeader() failed. Unable to become leader").emit();

            CloseQuietly.close(leaderLatch);
            leader = false;
            //Exit and Kubernetes would simply create a new replacement pod.
            System.exit(1);
          }
        },
        () -> {
          try {
            if (!leader) {
              LOGGER.warn("I'm being asked to stop being leader. But I am not the leader. Ignored event.");
              return;
            }

            leader = false;
            listener.stopBeingLeader();
          }
          catch (Throwable ex) {
            LOGGER.makeAlert(ex, "listener.stopBeingLeader() failed. Unable to stopBeingLeader").emit();
          }
        }
    );
  }

  @Nullable
  @Override
  public String getCurrentLeader()
  {
    try {
      return leaderLatch.getCurrentLeader();
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
      startLeaderElector(leaderLatch);
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
    CloseQuietly.close(leaderLatch);
  }
}
