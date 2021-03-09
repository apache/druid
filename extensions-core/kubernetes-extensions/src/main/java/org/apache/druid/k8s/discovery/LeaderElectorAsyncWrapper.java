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

import org.apache.druid.com.google.common.base.Preconditions;
import org.apache.druid.concurrent.LifecycleLock;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class LeaderElectorAsyncWrapper implements Closeable
{
  private static final Logger LOGGER = new Logger(LeaderElectorAsyncWrapper.class);

  private ExecutorService executor;
  private final AtomicReference<Future> futureRef = new AtomicReference<>();

  private final K8sLeaderElector k8sLeaderElector;

  private final LifecycleLock lifecycleLock = new LifecycleLock();

  public LeaderElectorAsyncWrapper(
      String candidateId,
      String lockResourceName,
      String lockResourceNamespace,
      K8sDiscoveryConfig discoveryConfig,
      K8sLeaderElectorFactory k8sLeaderElectorFactory
  )
  {
    Preconditions.checkArgument(
        K8sDiscoveryConfig.K8S_RESOURCE_NAME_REGEX.matcher(lockResourceName).matches(),
        "lockResourceName[%s] must match regex[%s]",
        lockResourceName,
        K8sDiscoveryConfig.K8S_RESOURCE_NAME_REGEX.pattern()
    );
    LOGGER.info(
        "Creating LeaderElector with candidateId[%s], lockResourceName[%s],  k8sNamespace[%s].",
        candidateId,
        lockResourceName,
        lockResourceNamespace
    );

    k8sLeaderElector = k8sLeaderElectorFactory.create(candidateId, lockResourceNamespace, lockResourceName);
  }

  public void run(Runnable startLeadingHook, Runnable stopLeadingHook)
  {
    if (!lifecycleLock.canStart()) {
      throw new ISE("can't start.");
    }

    try {
      executor = Execs.singleThreaded(this.getClass().getSimpleName());
      futureRef.set(executor.submit(
          () -> {
            while (lifecycleLock.awaitStarted(1, TimeUnit.MILLISECONDS)) {
              try {
                k8sLeaderElector.run(startLeadingHook, stopLeadingHook);
              }
              catch (Throwable ex) {
                LOGGER.error(ex, "Exception in K8s LeaderElector.run()");
              }
            }
          }
      ));
      lifecycleLock.started();
    }
    finally {
      lifecycleLock.exitStart();
    }
  }

  @Override
  public void close()
  {
    if (!lifecycleLock.canStop()) {
      throw new ISE("can't stop.");
    }

    try {
      futureRef.get().cancel(true);
      executor.shutdownNow();
      if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
        LOGGER.warn("Failed to terminate [%s] executor.", this.getClass().getSimpleName());
      }
    }
    catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }
  }

  public String getCurrentLeader()
  {
    return k8sLeaderElector.getCurrentLeader();
  }
}
