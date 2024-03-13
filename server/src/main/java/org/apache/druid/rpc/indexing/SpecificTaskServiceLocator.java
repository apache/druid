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

package org.apache.druid.rpc.indexing;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceLocator;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Service locator for a specific task. Uses the {@link OverlordClient#taskStatuses(Set)} API to locate tasks.
 *
 * This locator has an internal cache that is updated if the last check has been over {@link #LOCATION_CACHE_MS} ago.
 *
 * This locator is Closeable, like all ServiceLocators, but it is not essential that you actually close it. Closing
 * does not free any resources: it merely makes future calls to {@link #locate()} return
 * {@link ServiceLocations#closed()}.
 */
public class SpecificTaskServiceLocator implements ServiceLocator
{
  private static final String BASE_PATH = "/druid/worker/v1/chat";
  private static final long LOCATION_CACHE_MS = 30_000;

  private final String taskId;
  private final OverlordClient overlordClient;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private TaskState lastKnownState = TaskState.RUNNING; // Assume task starts out running.

  @GuardedBy("lock")
  private ServiceLocation lastKnownLocation;

  @GuardedBy("lock")
  private boolean closed = false;

  @GuardedBy("lock")
  private long lastUpdateTime = -1;

  @GuardedBy("lock")
  private SettableFuture<ServiceLocations> pendingFuture = null;

  public SpecificTaskServiceLocator(final String taskId, final OverlordClient overlordClient)
  {
    this.taskId = taskId;
    this.overlordClient = overlordClient;
  }

  @Override
  public ListenableFuture<ServiceLocations> locate()
  {
    synchronized (lock) {
      if (pendingFuture != null) {
        return Futures.nonCancellationPropagating(pendingFuture);
      } else if (closed || lastKnownState != TaskState.RUNNING) {
        return Futures.immediateFuture(ServiceLocations.closed());
      } else if (lastKnownLocation == null || lastUpdateTime + LOCATION_CACHE_MS < System.currentTimeMillis()) {
        final ListenableFuture<Map<String, TaskStatus>> taskStatusFuture;

        try {
          taskStatusFuture = overlordClient.taskStatuses(ImmutableSet.of(taskId));
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }

        // Use shared future for concurrent calls to "locate"; don't want multiple calls out to the Overlord at once.
        // Alias pendingFuture to retVal in case taskStatusFuture is already resolved. (This will make the callback
        // below execute immediately, firing and nulling out pendingFuture.)
        final SettableFuture<ServiceLocations> retVal = (pendingFuture = SettableFuture.create());
        pendingFuture.addListener(
            () -> {
              if (!taskStatusFuture.isDone()) {
                // pendingFuture may resolve without taskStatusFuture due to close().
                taskStatusFuture.cancel(true);
              }
            },
            Execs.directExecutor()
        );

        Futures.addCallback(
            taskStatusFuture,
            new FutureCallback<Map<String, TaskStatus>>()
            {
              @Override
              public void onSuccess(final Map<String, TaskStatus> taskStatusMap)
              {
                synchronized (lock) {
                  if (pendingFuture != null) {
                    lastUpdateTime = System.currentTimeMillis();

                    final TaskStatus status = taskStatusMap.get(taskId);

                    if (status == null) {
                      // If the task status is unknown, we'll treat it as closed.
                      lastKnownState = null;
                      lastKnownLocation = null;
                    } else {
                      lastKnownState = status.getStatusCode();

                      if (TaskLocation.unknown().equals(status.getLocation())) {
                        lastKnownLocation = null;
                      } else {
                        lastKnownLocation = new ServiceLocation(
                            status.getLocation().getHost(),
                            status.getLocation().getPort(),
                            status.getLocation().getTlsPort(),
                            StringUtils.format("%s/%s", BASE_PATH, StringUtils.urlEncode(taskId))
                        );
                      }
                    }

                    if (lastKnownState != TaskState.RUNNING) {
                      pendingFuture.set(ServiceLocations.closed());
                    } else if (lastKnownLocation == null) {
                      pendingFuture.set(ServiceLocations.forLocations(Collections.emptySet()));
                    } else {
                      pendingFuture.set(ServiceLocations.forLocation(lastKnownLocation));
                    }

                    // Clear pendingFuture once it has been set.
                    pendingFuture = null;
                  }
                }
              }

              @Override
              public void onFailure(Throwable t)
              {
                synchronized (lock) {
                  if (pendingFuture != null) {
                    pendingFuture.setException(t);

                    // Clear pendingFuture once it has been set.
                    pendingFuture = null;
                  }
                }
              }
            },
            MoreExecutors.directExecutor()
        );

        return Futures.nonCancellationPropagating(retVal);
      } else {
        return Futures.immediateFuture(ServiceLocations.forLocation(lastKnownLocation));
      }
    }
  }

  @Override
  public void close()
  {
    // Class-level Javadocs promise that this method does not actually free resources: it only alters behavior
    // for future calls to locate(). This is exploited in TaskServiceClients.makeClient.

    synchronized (lock) {
      // Idempotent: can call close() multiple times so long as start() has already been called.
      if (!closed) {
        if (pendingFuture != null) {
          pendingFuture.set(ServiceLocations.closed());

          // Clear pendingFuture once it has been set.
          pendingFuture = null;
        }

        closed = true;
      }
    }
  }
}
