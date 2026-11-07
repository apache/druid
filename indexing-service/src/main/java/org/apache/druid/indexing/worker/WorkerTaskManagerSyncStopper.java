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

package org.apache.druid.indexing.worker;

import com.google.inject.Inject;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;

/**
 * Lifecycle hook that calls {@link WorkerTaskManager#stopChangeHistory()}. This ends an in-flight sync request from
 * the Overlord, which would otherwise hold up Jetty server graceful shutdown.
 */
public class WorkerTaskManagerSyncStopper
{
  private final WorkerTaskManager workerTaskManager;

  @Inject
  public WorkerTaskManagerSyncStopper(final WorkerTaskManager workerTaskManager)
  {
    this.workerTaskManager = workerTaskManager;
  }

  /**
   * Calls {@link WorkerTaskManager#stopChangeHistory()}.
   */
  @LifecycleStop
  public void stop()
  {
    workerTaskManager.stopChangeHistory();
  }
}
