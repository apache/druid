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

package org.apache.druid.msq.exec;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.msq.indexing.WorkerCount;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Used by {@link ControllerImpl} to discover and manage workers.
 *
 * Worker managers capable of retrying should extend {@link RetryCapableWorkerManager} (an extension of this interface).
 */
public interface WorkerManager
{
  int UNKNOWN_WORKER_NUMBER = -1;

  /**
   * Starts this manager.
   *
   * Returns a future that resolves successfully when all workers end successfully or are canceled. The returned future
   * resolves to an exception if one of the workers fails without being explicitly canceled, or if something else
   * goes wrong.
   */
  ListenableFuture<?> start();

  /**
   * Launch additional workers, if needed, to bring the number of running workers up to {@code workerCount}.
   * Blocks until the requested workers are launched. If enough workers are already running, this method does nothing.
   */
  void launchWorkersIfNeeded(int workerCount) throws InterruptedException;

  /**
   * Blocks until workers with the provided worker numbers (indexes into {@link #getWorkerIds()} are ready to be
   * contacted for work.
   */
  void waitForWorkers(Set<Integer> workerNumbers) throws InterruptedException;

  /**
   * List of currently-active workers.
   */
  List<String> getWorkerIds();

  /**
   * Number of currently-active and currently-pending workers.
   */
  WorkerCount getWorkerCount();

  /**
   * Worker number of a worker with the provided ID, or {@link #UNKNOWN_WORKER_NUMBER} if none exists.
   */
  int getWorkerNumber(String workerId);

  /**
   * Whether an active worker exists with the provided ID.
   */
  boolean isWorkerActive(String workerId);

  /**
   * Map of worker number to list of all workers currently running with that number. More than one worker per number
   * only occurs when fault tolerance is enabled.
   */
  Map<Integer, List<WorkerStats>> getWorkerStats();

  /**
   * Blocks until all workers exit. Returns quietly, no matter whether there was an exception associated with the
   * future from {@link #start()} or not.
   *
   * @param interrupt whether to interrupt currently-running work
   */
  void stop(boolean interrupt);
}
