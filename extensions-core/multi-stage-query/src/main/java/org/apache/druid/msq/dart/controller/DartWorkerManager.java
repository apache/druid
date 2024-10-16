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

package org.apache.druid.msq.dart.controller;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.dart.worker.DartWorkerClient;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerManager;
import org.apache.druid.msq.exec.WorkerStats;
import org.apache.druid.msq.indexing.WorkerCount;
import org.apache.druid.utils.CloseableUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Dart implementation of the {@link WorkerManager} returned by {@link ControllerContext#newWorkerManager}.
 *
 * This manager does not actually launch workers. The workers are housed on long-lived servers outside of this
 * manager's control. This manager merely reports on their existence.
 */
public class DartWorkerManager implements WorkerManager
{
  private static final Logger log = new Logger(DartWorkerManager.class);

  private final List<String> workerIds;
  private final DartWorkerClient workerClient;
  private final Object2IntMap<String> workerIdToNumber;
  private final AtomicReference<State> state = new AtomicReference<>(State.NEW);
  private final SettableFuture<?> stopFuture = SettableFuture.create();

  enum State
  {
    NEW,
    STARTED,
    STOPPED
  }

  public DartWorkerManager(
      final List<String> workerIds,
      final DartWorkerClient workerClient
  )
  {
    this.workerIds = workerIds;
    this.workerClient = workerClient;
    this.workerIdToNumber = new Object2IntOpenHashMap<>();
    this.workerIdToNumber.defaultReturnValue(UNKNOWN_WORKER_NUMBER);

    for (int i = 0; i < workerIds.size(); i++) {
      workerIdToNumber.put(workerIds.get(i), i);
    }
  }

  @Override
  public ListenableFuture<?> start()
  {
    if (!state.compareAndSet(State.NEW, State.STARTED)) {
      throw new ISE("Cannot start from state[%s]", state.get());
    }

    return stopFuture;
  }

  @Override
  public void launchWorkersIfNeeded(int workerCount)
  {
    // Nothing to do, just validate the count.
    if (workerCount > workerIds.size()) {
      throw DruidException.defensive(
          "Desired workerCount[%s] must be less than or equal to actual workerCount[%s]",
          workerCount,
          workerIds.size()
      );
    }
  }

  @Override
  public void waitForWorkers(Set<Integer> workerNumbers)
  {
    // Nothing to wait for, just validate the numbers.
    for (final int workerNumber : workerNumbers) {
      if (workerNumber >= workerIds.size()) {
        throw DruidException.defensive(
            "Desired workerNumber[%s] must be less than workerCount[%s]",
            workerNumber,
            workerIds.size()
        );
      }
    }
  }

  @Override
  public List<String> getWorkerIds()
  {
    return workerIds;
  }

  @Override
  public WorkerCount getWorkerCount()
  {
    return new WorkerCount(workerIds.size(), 0);
  }

  @Override
  public int getWorkerNumber(String workerId)
  {
    return workerIdToNumber.getInt(workerId);
  }

  @Override
  public boolean isWorkerActive(String workerId)
  {
    return workerIdToNumber.containsKey(workerId);
  }

  @Override
  public Map<Integer, List<WorkerStats>> getWorkerStats()
  {
    final Int2ObjectMap<List<WorkerStats>> retVal = new Int2ObjectAVLTreeMap<>();

    for (int i = 0; i < workerIds.size(); i++) {
      retVal.put(i, Collections.singletonList(new WorkerStats(workerIds.get(i), TaskState.RUNNING, -1, -1)));
    }

    return retVal;
  }

  /**
   * Stop method. Possibly signals workers to stop, but does not actually wait for them to exit.
   *
   * If "interrupt" is false, does nothing special (other than setting {@link #stopFuture}). The assumption is that
   * a previous call to {@link WorkerClient#postFinish} would have caused the worker to exit.
   *
   * If "interrupt" is true, sends {@link DartWorkerClient#stopWorker(String)} to workers to stop the current query ID.
   *
   * @param interrupt whether to interrupt currently-running work
   */
  @Override
  public void stop(boolean interrupt)
  {
    if (state.compareAndSet(State.STARTED, State.STOPPED)) {
      if (interrupt) {
        final List<ListenableFuture<?>> futures = new ArrayList<>();

        // Send stop commands to all workers. This ensures they exit promptly, and do not get left in a zombie state.
        // For this reason, the workerClient uses an unlimited retry policy. If a stop command is lost, a worker
        // could get stuck in a zombie state without its controller. This state would persist until the server that
        // ran the controller shuts down or restarts. At that time, the listener in DartWorkerRunner.BrokerListener
        // calls "controllerFailed()" on the Worker, and the zombie worker would exit.

        for (final String workerId : workerIds) {
          futures.add(workerClient.stopWorker(workerId));
        }

        // Block until messages are acknowledged, or until the worker we're communicating with has failed.

        try {
          FutureUtils.getUnchecked(Futures.successfulAsList(futures), false);
        }
        catch (Throwable ignored) {
          // Suppress errors.
        }
      }

      CloseableUtils.closeAndSuppressExceptions(workerClient, e -> log.warn(e, "Failed to close workerClient"));
      stopFuture.set(null);
    }
  }
}
