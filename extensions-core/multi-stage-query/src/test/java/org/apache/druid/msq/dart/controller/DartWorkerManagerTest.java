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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.dart.worker.DartWorkerClient;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.WorkerManager;
import org.apache.druid.msq.exec.WorkerStats;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DartWorkerManagerTest
{
  private static final List<String> WORKERS = ImmutableList.of(
      new WorkerId("http", "localhost:1001", "abc").toString(),
      new WorkerId("http", "localhost:1002", "abc").toString()
  );

  private DartWorkerManager workerManager;
  private AutoCloseable mockCloser;

  @Mock
  private DartWorkerClient workerClient;

  @BeforeEach
  public void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);
    workerManager = new DartWorkerManager(WORKERS, workerClient);
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    mockCloser.close();
  }

  @Test
  public void test_getWorkerCount()
  {
    Assertions.assertEquals(0, workerManager.getWorkerCount().getPendingWorkerCount());
    Assertions.assertEquals(2, workerManager.getWorkerCount().getRunningWorkerCount());
  }

  @Test
  public void test_getWorkerIds()
  {
    Assertions.assertEquals(WORKERS, workerManager.getWorkerIds());
  }

  @Test
  public void test_getWorkerStats()
  {
    final Map<Integer, List<WorkerStats>> stats = workerManager.getWorkerStats();
    Assertions.assertEquals(
        ImmutableMap.of(
            0, Collections.singletonList(new WorkerStats(WORKERS.get(0), TaskState.RUNNING, -1, -1)),
            1, Collections.singletonList(new WorkerStats(WORKERS.get(1), TaskState.RUNNING, -1, -1))
        ),
        stats
    );
  }

  @Test
  public void test_getWorkerNumber()
  {
    Assertions.assertEquals(0, workerManager.getWorkerNumber(WORKERS.get(0)));
    Assertions.assertEquals(1, workerManager.getWorkerNumber(WORKERS.get(1)));
    Assertions.assertEquals(WorkerManager.UNKNOWN_WORKER_NUMBER, workerManager.getWorkerNumber("nonexistent"));
  }

  @Test
  public void test_isWorkerActive()
  {
    Assertions.assertTrue(workerManager.isWorkerActive(WORKERS.get(0)));
    Assertions.assertTrue(workerManager.isWorkerActive(WORKERS.get(1)));
    Assertions.assertFalse(workerManager.isWorkerActive("nonexistent"));
  }

  @Test
  public void test_launchWorkersIfNeeded()
  {
    workerManager.launchWorkersIfNeeded(0); // Does nothing, less than WORKERS.size()
    workerManager.launchWorkersIfNeeded(1); // Does nothing, less than WORKERS.size()
    workerManager.launchWorkersIfNeeded(2); // Does nothing, equal to WORKERS.size()
    Assert.assertThrows(
        DruidException.class,
        () -> workerManager.launchWorkersIfNeeded(3)
    );
  }

  @Test
  public void test_waitForWorkers()
  {
    workerManager.launchWorkersIfNeeded(2);
    workerManager.waitForWorkers(IntSet.of(0, 1)); // Returns immediately
  }

  @Test
  public void test_start_stop_noInterrupt()
  {
    Mockito.when(workerClient.stopWorker(WORKERS.get(0)))
           .thenReturn(Futures.immediateFuture(null));
    Mockito.when(workerClient.stopWorker(WORKERS.get(1)))
           .thenReturn(Futures.immediateFuture(null));

    final ListenableFuture<?> future = workerManager.start();
    workerManager.stop(false);

    // Ensure the future from start() resolves.
    Assertions.assertNull(FutureUtils.getUnchecked(future, true));
  }

  @Test
  public void test_start_stop_interrupt()
  {
    Mockito.when(workerClient.stopWorker(WORKERS.get(0)))
           .thenReturn(Futures.immediateFuture(null));
    Mockito.when(workerClient.stopWorker(WORKERS.get(1)))
           .thenReturn(Futures.immediateFuture(null));

    final ListenableFuture<?> future = workerManager.start();
    workerManager.stop(true);

    // Ensure the future from start() resolves.
    Assertions.assertNull(FutureUtils.getUnchecked(future, true));
  }

  @Test
  public void test_start_stop_interrupt_clientError()
  {
    Mockito.when(workerClient.stopWorker(WORKERS.get(0)))
           .thenReturn(Futures.immediateFailedFuture(new ISE("stop failure")));
    Mockito.when(workerClient.stopWorker(WORKERS.get(1)))
           .thenReturn(Futures.immediateFuture(null));

    final ListenableFuture<?> future = workerManager.start();
    workerManager.stop(true);

    // Ensure the future from start() resolves.
    Assertions.assertNull(FutureUtils.getUnchecked(future, true));
  }
}
