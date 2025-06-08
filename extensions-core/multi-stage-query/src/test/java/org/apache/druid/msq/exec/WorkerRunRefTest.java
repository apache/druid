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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.key.ClusterByPartitions;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class WorkerRunRefTest
{
  private static final Logger log = new Logger(WorkerRunRefTest.class);
  private ListeningExecutorService exec;

  @Before
  public void setUp()
  {
    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(2, "worker-run-ref-test-%s"));
  }

  @After
  public void tearDown() throws InterruptedException
  {
    exec.shutdownNow();
    if (!exec.awaitTermination(5, TimeUnit.MINUTES)) {
      log.warn("Could not terminate executor within 5 minutes");
    }
  }

  @Test
  public void testInterruptionOfLongRunningWorker() throws Exception
  {
    final CountDownLatch workerStarted = new CountDownLatch(1);
    final CountDownLatch workerFinished = new CountDownLatch(1);
    final AtomicBoolean wasInterrupted = new AtomicBoolean(false);
    final Worker worker = new TestWorker("test-worker")
    {
      @Override
      public void run()
      {
        try {
          workerStarted.countDown();
          Thread.sleep(300_000); // Sleep for 5 minutes
        }
        catch (InterruptedException e) {
          wasInterrupted.set(true);
        }
        finally {
          workerFinished.countDown();
        }
      }
    };

    final WorkerRunRef runRef = new WorkerRunRef();
    final ListenableFuture<?> future = runRef.run(worker, exec);

    // Wait for worker to start
    workerStarted.await();

    // Cancel the worker
    runRef.cancel();

    // Wait for worker to finish
    try {
      future.get();
    }
    catch (Exception ignored) {
      // ignore
    }

    Assert.assertTrue("Future should be done", future.isDone());
    Assert.assertTrue("Future should be canceled", future.isCancelled());
    Assert.assertEquals("Latch should have counted down", 0, workerFinished.getCount());
    Assert.assertTrue("Worker should have been interrupted", wasInterrupted.get());

    // awaitStop should return immediately since the worker is done
    runRef.awaitStop();
  }

  @Test
  public void testSuccessfulCompletionOfWorker()
  {
    final Worker worker = new TestWorker("test-worker")
    {
      @Override
      public void run()
      {
        // Exit immediately
      }
    };

    final WorkerRunRef runRef = new WorkerRunRef();
    final ListenableFuture<?> future = runRef.run(worker, exec);

    // Wait for worker to complete
    try {
      future.get();
    }
    catch (Exception ignored) {
      // ignore
    }

    // Future should be done and not canceled
    Assert.assertTrue("Future should be done", future.isDone());
    Assert.assertFalse("Future should not be canceled", future.isCancelled());

    // awaitStop should return immediately since the worker is done
    runRef.awaitStop();
  }

  @Test
  public void testUnsuccessfulCompletionOfWorker()
  {
    final RuntimeException expectedException = new RuntimeException("Worker failed");
    final Worker worker = new TestWorker("test-worker")
    {
      @Override
      public void run()
      {
        throw expectedException;
      }
    };

    final WorkerRunRef runRef = new WorkerRunRef();
    final ListenableFuture<?> future = runRef.run(worker, exec);

    // Wait for worker to finish
    try {
      future.get();
    }
    catch (Exception ignored) {
      // ignore
    }

    // Future should be done and not canceled
    Assert.assertTrue("Future should be done", future.isDone());
    Assert.assertFalse("Future should not be canceled", future.isCancelled());

    // awaitStop should not throw even though the worker failed
    runRef.awaitStop();
  }

  @Test
  public void testCancelBeforeRun()
  {
    final CountDownLatch workerStarted = new CountDownLatch(1);
    final Worker worker = new TestWorker("test-worker")
    {
      @Override
      public void run()
      {
        workerStarted.countDown();
      }
    };

    final WorkerRunRef runRef = new WorkerRunRef();

    // Cancel before run
    runRef.cancel();

    // Run should return a completed future
    final ListenableFuture<?> future = runRef.run(worker, exec);
    Assert.assertTrue("Future should be done", future.isDone());
    Assert.assertFalse("Future should not be canceled", future.isCancelled());

    // Worker should not have run
    Assert.assertEquals(1, workerStarted.getCount());
  }

  @Test
  public void testMultipleRunsThrowException()
  {
    final Worker worker = new TestWorker("test-worker")
    {
      @Override
      public void run()
      {
        // Do nothing
      }
    };

    final WorkerRunRef runRef = new WorkerRunRef();

    // First run should succeed
    runRef.run(worker, exec);

    // Second run should throw
    Assert.assertThrows(
        DruidException.class,
        () -> runRef.run(worker, exec)
    );
  }

  @Test
  public void testAwaitStopWithoutRun()
  {
    final WorkerRunRef runRef = new WorkerRunRef();

    // awaitStop without run should throw
    Assert.assertThrows(
        DruidException.class,
        runRef::awaitStop
    );
  }

  /**
   * Base class for test workers that implements all required methods as no-ops.
   */
  private abstract static class TestWorker implements Worker
  {
    private final String id;

    TestWorker(final String id)
    {
      this.id = id;
    }

    @Override
    public String id()
    {
      return id;
    }

    @Override
    public void postWorkOrder(WorkOrder workOrder)
    {
      // No-op for tests
    }

    @Override
    public ClusterByStatisticsSnapshot fetchStatisticsSnapshot(StageId stageId)
    {
      return null;
    }

    @Override
    public ClusterByStatisticsSnapshot fetchStatisticsSnapshotForTimeChunk(StageId stageId, long timeChunk)
    {
      return null;
    }

    @Override
    public boolean postResultPartitionBoundaries(StageId stageId, ClusterByPartitions stagePartitionBoundaries)
    {
      return false;
    }

    @Override
    public ListenableFuture<InputStream> readStageOutput(StageId stageId, int partitionNumber, long offset)
    {
      return null;
    }

    @Override
    public CounterSnapshotsTree getCounters()
    {
      return null;
    }

    @Override
    public void postCleanupStage(StageId stageId)
    {
      // No-op for tests
    }

    @Override
    public void postFinish()
    {
      // No-op for tests
    }
  }
}
