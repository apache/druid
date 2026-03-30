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
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.indexing.error.CancellationReason;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.report.MSQStatusReport;
import org.apache.druid.msq.indexing.report.MSQTaskReportPayload;
import org.apache.druid.msq.test.NoopQueryListener;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ControllerHolderTest
{
  private static final Logger log = new Logger(ControllerHolderTest.class);
  private ListeningExecutorService exec;
  private ScheduledExecutorService scheduledExec;

  @Before
  public void setUp()
  {
    exec = MoreExecutors.listeningDecorator(Execs.multiThreaded(2, "controller-holder-test-%s"));
    scheduledExec = Executors.newSingleThreadScheduledExecutor(
        Execs.makeThreadFactory("controller-holder-test-timeout-%s")
    );
  }

  @After
  public void tearDown() throws InterruptedException
  {
    exec.shutdownNow();
    scheduledExec.shutdownNow();
    if (!exec.awaitTermination(5, TimeUnit.MINUTES)) {
      log.warn("Could not terminate run executor within 5 minutes");
    }
    if (!scheduledExec.awaitTermination(5, TimeUnit.MINUTES)) {
      log.warn("Could not terminate timeout executor within 5 minutes");
    }
  }

  @Test
  public void testInterruptionOfLongRunningController() throws Exception
  {
    final CountDownLatch controllerStarted = new CountDownLatch(1);
    final CountDownLatch controllerFinished = new CountDownLatch(1);
    final AtomicBoolean wasInterrupted = new AtomicBoolean(false);
    final Controller controller = new TestController("test-query")
    {
      @Override
      public void run(final QueryListener listener)
      {
        try {
          controllerStarted.countDown();
          Thread.sleep(300_000);
        }
        catch (InterruptedException e) {
          wasInterrupted.set(true);
        }
        finally {
          listener.onQueryComplete(makeSuccessReport());
          controllerFinished.countDown();
        }
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "sql-1", null, null, DateTimes.nowUtc());
    final ListenableFuture<?> future = holder.runAsync(new NoopQueryListener(), null, exec, scheduledExec);

    controllerStarted.await();
    holder.cancel(CancellationReason.USER_REQUEST);
    controllerFinished.await();

    try {
      future.get(5, TimeUnit.SECONDS);
    }
    catch (Exception ignored) {
      // ignore
    }

    Assert.assertTrue("Controller should have been interrupted", wasInterrupted.get());
    Assert.assertEquals(ControllerHolder.State.CANCELED, holder.getState());
  }

  @Test
  public void testCancelCallsStop() throws Exception
  {
    final CountDownLatch controllerStarted = new CountDownLatch(1);
    final CountDownLatch controllerFinished = new CountDownLatch(1);
    final AtomicBoolean stopCalled = new AtomicBoolean(false);
    final Controller controller = new TestController("test-query")
    {
      @Override
      public void run(final QueryListener listener)
      {
        try {
          controllerStarted.countDown();
          Thread.sleep(300_000);
        }
        catch (InterruptedException e) {
          // expected
        }
        finally {
          listener.onQueryComplete(makeSuccessReport());
          controllerFinished.countDown();
        }
      }

      @Override
      public void stop(final CancellationReason reason)
      {
        stopCalled.set(true);
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "sql-1", null, null, DateTimes.nowUtc());
    holder.runAsync(new NoopQueryListener(), null, exec, scheduledExec);

    controllerStarted.await();
    holder.cancel(CancellationReason.USER_REQUEST);
    controllerFinished.await();

    Assert.assertTrue("stop() should have been called as failsafe", stopCalled.get());
  }

  @Test
  public void testSuccessfulCompletion() throws Exception
  {
    final Controller controller = new TestController("test-query")
    {
      @Override
      public void run(final QueryListener listener)
      {
        listener.onQueryComplete(makeSuccessReport());
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "sql-1", null, null, DateTimes.nowUtc());
    final ListenableFuture<?> future = holder.runAsync(new NoopQueryListener(), null, exec, scheduledExec);
    future.get(5, TimeUnit.SECONDS);

    Assert.assertEquals(ControllerHolder.State.SUCCESS, holder.getState());
  }

  @Test
  public void testCancelBeforeRun() throws Exception
  {
    final AtomicBoolean controllerRan = new AtomicBoolean(false);
    final Controller controller = new TestController("test-query")
    {
      @Override
      public void run(final QueryListener listener)
      {
        controllerRan.set(true);
        listener.onQueryComplete(makeSuccessReport());
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "sql-1", null, null, DateTimes.nowUtc());

    // Cancel before run
    holder.cancel(CancellationReason.USER_REQUEST);
    Assert.assertEquals(ControllerHolder.State.CANCELED, holder.getState());

    // Run should complete quickly without running the controller
    final ListenableFuture<?> future = holder.runAsync(new NoopQueryListener(), null, exec, scheduledExec);
    future.get(5, TimeUnit.SECONDS);

    Assert.assertFalse("Controller should not have run", controllerRan.get());
  }

  @Test
  public void testDoubleCancelIsIdempotent() throws Exception
  {
    final CountDownLatch controllerStarted = new CountDownLatch(1);
    final CountDownLatch controllerFinished = new CountDownLatch(1);
    final Controller controller = new TestController("test-query")
    {
      @Override
      public void run(final QueryListener listener)
      {
        try {
          controllerStarted.countDown();
          Thread.sleep(300_000);
        }
        catch (InterruptedException e) {
          // expected
        }
        finally {
          listener.onQueryComplete(makeSuccessReport());
          controllerFinished.countDown();
        }
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "sql-1", null, null, DateTimes.nowUtc());
    holder.runAsync(new NoopQueryListener(), null, exec, scheduledExec);

    controllerStarted.await();

    // Cancel twice — should not throw
    holder.cancel(CancellationReason.USER_REQUEST);
    holder.cancel(CancellationReason.USER_REQUEST);
    controllerFinished.await();

    Assert.assertEquals(ControllerHolder.State.CANCELED, holder.getState());
  }

  @Test
  public void testCancelAfterCompletion() throws Exception
  {
    final Controller controller = new TestController("test-query")
    {
      @Override
      public void run(final QueryListener listener)
      {
        listener.onQueryComplete(makeSuccessReport());
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "sql-1", null, null, DateTimes.nowUtc());
    final ListenableFuture<?> future = holder.runAsync(new NoopQueryListener(), null, exec, scheduledExec);
    future.get(5, TimeUnit.SECONDS);

    Assert.assertEquals(ControllerHolder.State.SUCCESS, holder.getState());

    // Cancel after completion — should be a no-op
    holder.cancel(CancellationReason.USER_REQUEST);
    Assert.assertEquals(ControllerHolder.State.SUCCESS, holder.getState());
  }

  @Test
  public void testWithNullRegistry() throws Exception
  {
    final Controller controller = new TestController("test-query")
    {
      @Override
      public void run(final QueryListener listener)
      {
        listener.onQueryComplete(makeSuccessReport());
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "id", null, null, DateTimes.nowUtc());
    final ListenableFuture<?> future = holder.runAsync(
        new NoopQueryListener(),
        null,
        Execs.directExecutor(),
        scheduledExec
    );
    future.get(5, TimeUnit.SECONDS);

    Assert.assertEquals(ControllerHolder.State.SUCCESS, holder.getState());
  }

  @Test
  public void testWithRegistry() throws Exception
  {
    final AtomicBoolean registered = new AtomicBoolean(false);
    final AtomicBoolean deregistered = new AtomicBoolean(false);
    final ControllerRegistry registry = new ControllerRegistry()
    {
      @Override
      public void register(final ControllerHolder holder)
      {
        registered.set(true);
      }

      @Override
      public void deregister(final ControllerHolder holder, @Nullable final TaskReport.ReportMap report)
      {
        deregistered.set(true);
      }
    };

    final Controller controller = new TestController("test-query")
    {
      @Override
      public void run(final QueryListener listener)
      {
        listener.onQueryComplete(makeSuccessReport());
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "sql-1", null, null, DateTimes.nowUtc());
    final ListenableFuture<?> future = holder.runAsync(new NoopQueryListener(), registry, exec, scheduledExec);
    future.get(5, TimeUnit.SECONDS);

    Assert.assertTrue("Should have been registered", registered.get());
    Assert.assertTrue("Should have been deregistered", deregistered.get());
  }

  @Test
  public void testTimeout() throws Exception
  {
    final Controller controller = new TestController("test-query")
    {
      @Override
      public QueryContext getQueryContext()
      {
        return QueryContext.of(Map.of(QueryContexts.TIMEOUT_KEY, 1));
      }

      @Override
      public void run(final QueryListener listener)
      {
        try {
          Thread.sleep(300_000);
        }
        catch (InterruptedException ignored) {
          // expected — canceled due to timeout
        }
        finally {
          listener.onQueryComplete(makeSuccessReport());
        }
      }
    };

    final ControllerHolder holder = new ControllerHolder(controller, "sql-1", null, null, DateTimes.nowUtc());
    final ListenableFuture<?> future = holder.runAsync(new NoopQueryListener(), null, exec, scheduledExec);
    future.get(30, TimeUnit.SECONDS);

    Assert.assertEquals(ControllerHolder.State.CANCELED, holder.getState());
  }

  private static MSQTaskReportPayload makeSuccessReport()
  {
    final MSQStatusReport statusReport =
        new MSQStatusReport(TaskState.SUCCESS, null, null, null, 0, Map.of(), 0, 0, null, null);
    return new MSQTaskReportPayload(statusReport, null, null, null);
  }

  /**
   * Base class for test controllers that implements all required methods as no-ops.
   */
  private abstract static class TestController implements Controller
  {
    private final String queryId;

    TestController(final String queryId)
    {
      this.queryId = queryId;
    }

    @Override
    public String queryId()
    {
      return queryId;
    }

    @Override
    public void stop(final CancellationReason reason)
    {
    }

    @Override
    public void resultsComplete(
        final String queryId,
        final int stageNumber,
        final int workerNumber,
        final Object resultObject
    )
    {
    }

    @Override
    public void updatePartialKeyStatisticsInformation(
        final int stageNumber,
        final int workerNumber,
        final Object partialKeyStatisticsInformationObject
    )
    {
    }

    @Override
    public void doneReadingInput(final int stageNumber, final int workerNumber)
    {
    }

    @Override
    public void workerError(final MSQErrorReport errorReport)
    {
    }

    @Override
    public void workerWarning(final java.util.List<MSQErrorReport> errorReports)
    {
    }

    @Override
    public void updateCounters(
        final String taskId,
        final org.apache.druid.msq.counters.CounterSnapshotsTree snapshotsTree
    )
    {
    }

    @Override
    public boolean hasWorker(final String workerId)
    {
      return false;
    }

    @Override
    public java.util.List<String> getWorkerIds()
    {
      return java.util.List.of();
    }

    @Override
    public TaskReport.ReportMap liveReports()
    {
      return new TaskReport.ReportMap();
    }

    @Override
    public ControllerContext getControllerContext()
    {
      return null;
    }

    @Override
    public QueryContext getQueryContext()
    {
      return QueryContext.empty();
    }
  }
}
