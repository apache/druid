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

package org.apache.druid.msq.dart.worker;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Injector;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.msq.dart.DartResourcePermissionMapper;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.dart.guice.DartWorkerConfig;
import org.apache.druid.msq.dart.worker.http.GetWorkersResponse;
import org.apache.druid.msq.dart.worker.http.SyncWorkersRequest;
import org.apache.druid.msq.dart.worker.http.SyncWorkersResponse;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.exec.WorkerClient;
import org.apache.druid.msq.exec.WorkerContext;
import org.apache.druid.msq.exec.WorkerImpl;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.testing.junit.LoggerCaptureExtension;
import org.joda.time.Period;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Functional test of {@link DartWorkerRunner}.
 */
public class DartWorkerRunnerTest
{
  private static final int MAX_WORKERS = 1;
  private static final String QUERY_ID = "abc";
  private static final WorkerId WORKER_ID = new WorkerId("http", "localhost:8282", QUERY_ID);
  private static final String CONTROLLER_SERVER_HOST = "localhost:8081";
  private static final String OTHER_CONTROLLER_SERVER_HOST = "localhost:9081";
  private static final Duration CONTROLLER_TIMEOUT = Duration.ofMinutes(3);
  private static final Duration SYNC_GRACE = Duration.ofSeconds(10);

  private ListeningExecutorService workerExec;
  private DartWorkerRunner workerRunner;
  private TestClock clock;
  private AutoCloseable mockCloser;

  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  @RegisterExtension
  public final LoggerCaptureExtension logCapture = new LoggerCaptureExtension(DartWorkerRunner.class);

  @Mock
  private DartWorkerContextFactory workerFactory;

  @Mock
  private WorkerContext workerContext;

  @Mock
  private Outbox<ControllerMessage> outbox;

  @Mock
  private AuthorizerMapper authorizerMapper;

  @Mock
  private ScheduledExecutorService reaperExec;

  @BeforeEach
  public void setUp() throws InterruptedException
  {
    mockCloser = MockitoAnnotations.openMocks(this);
    clock = new TestClock();
    workerExec = MoreExecutors.listeningDecorator(Execs.multiThreaded(MAX_WORKERS, "worker-exec-%s"));
    workerRunner = makeWorkerRunner(CONTROLLER_TIMEOUT, SYNC_GRACE);

    // "workerFactory" builds "workerContext".
    Mockito.when(
        workerFactory.build(
            QUERY_ID,
            CONTROLLER_SERVER_HOST,
            temporaryFolder.getRoot(),
            QueryContext.empty()
        )
    ).thenReturn(workerContext);

    // Set up workerContext.workerId() to return WORKER_ID.
    Mockito.when(workerContext.workerId()).thenReturn(WORKER_ID.toString());

    // Set up workerContext.selfNode() to return a mock DruidNode
    DruidNode selfNode = new DruidNode("worker", "localhost", false, 8282, -1, true, false);
    Mockito.when(workerContext.selfNode()).thenReturn(selfNode);

    // Set up what WorkerImpl needs to start, so workers keep running until they are canceled.
    final Injector injector = Mockito.mock(Injector.class);
    Mockito.when(injector.getInstance(QueryProcessingPool.class)).thenReturn(Mockito.mock(QueryProcessingPool.class));
    Mockito.when(workerContext.injector()).thenReturn(injector);
    Mockito.when(workerContext.makeWorkerClient()).thenReturn(Mockito.mock(WorkerClient.class));

    // Let stop() see reaperExec terminate.
    Mockito.when(reaperExec.awaitTermination(ArgumentMatchers.anyLong(), ArgumentMatchers.any()))
           .thenReturn(true);

    // Start workerRunner. Tests call reapControllersIfNeeded() directly rather than relying on reaperExec.
    workerRunner.start();
    Mockito.verify(reaperExec).schedule(
        ArgumentMatchers.any(Runnable.class),
        ArgumentMatchers.anyLong(),
        ArgumentMatchers.eq(TimeUnit.MILLISECONDS)
    );
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    workerExec.shutdown();
    workerRunner.stop();
    mockCloser.close();

    if (!workerExec.awaitTermination(1, TimeUnit.MINUTES)) {
      throw new ISE("workerExec did not terminate within timeout");
    }
  }

  @Test
  public void test_getWorkersResponse_empty()
  {
    final GetWorkersResponse workersResponse = workerRunner.getWorkersResponse();
    Assertions.assertEquals(new GetWorkersResponse(Collections.emptyList(), 0L), workersResponse);
  }

  @Test
  public void test_getWorkerResource_notFound()
  {
    Assertions.assertNull(workerRunner.getWorkerResource("nonexistent"));
  }

  @Test
  public void test_createAndCleanTempDirectory() throws IOException
  {
    workerRunner.stop();

    // Create an empty directory "x".
    FileUtils.mkdirp(new File(temporaryFolder.getRoot(), "x"));
    Assertions.assertArrayEquals(
        new File[]{new File(temporaryFolder.getRoot(), "x")},
        temporaryFolder.getRoot().listFiles()
    );

    // Run "createAndCleanTempDirectory", which will delete it.
    workerRunner.createAndCleanTempDirectory();
    Assertions.assertArrayEquals(new File[]{}, temporaryFolder.getRoot().listFiles());
  }

  @Test
  public void test_constructor_controllerTimeoutTooLow()
  {
    final DruidException e =
        Assertions.assertThrows(DruidException.class, () -> makeWorkerRunner(Duration.ofSeconds(30), SYNC_GRACE));

    Assertions.assertTrue(e.getMessage().contains("controllerTimeout"), e.getMessage());
  }

  @Test
  public void test_constructor_syncGracePeriodNotPositive()
  {
    final DruidException e =
        Assertions.assertThrows(DruidException.class, () -> makeWorkerRunner(CONTROLLER_TIMEOUT, Duration.ZERO));

    Assertions.assertTrue(e.getMessage().contains("syncGracePeriod"), e.getMessage());
  }

  @Test
  public void test_stopWorker_nonexistent()
  {
    // Nothing happens when we do this. Just verifying an exception isn't thrown.
    workerRunner.stopWorker("nonexistent");
  }

  @Test
  public void test_startWorker()
  {
    // Start the worker twice (startWorker is idempotent; nothing special happens the second time).
    final Worker workerFromStart = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    final Worker workerFromStart2 = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    Assertions.assertSame(workerContext, ((WorkerImpl) workerFromStart).getWorkerContext());
    Assertions.assertSame(workerContext, ((WorkerImpl) workerFromStart2).getWorkerContext());

    // Worker should enter the GetWorkersResponse.
    final GetWorkersResponse workersResponse = workerRunner.getWorkersResponse();
    Assertions.assertEquals(1, workersResponse.getWorkers().size());
    Assertions.assertEquals(QUERY_ID, workersResponse.getWorkers().get(0).getDartQueryId());
    Assertions.assertEquals(CONTROLLER_SERVER_HOST, workersResponse.getWorkers().get(0).getControllerHost());
    Assertions.assertEquals(WORKER_ID, workersResponse.getWorkers().get(0).getWorkerId());

    // Worker should have a resource.
    Assertions.assertNotNull(workerRunner.getWorkerResource(QUERY_ID));
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  public void test_reapControllersIfNeeded_afterTimeout() throws InterruptedException
  {
    workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    Assertions.assertEquals(1, workerRunner.getWorkersResponse().getWorkers().size());

    // Controller has not timed out yet.
    clock.advance(CONTROLLER_TIMEOUT);
    workerRunner.reapControllersIfNeeded();
    Assertions.assertEquals(1, workerRunner.getWorkersResponse().getWorkers().size());
    Mockito.verify(outbox, Mockito.never()).resetOutbox(ArgumentMatchers.anyString());

    // Now it has.
    clock.advance(Duration.ofMillis(1));
    workerRunner.reapControllersIfNeeded();
    workerRunner.awaitQuerySet(Set::isEmpty);

    // Reset once by the reap, and again when the controller's last worker exits.
    Mockito.verify(outbox, Mockito.times(2)).resetOutbox(CONTROLLER_SERVER_HOST);
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  public void test_reapControllersIfNeeded_contactKeepsControllerAlive() throws InterruptedException
  {
    workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());

    // Contact from the controller, through a relay poll or a sync, keeps it alive past the original timeout.
    clock.advance(CONTROLLER_TIMEOUT);
    workerRunner.controllerHeartbeat(CONTROLLER_SERVER_HOST);
    clock.advance(CONTROLLER_TIMEOUT);
    workerRunner.syncController(CONTROLLER_SERVER_HOST, new SyncWorkersRequest(Set.of(QUERY_ID)));
    clock.advance(CONTROLLER_TIMEOUT);
    workerRunner.reapControllersIfNeeded();
    Mockito.verify(outbox, Mockito.never()).resetOutbox(ArgumentMatchers.anyString());
    assertWorkerNotCanceled();

    // Contact from some other controller does not.
    workerRunner.controllerHeartbeat(OTHER_CONTROLLER_SERVER_HOST);
    clock.advance(Duration.ofMillis(1));
    workerRunner.reapControllersIfNeeded();
    workerRunner.awaitQuerySet(Set::isEmpty);
    Mockito.verify(outbox, Mockito.times(2)).resetOutbox(CONTROLLER_SERVER_HOST);
    Mockito.verify(outbox, Mockito.never()).resetOutbox(OTHER_CONTROLLER_SERVER_HOST);
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  public void test_reapControllersIfNeeded_resetsOutboxWithoutWorkers()
  {
    workerRunner.controllerHeartbeat(CONTROLLER_SERVER_HOST);
    clock.advance(CONTROLLER_TIMEOUT.plusMillis(1));
    workerRunner.reapControllersIfNeeded();
    Mockito.verify(outbox).resetOutbox(CONTROLLER_SERVER_HOST);

    // Reaping again does nothing, since the controller has been forgotten.
    workerRunner.reapControllersIfNeeded();
    Mockito.verify(outbox, Mockito.times(1)).resetOutbox(CONTROLLER_SERVER_HOST);
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  public void test_syncController_cancelsAfterGrace() throws InterruptedException
  {
    workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());

    // A sync that leaves the query out, then one that includes it, as when a stale sync arrives late. The first
    // response still lists the query.
    Assertions.assertEquals(
        new SyncWorkersResponse(Set.of(QUERY_ID)),
        workerRunner.syncController(CONTROLLER_SERVER_HOST, new SyncWorkersRequest(Set.of()))
    );
    workerRunner.syncController(CONTROLLER_SERVER_HOST, new SyncWorkersRequest(Set.of(QUERY_ID)));

    // Grace period starts over with the next sync that leaves the query out.
    clock.advance(SYNC_GRACE);
    workerRunner.syncController(CONTROLLER_SERVER_HOST, new SyncWorkersRequest(Set.of()));
    clock.advance(SYNC_GRACE.minusMillis(1));
    workerRunner.syncController(CONTROLLER_SERVER_HOST, new SyncWorkersRequest(Set.of()));
    assertWorkerNotCanceled();

    // Sync after the grace period cancels the worker.
    clock.advance(Duration.ofMillis(1));
    workerRunner.syncController(CONTROLLER_SERVER_HOST, new SyncWorkersRequest(Set.of()));
    Assertions.assertEquals(1, logCapture.getLogEvents().size());
    workerRunner.awaitQuerySet(Set::isEmpty);
  }

  @Test
  public void test_syncController_otherControllerDoesNotCancel()
  {
    workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());

    Assertions.assertEquals(
        new SyncWorkersResponse(Set.of()),
        workerRunner.syncController(OTHER_CONTROLLER_SERVER_HOST, new SyncWorkersRequest(Set.of()))
    );
    clock.advance(SYNC_GRACE.multipliedBy(2));
    workerRunner.syncController(OTHER_CONTROLLER_SERVER_HOST, new SyncWorkersRequest(Set.of()));
    assertWorkerNotCanceled();
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  public void test_startWorker_thenStopWorker() throws InterruptedException
  {
    // Start the worker.
    final Worker workerFromStart = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    Assertions.assertSame(workerContext, ((WorkerImpl) workerFromStart).getWorkerContext());
    Assertions.assertEquals(1, workerRunner.getWorkersResponse().getWorkers().size());

    // Stop that worker.
    workerRunner.stopWorker(QUERY_ID);

    // Worker should go away.
    workerRunner.awaitQuerySet(Set::isEmpty);
    Assertions.assertEquals(0, workerRunner.getWorkersResponse().getWorkers().size());
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  public void test_startWorker_thenStopRunner() throws InterruptedException
  {
    // Start the worker.
    final Worker workerFromStart = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    Assertions.assertSame(workerContext, ((WorkerImpl) workerFromStart).getWorkerContext());
    Assertions.assertEquals(1, workerRunner.getWorkersResponse().getWorkers().size());

    // Stop runner.
    workerRunner.stop();

    // Worker should go away.
    workerRunner.awaitQuerySet(Set::isEmpty);
    Assertions.assertEquals(0, workerRunner.getWorkersResponse().getWorkers().size());
  }

  /**
   * Asserts that {@link DartWorkerRunner} has not canceled the worker. Checks that nothing was logged, since
   * cancellations are logged right away, but workers leave {@link DartWorkerRunner#getWorkersResponse()} only
   * once they exit.
   */
  private void assertWorkerNotCanceled()
  {
    Assertions.assertEquals(1, workerRunner.getWorkersResponse().getWorkers().size());
    Assertions.assertEquals(List.of(), logCapture.getLogEvents());
  }

  private DartWorkerRunner makeWorkerRunner(final Duration controllerTimeout, final Duration syncGracePeriod)
  {
    return new DartWorkerRunner(
        workerFactory,
        workerExec,
        outbox,
        new DartResourcePermissionMapper(),
        authorizerMapper,
        new DartWorkerConfig()
        {
          @Override
          public Period getControllerTimeout()
          {
            return Period.millis((int) controllerTimeout.toMillis());
          }

          @Override
          public Period getSyncGracePeriod()
          {
            return Period.millis((int) syncGracePeriod.toMillis());
          }
        },
        temporaryFolder.getRoot(),
        clock,
        reaperExec
    );
  }

  /**
   * A manually-advanced {@link Clock} for tests.
   */
  private static class TestClock extends Clock
  {
    private long nowMillis;

    public void advance(final Duration duration)
    {
      nowMillis += duration.toMillis();
    }

    @Override
    public long millis()
    {
      return nowMillis;
    }

    @Override
    public Instant instant()
    {
      return Instant.ofEpochMilli(millis());
    }

    @Override
    public ZoneId getZone()
    {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(final ZoneId zone)
    {
      throw new UnsupportedOperationException();
    }
  }
}
