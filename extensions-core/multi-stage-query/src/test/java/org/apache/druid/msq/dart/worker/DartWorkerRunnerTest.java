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

import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.msq.dart.DartResourcePermissionMapper;
import org.apache.druid.msq.dart.worker.http.GetWorkersResponse;
import org.apache.druid.msq.exec.Worker;
import org.apache.druid.msq.indexing.error.CanceledFault;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.security.AuthorizerMapper;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
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
  private static final DiscoveryDruidNode CONTROLLER_DISCOVERY_NODE =
      new DiscoveryDruidNode(
          new DruidNode("no", "localhost", false, 8081, -1, true, false),
          NodeRole.BROKER,
          Collections.emptyMap()
      );

  private final SettableFuture<?> workerRun = SettableFuture.create();

  private ExecutorService workerExec;
  private DartWorkerRunner workerRunner;
  private AutoCloseable mockCloser;

  @TempDir
  public Path temporaryFolder;

  @Mock
  private DartWorkerFactory workerFactory;

  @Mock
  private Worker worker;

  @Mock
  private DruidNodeDiscoveryProvider discoveryProvider;

  @Mock
  private DruidNodeDiscovery discovery;

  @Mock
  private AuthorizerMapper authorizerMapper;

  @Captor
  private ArgumentCaptor<DruidNodeDiscovery.Listener> discoveryListener;

  @BeforeEach
  public void setUp()
  {
    mockCloser = MockitoAnnotations.openMocks(this);
    workerRunner = new DartWorkerRunner(
        workerFactory,
        workerExec = Execs.multiThreaded(MAX_WORKERS, "worker-exec-%s"),
        discoveryProvider,
        new DartResourcePermissionMapper(),
        authorizerMapper,
        temporaryFolder.toFile()
    );

    // "discoveryProvider" provides "discovery".
    Mockito.when(discoveryProvider.getForNodeRole(NodeRole.BROKER)).thenReturn(discovery);

    // "workerFactory" builds "worker".
    Mockito.when(
        workerFactory.build(
            QUERY_ID,
            CONTROLLER_SERVER_HOST,
            temporaryFolder.toFile(),
            QueryContext.empty()
        )
    ).thenReturn(worker);

    // "worker.run()" exits when "workerRun" resolves.
    Mockito.doAnswer(invocation -> {
      workerRun.get();
      return null;
    }).when(worker).run();

    // "worker.stop()" sets "workerRun" to a cancellation error.
    Mockito.doAnswer(invocation -> {
      workerRun.setException(new MSQException(CanceledFault.instance()));
      return null;
    }).when(worker).stop();

    // "worker.controllerFailed()" sets "workerRun" to an error.
    Mockito.doAnswer(invocation -> {
      workerRun.setException(new ISE("Controller failed"));
      return null;
    }).when(worker).controllerFailed();

    // "worker.awaitStop()" waits for "workerRun". It does not throw an exception, just like WorkerImpl.awaitStop.
    Mockito.doAnswer(invocation -> {
      try {
        workerRun.get();
      }
      catch (Throwable e) {
        // Suppress
      }
      return null;
    }).when(worker).awaitStop();

    // "worker.id()" returns WORKER_ID.
    Mockito.when(worker.id()).thenReturn(WORKER_ID.toString());

    // Start workerRunner, capture listener in "discoveryListener".
    workerRunner.start();
    Mockito.verify(discovery).registerListener(discoveryListener.capture());
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
    Assertions.assertEquals(new GetWorkersResponse(Collections.emptyList()), workersResponse);
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
    FileUtils.mkdirp(new File(temporaryFolder.toFile(), "x"));
    Assertions.assertArrayEquals(
        new File[]{new File(temporaryFolder.toFile(), "x")},
        temporaryFolder.toFile().listFiles()
    );

    // Run "createAndCleanTempDirectory", which will delete it.
    workerRunner.createAndCleanTempDirectory();
    Assertions.assertArrayEquals(new File[]{}, temporaryFolder.toFile().listFiles());
  }

  @Test
  public void test_startWorker_controllerNotActive()
  {
    final DruidException e = Assertions.assertThrows(
        DruidException.class,
        () -> workerRunner.startWorker("abc", CONTROLLER_SERVER_HOST, QueryContext.empty())
    );

    MatcherAssert.assertThat(
        e,
        ThrowableMessageMatcher.hasMessage(CoreMatchers.containsString(
            "Received startWorker request for unknown controller"))
    );
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
    // Activate controller.
    discoveryListener.getValue().nodesAdded(Collections.singletonList(CONTROLLER_DISCOVERY_NODE));

    // Start the worker twice (startWorker is idempotent; nothing special happens the second time).
    final Worker workerFromStart = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    final Worker workerFromStart2 = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    Assertions.assertSame(worker, workerFromStart);
    Assertions.assertSame(worker, workerFromStart2);

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
  public void test_startWorker_thenRemoveController() throws InterruptedException
  {
    // Activate controller.
    discoveryListener.getValue().nodesAdded(Collections.singletonList(CONTROLLER_DISCOVERY_NODE));

    // Start the worker.
    final Worker workerFromStart = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    Assertions.assertSame(worker, workerFromStart);
    Assertions.assertEquals(1, workerRunner.getWorkersResponse().getWorkers().size());

    // Deactivate controller.
    discoveryListener.getValue().nodesRemoved(Collections.singletonList(CONTROLLER_DISCOVERY_NODE));

    // Worker should go away.
    workerRunner.awaitQuerySet(Set::isEmpty);
    Assertions.assertEquals(0, workerRunner.getWorkersResponse().getWorkers().size());
  }

  @Test
  @Timeout(value = 1, unit = TimeUnit.MINUTES)
  public void test_startWorker_thenStopWorker() throws InterruptedException
  {
    // Activate controller.
    discoveryListener.getValue().nodesAdded(Collections.singletonList(CONTROLLER_DISCOVERY_NODE));

    // Start the worker.
    final Worker workerFromStart = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    Assertions.assertSame(worker, workerFromStart);
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
    // Activate controller.
    discoveryListener.getValue().nodesAdded(Collections.singletonList(CONTROLLER_DISCOVERY_NODE));

    // Start the worker.
    final Worker workerFromStart = workerRunner.startWorker(QUERY_ID, CONTROLLER_SERVER_HOST, QueryContext.empty());
    Assertions.assertSame(worker, workerFromStart);
    Assertions.assertEquals(1, workerRunner.getWorkersResponse().getWorkers().size());

    // Stop runner.
    workerRunner.stop();

    // Worker should go away.
    workerRunner.awaitQuerySet(Set::isEmpty);
    Assertions.assertEquals(0, workerRunner.getWorkersResponse().getWorkers().size());
  }
}
