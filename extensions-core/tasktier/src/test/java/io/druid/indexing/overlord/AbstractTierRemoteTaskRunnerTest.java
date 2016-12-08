/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.util.Modules;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceEventBuilder;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.HttpResponseHandler;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.cli.CliOverlord;
import io.druid.concurrent.Execs;
import io.druid.guice.GuiceInjectors;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.resources.TierRunningCheckResource;
import io.druid.initialization.Initialization;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.server.DruidNode;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class AbstractTierRemoteTaskRunnerTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();
  private final HttpClient client = EasyMock.createStrictMock(HttpClient.class);
  private final TierTaskDiscovery tierTaskDiscovery = EasyMock.createStrictMock(TierTaskDiscovery.class);
  private final TaskStorage taskStorage = EasyMock.createStrictMock(TaskStorage.class);
  private final Task task = NoopTask.create();
  private final Capture<Task> taskCapture = Capture.newInstance();
  private final DruidNode workerNode = new DruidNode("some service", "localhost", 0);

  private final ScheduledExecutorService executorService = Execs.scheduledSingleThreaded("AbstractTierTestService--%s");
  private final AbstractTierRemoteTaskRunner abstractTierRemoteTaskRunner = new AbstractTierRemoteTaskRunner(
      tierTaskDiscovery,
      client,
      taskStorage,
      new ScheduledExecutorFactory()
      {
        @Override
        public ScheduledExecutorService create(int corePoolSize, String nameFormat)
        {
          return executorService;
        }
      }
  )
  {

    @Override
    protected void launch(SettableFuture future, Task task)
    {
      taskCapture.setValue(task);
    }
  };

  @Before
  public void generalSetup()
  {
    EasyMock.replay(client);
    EasyMock.replay(tierTaskDiscovery);
    EasyMock.replay(taskStorage);
    noKnown();
  }

  private void useDiscoverySingleNode()
  {
    EasyMock.reset(tierTaskDiscovery);
    EasyMock.expect(tierTaskDiscovery.getNodeForTask(EasyMock.eq(task.getId())))
            .andReturn(Optional.of(workerNode)).once();
    EasyMock.replay(tierTaskDiscovery);
  }

  private void useDiscoveryMap()
  {
    EasyMock.reset(tierTaskDiscovery);
    EasyMock.expect(tierTaskDiscovery.getTasks())
            .andReturn(
                ImmutableMap.<String, DruidNode>of(
                    task.getId(),
                    workerNode
                )
            ).anyTimes();
    EasyMock.replay(tierTaskDiscovery);
  }

  private void useGeneralClient()
  {
    final SettableFuture<Boolean> future = SettableFuture.create();
    future.set(true);
    EasyMock.reset(client);
    EasyMock.expect(client.go(EasyMock.anyObject(Request.class), EasyMock.anyObject(HttpResponseHandler.class)))
            .andReturn(future)
            .once();
    EasyMock.replay(client);
  }

  private void noRunning()
  {
    Assert.assertTrue(abstractTierRemoteTaskRunner.getRunningTasks().isEmpty());
  }

  private void noPending()
  {
    Assert.assertTrue(abstractTierRemoteTaskRunner.getPendingTasks().isEmpty());
  }

  private void noKnown()
  {
    Assert.assertTrue(abstractTierRemoteTaskRunner.getKnownTasks().isEmpty());
  }

  private void onlyRunning()
  {
    noPending();
    testOnlyTasks(abstractTierRemoteTaskRunner.getKnownTasks(), ImmutableSet.of(task.getId()));
    testOnlyTasks(abstractTierRemoteTaskRunner.getRunningTasks(), ImmutableSet.of(task.getId()));
  }

  private void onlyPending()
  {
    noRunning();
    testOnlyTasks(abstractTierRemoteTaskRunner.getPendingTasks(), ImmutableSet.of(task.getId()));
    testOnlyTasks(abstractTierRemoteTaskRunner.getKnownTasks(), ImmutableSet.of(task.getId()));
  }

  private void onlyKnown()
  {
    noPending();
    noRunning();
    testOnlyTasks(abstractTierRemoteTaskRunner.getKnownTasks(), ImmutableSet.of(task.getId()));
  }

  @Test
  public void testSimpleRun()
  {
    final ListenableFuture<TaskStatus> future = submit();
    onlyPending();
  }

  @Test
  public void testMultiStart()
  {
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof ISE && "Already started".equals(((ISE) o).getMessage());
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });

    useDiscoveryMap();
    useGeneralClient();
    abstractTierRemoteTaskRunner.start();
    onlyRunning();
    EasyMock.verify(tierTaskDiscovery);
    abstractTierRemoteTaskRunner.start();
  }

  @Test
  public void testSimpleStatusUpdatesWithOneRetry() throws InterruptedException, ExecutionException, TimeoutException
  {
    for (TaskStatus.Status status : TaskStatus.Status.values()) {
      final String statuString = status.toString();
      EasyMock.reset(taskStorage);

      final ListenableFuture<TaskStatus> future = submit();

      onlyPending();

      final Capture<String> taskIdCapture = Capture.newInstance();
      EasyMock.expect(taskStorage.getStatus(EasyMock.capture(taskIdCapture))).andReturn(
          Optional.<TaskStatus>absent()
      ).once().andReturn(
          Optional.of(
              TaskStatus.fromCode(
                  task.getId(),
                  status
              )
          )
      ).once();
      EasyMock.replay(taskStorage);

      Assert.assertTrue(
          statuString,
          abstractTierRemoteTaskRunner.reportStatus(TaskStatus.fromCode(task.getId(), status))
      );

      if (StatefulTaskRunnerWorkItem.State.of(status).isTerminal()) {
        Assert.assertTrue(statuString, taskIdCapture.hasCaptured());
        Assert.assertEquals(statuString, task.getId(), taskIdCapture.getValue());
        EasyMock.verify(taskStorage);

        Assert.assertEquals(statuString, status, future.get(10, TimeUnit.MILLISECONDS).getStatusCode());

        final ListenableFuture<TaskStatus> future2 = abstractTierRemoteTaskRunner.run(task);
        Assert.assertEquals(statuString, future, future2);
        Assert.assertEquals(statuString, status, future2.get(10, TimeUnit.MILLISECONDS).getStatusCode());
      }
      abstractTierRemoteTaskRunner.stop(); // To clear the IDs
    }
  }

  @Test
  public void testStatusUpdateOnUnknown() throws InterruptedException, ExecutionException, TimeoutException
  {
    for (TaskStatus.Status status : TaskStatus.Status.values()) {
      EasyMock.reset(taskStorage);

      final Capture<String> taskIdCapture = Capture.newInstance();
      EasyMock.expect(taskStorage.getStatus(EasyMock.capture(taskIdCapture))).andReturn(
          Optional.of(
              TaskStatus.fromCode(
                  task.getId(),
                  status
              )
          )
      ).once();
      EasyMock.replay(taskStorage);

      noKnown();

      final TaskStatus taskStatus = TaskStatus.fromCode(task.getId(), status);
      Assert.assertTrue(abstractTierRemoteTaskRunner.reportStatus(taskStatus));
      if (!taskStatus.isComplete()) {
        continue;
      }
      final ListenableFuture<TaskStatus> future = abstractTierRemoteTaskRunner.run(task);

      Assert.assertEquals(status, future.get(10, TimeUnit.MILLISECONDS).getStatusCode());

      Assert.assertTrue(taskIdCapture.hasCaptured());
      Assert.assertEquals(task.getId(), taskIdCapture.getValue());
      EasyMock.verify(taskStorage);

      final ListenableFuture<TaskStatus> future2 = abstractTierRemoteTaskRunner.run(task);
      Assert.assertEquals(future, future2);
      Assert.assertEquals(status, future2.get(10, TimeUnit.MILLISECONDS).getStatusCode());
      abstractTierRemoteTaskRunner.stop(); // To clear the IDs
    }
  }


  @Test
  public void testStatusUpdateOnTerminalMismatch() throws InterruptedException, ExecutionException, TimeoutException
  {
    TaskStatus.Status status = TaskStatus.Status.FAILED;
    EasyMock.reset(taskStorage);

    final Capture<String> taskIdCapture = Capture.newInstance();
    EasyMock.expect(taskStorage.getStatus(EasyMock.capture(taskIdCapture))).andReturn(
        Optional.of(
            TaskStatus.fromCode(
                task.getId(),
                TaskStatus.Status.SUCCESS
            )
        )
    ).once();
    EasyMock.replay(taskStorage);

    noKnown();

    final TaskStatus taskStatus = TaskStatus.fromCode(task.getId(), status);
    final ServiceEmitter serviceEmitter = EasyMock.createStrictMock(ServiceEmitter.class);
    serviceEmitter.emit(EasyMock.anyObject(ServiceEventBuilder.class));
    EasyMock.expectLastCall().once();
    EasyMock.replay(serviceEmitter);
    EmittingLogger.registerEmitter(serviceEmitter);
    Assert.assertTrue(abstractTierRemoteTaskRunner.reportStatus(taskStatus));
    EasyMock.verify(serviceEmitter);
  }

  @Test
  public void testStatusUpdateErrorThenSuccess() throws InterruptedException, ExecutionException, TimeoutException
  {
    TaskStatus.Status status = TaskStatus.Status.SUCCESS;
    EasyMock.reset(taskStorage);

    final RuntimeException testException = new RuntimeException("test exception");
    final Capture<String> taskIdCapture = Capture.newInstance();
    EasyMock.expect(taskStorage.getStatus(EasyMock.capture(taskIdCapture)))
            .andThrow(testException)
            .once()
            .andReturn(
                Optional.of(
                    TaskStatus.fromCode(
                        task.getId(),
                        TaskStatus.Status.SUCCESS
                    )
                )
            ).once();
    EasyMock.replay(taskStorage);

    noKnown();

    final TaskStatus taskStatus = TaskStatus.fromCode(task.getId(), status);
    try {
      abstractTierRemoteTaskRunner.reportStatus(taskStatus);
      Assert.fail("Should not get here");
    }
    catch (RuntimeException e) {
      Assert.assertEquals("Exceptions did not match", testException, e);
    }
    onlyKnown();

    Assert.assertTrue(abstractTierRemoteTaskRunner.reportStatus(taskStatus));
    final ListenableFuture<TaskStatus> future = abstractTierRemoteTaskRunner.run(task);

    Assert.assertEquals(status, future.get(10, TimeUnit.MILLISECONDS).getStatusCode());

    Assert.assertTrue(taskIdCapture.hasCaptured());
    Assert.assertEquals(task.getId(), taskIdCapture.getValue());
    EasyMock.verify(taskStorage);

    final ListenableFuture<TaskStatus> future2 = abstractTierRemoteTaskRunner.run(task);
    Assert.assertEquals(future, future2);
    Assert.assertEquals(status, future2.get(10, TimeUnit.MILLISECONDS).getStatusCode());
    abstractTierRemoteTaskRunner.stop(); // To clear the IDs
  }

  @Test
  public void testStatusUpdateAfterRunning() throws InterruptedException, ExecutionException, TimeoutException
  {
    TaskStatus.Status status = TaskStatus.Status.SUCCESS;
    EasyMock.reset(taskStorage);

    final Capture<String> taskIdCapture = Capture.newInstance();
    EasyMock.expect(taskStorage.getStatus(EasyMock.capture(taskIdCapture))).andReturn(
        Optional.of(
            TaskStatus.fromCode(
                task.getId(),
                TaskStatus.Status.RUNNING
            )
        )
    ).once().andReturn(
        Optional.of(
            TaskStatus.fromCode(
                task.getId(),
                TaskStatus.Status.SUCCESS
            )
        )
    ).once();
    EasyMock.replay(taskStorage);

    noKnown();

    final TaskStatus taskStatus = TaskStatus.fromCode(task.getId(), status);
    Assert.assertFalse(abstractTierRemoteTaskRunner.reportStatus(taskStatus));
    Assert.assertTrue(abstractTierRemoteTaskRunner.reportStatus(taskStatus));
    final ListenableFuture<TaskStatus> future = abstractTierRemoteTaskRunner.run(task);

    Assert.assertEquals(status, future.get(10, TimeUnit.MILLISECONDS).getStatusCode());

    Assert.assertTrue(taskIdCapture.hasCaptured());
    Assert.assertEquals(task.getId(), taskIdCapture.getValue());
    EasyMock.verify(taskStorage);

    final ListenableFuture<TaskStatus> future2 = abstractTierRemoteTaskRunner.run(task);
    Assert.assertEquals(future, future2);
    Assert.assertEquals(status, future2.get(10, TimeUnit.MILLISECONDS).getStatusCode());
    abstractTierRemoteTaskRunner.stop(); // To clear the IDs
  }


  @Test
  public void testStatusUpdateMismatch() throws InterruptedException, ExecutionException, TimeoutException
  {
    final ListenableFuture<TaskStatus> future = submit();

    Assert.assertTrue(((SettableFuture<TaskStatus>) future).set(TaskStatus.success(task.getId())));

    testOnlyTasks(abstractTierRemoteTaskRunner.getKnownTasks(), ImmutableSet.of(task.getId()));
    Assert.assertTrue(abstractTierRemoteTaskRunner.reportStatus(TaskStatus.failure(task.getId())));

    Assert.assertEquals(TaskStatus.Status.SUCCESS, future.get(10, TimeUnit.MILLISECONDS).getStatusCode());

    final ListenableFuture<TaskStatus> future2 = abstractTierRemoteTaskRunner.run(task);
    Assert.assertEquals(future, future2);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, future2.get(10, TimeUnit.MILLISECONDS).getStatusCode());
  }

  @Test
  public void testSimpleStreamLog() throws IOException
  {
    useDiscoverySingleNode();
    final ListenableFuture<TaskStatus> future = submit();

    final String testLog = "this is a log test";

    final Optional<ByteSource> byteSourceOptional = abstractTierRemoteTaskRunner.streamTaskLog(task.getId(), 0);
    Assert.assertTrue(byteSourceOptional.isPresent());

    // Request is lazy
    final Capture<Request> requestCapture = Capture.newInstance();
    final Capture<InputStreamResponseHandler> handlerCapture = Capture.newInstance();
    final SettableFuture<InputStream> clientFuture = SettableFuture.create();
    final ByteArrayInputStream bais = new ByteArrayInputStream(StringUtils.toUtf8(testLog));
    Assert.assertTrue(clientFuture.set(bais));
    EasyMock.resetToStrict(client);
    EasyMock.expect(client.go(
        EasyMock.capture(requestCapture),
        EasyMock.capture(handlerCapture),
        EasyMock.eq(Duration.parse("PT120s"))
    )).andReturn(
        clientFuture
    ).once();
    EasyMock.replay(client);
    Assert.assertEquals(testLog, byteSourceOptional.get().asCharSource(Charsets.UTF_8).read());
    Assert.assertTrue(requestCapture.hasCaptured());
    Assert.assertTrue(handlerCapture.hasCaptured());
    EasyMock.verify(client);
  }

  @Test
  public void testStreamLogInterrupted() throws Exception
  {
    final InterruptedException ex = new InterruptedException("test exception");
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    useDiscoverySingleNode();
    final ListenableFuture<TaskStatus> future = submit();

    final String testLog = "this is a log test";

    final Optional<ByteSource> byteSourceOptional = abstractTierRemoteTaskRunner.streamTaskLog(task.getId(), 0);
    Assert.assertTrue(byteSourceOptional.isPresent());

    // Request is lazy
    final Capture<Request> requestCapture = Capture.newInstance();
    final Capture<InputStreamResponseHandler> handlerCapture = Capture.newInstance();
    final ListenableFuture<InputStream> clientFuture = EasyMock.createStrictMock(ListenableFuture.class);
    EasyMock.expect(clientFuture.get()).andThrow(ex).once();
    EasyMock.replay(clientFuture);
    EasyMock.resetToStrict(client);
    EasyMock.expect(client.go(
        EasyMock.capture(requestCapture),
        EasyMock.capture(handlerCapture),
        EasyMock.eq(Duration.parse("PT120s"))
    )).andReturn(clientFuture).once();
    EasyMock.replay(client);
    Assert.assertEquals(testLog, byteSourceOptional.get().asCharSource(Charsets.UTF_8).read());
    Assert.assertTrue(requestCapture.hasCaptured());
    Assert.assertTrue(handlerCapture.hasCaptured());
    EasyMock.verify(client);
    EasyMock.verify(clientFuture);
  }


  @Test
  public void testStreamLogExecutionIOException() throws Exception
  {
    final IOException ex = new IOException("test exception");
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    useDiscoverySingleNode();
    final ListenableFuture<TaskStatus> future = submit();

    final String testLog = "this is a log test";

    final Optional<ByteSource> byteSourceOptional = abstractTierRemoteTaskRunner.streamTaskLog(task.getId(), 0);
    Assert.assertTrue(byteSourceOptional.isPresent());

    // Request is lazy
    final Capture<Request> requestCapture = Capture.newInstance();
    final Capture<InputStreamResponseHandler> handlerCapture = Capture.newInstance();
    final SettableFuture<InputStream> clientFuture = SettableFuture.create();
    clientFuture.setException(ex);
    EasyMock.resetToStrict(client);
    EasyMock.expect(client.go(
        EasyMock.capture(requestCapture),
        EasyMock.capture(handlerCapture),
        EasyMock.eq(Duration.parse("PT120s"))
    )).andReturn(clientFuture).once();
    EasyMock.replay(client);
    Assert.assertEquals(testLog, byteSourceOptional.get().asCharSource(Charsets.UTF_8).read());
    Assert.assertTrue(requestCapture.hasCaptured());
    Assert.assertTrue(handlerCapture.hasCaptured());
    EasyMock.verify(client);
  }


  @Test
  public void testStreamLogExecutionException() throws Exception
  {
    final Exception ex = new Exception("test exception");
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    useDiscoverySingleNode();
    final ListenableFuture<TaskStatus> future = submit();

    final String testLog = "this is a log test";

    final Optional<ByteSource> byteSourceOptional = abstractTierRemoteTaskRunner.streamTaskLog(task.getId(), 0);
    Assert.assertTrue(byteSourceOptional.isPresent());

    // Request is lazy
    final Capture<Request> requestCapture = Capture.newInstance();
    final Capture<InputStreamResponseHandler> handlerCapture = Capture.newInstance();
    final SettableFuture<InputStream> clientFuture = SettableFuture.create();
    clientFuture.setException(ex);
    EasyMock.resetToStrict(client);
    EasyMock.expect(client.go(
        EasyMock.capture(requestCapture),
        EasyMock.capture(handlerCapture),
        EasyMock.eq(Duration.parse("PT120s"))
    )).andReturn(clientFuture).once();
    EasyMock.replay(client);
    Assert.assertEquals(testLog, byteSourceOptional.get().asCharSource(Charsets.UTF_8).read());
    Assert.assertTrue(requestCapture.hasCaptured());
    Assert.assertTrue(handlerCapture.hasCaptured());
    EasyMock.verify(client);
  }


  @Test
  public void testStreamLogExecutionRuntimeException() throws Exception
  {
    final RuntimeException ex = new RuntimeException("test exception");
    expectedException.expect(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o == ex;
      }

      @Override
      public void describeTo(Description description)
      {

      }
    });
    useDiscoverySingleNode();
    final ListenableFuture<TaskStatus> future = submit();

    final String testLog = "this is a log test";

    final Optional<ByteSource> byteSourceOptional = abstractTierRemoteTaskRunner.streamTaskLog(task.getId(), 0);
    Assert.assertTrue(byteSourceOptional.isPresent());

    // Request is lazy
    final Capture<Request> requestCapture = Capture.newInstance();
    final Capture<InputStreamResponseHandler> handlerCapture = Capture.newInstance();
    final SettableFuture<InputStream> clientFuture = SettableFuture.create();
    clientFuture.setException(ex);
    EasyMock.resetToStrict(client);
    EasyMock.expect(client.go(
        EasyMock.capture(requestCapture),
        EasyMock.capture(handlerCapture),
        EasyMock.eq(Duration.parse("PT120s"))
    )).andReturn(clientFuture).once();
    EasyMock.replay(client);
    Assert.assertEquals(testLog, byteSourceOptional.get().asCharSource(Charsets.UTF_8).read());
    Assert.assertTrue(requestCapture.hasCaptured());
    Assert.assertTrue(handlerCapture.hasCaptured());
    EasyMock.verify(client);
  }


  @Test
  public void testStreamMissingLog() throws IOException
  {
    EasyMock.reset(tierTaskDiscovery);
    EasyMock.expect(tierTaskDiscovery.getNodeForTask(EasyMock.anyString()))
            .andReturn(Optional.<DruidNode>absent()).once();
    EasyMock.replay(tierTaskDiscovery);
    Assert.assertFalse(abstractTierRemoteTaskRunner.streamTaskLog("does not exist", 0).isPresent());
    EasyMock.verify(tierTaskDiscovery);
  }

  @Test
  public void testMultipleUpdatesTaskIDs() throws InterruptedException, ExecutionException
  {
    final int numThreads = 10;
    final int numRepeats = 10;
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            numThreads,
            "listeningFork-%s"
        )
    );
    final List<ListenableFuture<?>> futureList = Lists.newArrayListWithExpectedSize(numThreads);
    final CountDownLatch ready = new CountDownLatch(numThreads);
    final CountDownLatch start = new CountDownLatch(1);
    final CountDownLatch done = new CountDownLatch(numThreads);

    useDiscoveryMap();

    for (int i = 0; i < numThreads; ++i) {
      futureList.add(
          service.submit(
              new Runnable()
              {
                @Override
                public void run()
                {
                  try {
                    ready.countDown();
                    start.await();
                    for (int j = 0; j < numRepeats; ++j) {
                      abstractTierRemoteTaskRunner.refreshTaskIds();
                    }
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                  }
                  finally {
                    done.countDown();
                  }
                }
              }
          )
      );
    }
    try {
      ready.await(1, TimeUnit.SECONDS);
      start.countDown();
      done.await(10, TimeUnit.SECONDS);
    }
    finally {
      service.shutdownNow();
    }
    // Shake out any errors
    Futures.allAsList(futureList).get();
    EasyMock.verify(tierTaskDiscovery);

    testOnlyTasks(abstractTierRemoteTaskRunner.getKnownTasks(), ImmutableSet.of(task.getId()));
  }

  @Test
  public void testSimpleRefreshTaskStatus() throws ExecutionException, InterruptedException
  {
    Assert.assertTrue(abstractTierRemoteTaskRunner.getKnownTasks().isEmpty());

    useDiscoveryMap();

    abstractTierRemoteTaskRunner.refreshTaskIds();
    testOnlyTasks(abstractTierRemoteTaskRunner.getKnownTasks(), ImmutableSet.of(task.getId()));

    final SettableFuture<Boolean> future = SettableFuture.create();
    future.set(true);

    final Capture<Request> requestCapture = Capture.newInstance();
    final Capture<HttpResponseHandler<Boolean, Boolean>> responseHandlerCapture = Capture.newInstance();
    EasyMock.reset(client);
    EasyMock.expect(client.go(EasyMock.capture(requestCapture), EasyMock.capture(responseHandlerCapture)))
            .andReturn(future)
            .once();
    EasyMock.replay(client);
    abstractTierRemoteTaskRunner.refreshTaskStatus().get();

    testOnlyTasks(abstractTierRemoteTaskRunner.getKnownTasks(), ImmutableSet.of(task.getId()));

    Assert.assertTrue(requestCapture.hasCaptured());
    Assert.assertTrue(responseHandlerCapture.hasCaptured());
    final Request request = requestCapture.getValue();
    Assert.assertEquals(
        String.format("http://%s:%d%s", workerNode.getHost(), workerNode.getPort(), TierRunningCheckResource.PATH),
        request.getUrl().toString()
    );
    Assert.assertEquals(HttpMethod.GET, request.getMethod());

    Assert.assertTrue(responseHandlerCapture.hasCaptured());
    final HttpResponseHandler<Boolean, Boolean> responseHandler = responseHandlerCapture.getValue();
    ClientResponse<Boolean> clientResponse = responseHandler.handleResponse(
        new DefaultHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.OK
        )
    );
    Assert.assertTrue(clientResponse.isFinished());
    clientResponse = responseHandler.handleResponse(
        new DefaultHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.NOT_FOUND
        )
    );
    Assert.assertTrue(clientResponse.isFinished());

    responseHandler.done(ClientResponse.finished(true));
    onlyRunning();

    responseHandler.done(ClientResponse.finished(false));
    onlyKnown();
  }


  @Test
  public void testRefreshTaskStatusMalformedURL() throws ExecutionException, InterruptedException
  {
    expectedException.expectCause(new BaseMatcher<Throwable>()
    {
      @Override
      public boolean matches(Object o)
      {
        return o instanceof MalformedURLException;
      }

      @Override
      public void describeTo(Description description)
      {
        description.appendText("MalformedURLException");
      }
    });

    Assert.assertTrue(abstractTierRemoteTaskRunner.getKnownTasks().isEmpty());

    useDiscoveryMap();

    EasyMock.reset(tierTaskDiscovery);
    EasyMock.expect(tierTaskDiscovery.getTasks())
            .andReturn(
                ImmutableMap.<String, DruidNode>of(
                    task.getId(),
                    new DruidNode("service", "localhost", -2)
                )
            ).anyTimes();
    EasyMock.replay(tierTaskDiscovery);

    abstractTierRemoteTaskRunner.refreshTaskIds();
    testOnlyTasks(abstractTierRemoteTaskRunner.getKnownTasks(), ImmutableSet.of(task.getId()));

    abstractTierRemoteTaskRunner.refreshTaskStatus().get();
  }

  @Test
  public void testRefreshTaskIdLostTask() throws ExecutionException, InterruptedException
  {
    noKnown();

    useDiscoveryMap();

    abstractTierRemoteTaskRunner.refreshTaskIds();

    onlyRunning();

    EasyMock.verify(tierTaskDiscovery);
    EasyMock.reset(tierTaskDiscovery);
    EasyMock.expect(tierTaskDiscovery.getTasks()).andReturn(ImmutableMap.<String, DruidNode>of()).anyTimes();
    EasyMock.replay(tierTaskDiscovery);

    abstractTierRemoteTaskRunner.refreshTaskIds();

    onlyKnown();

    Assert.assertEquals(
        StatefulTaskRunnerWorkItem.State.UNKNOWN,
        ((StatefulTaskRunnerWorkItem) Iterables.getOnlyElement(abstractTierRemoteTaskRunner.getKnownTasks())).state
            .get()
    );
  }

  @Test
  public void testFutureUpdatesRunState() throws ExecutionException, InterruptedException
  {
    final String taskId = task.getId();

    final ListenableFuture<TaskStatus> future = submit();

    onlyPending();

    useDiscoveryMap();

    abstractTierRemoteTaskRunner.refreshTaskIds();
    onlyRunning();

    useGeneralClient();

    abstractTierRemoteTaskRunner.refreshTaskStatus().get();

    onlyRunning();

    Assert.assertTrue(((SettableFuture<TaskStatus>) future).set(TaskStatus.success(taskId)));

    onlyKnown();
  }

  @Test
  public void testSubmitHighlander() throws InterruptedException, ExecutionException, TimeoutException
  {
    final Task task = NoopTask.create();
    final int numThreads = 10;
    final ListeningExecutorService service = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(
            numThreads,
            "testSubmitter-%s"
        )
    );
    final List<ListenableFuture<ListenableFuture<TaskStatus>>> futures = new ArrayList<>();
    final CountDownLatch ready = new CountDownLatch(numThreads);
    final CountDownLatch go = new CountDownLatch(1);

    for (int i = 0; i < numThreads; ++i) {
      futures.add(
          service.submit(
              new Callable<ListenableFuture<TaskStatus>>()
              {
                @Override
                public ListenableFuture<TaskStatus> call() throws Exception
                {
                  ready.countDown();
                  try {
                    go.await();
                  }
                  catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                  }
                  return abstractTierRemoteTaskRunner.run(task);
                }
              }
          )
      );
    }
    ready.await(1, TimeUnit.SECONDS);
    go.countDown();
    final List<ListenableFuture<TaskStatus>> futureList = Futures.allAsList(futures).get(10, TimeUnit.SECONDS);
    final ListenableFuture<TaskStatus> future = abstractTierRemoteTaskRunner.run(task);
    for (ListenableFuture<TaskStatus> other : futureList) {
      Assert.assertEquals(future, other);
    }
  }

  @Test
  public void testUnknownShutdown() throws ExecutionException, InterruptedException
  {
    EasyMock.reset(tierTaskDiscovery);
    EasyMock.expect(tierTaskDiscovery.getTasks()).andReturn(ImmutableMap.<String, DruidNode>of()).once();
    EasyMock.replay(tierTaskDiscovery);
    abstractTierRemoteTaskRunner.shutdown(task.getId());
    final ListenableFuture<TaskStatus> future = abstractTierRemoteTaskRunner.run(task);
    Assert.assertTrue(future.get().isFailure());
  }

  @Test
  public void testStartStop()
  {
    useDiscoveryMap();
    useGeneralClient();
    abstractTierRemoteTaskRunner.start();
    onlyRunning();
    EasyMock.verify(tierTaskDiscovery);

    abstractTierRemoteTaskRunner.stop();
    noKnown();

    EasyMock.reset(tierTaskDiscovery);
    EasyMock.expect(tierTaskDiscovery.getTasks()).andReturn(ImmutableMap.<String, DruidNode>of()).once();
    EasyMock.replay(tierTaskDiscovery);
    abstractTierRemoteTaskRunner.start();
    noKnown();
  }

  @Test
  public void testInjection()
  {
    final String propName = "druid.indexer.runner.type";
    final String prior = System.getProperty(propName);
    try {
      System.setProperty(propName, "routing");
      final CliOverlord overlord = new CliOverlord();
      final Injector injector = Initialization.makeInjectorWithModules(
          GuiceInjectors.makeStartupInjector(),
          ImmutableList.of(Modules.override(overlord.getModules()).with(new Module()
          {
            @Override
            public void configure(Binder binder)
            {
              binder.bind(ScheduledExecutorService.class).toInstance(executorService);
            }
          }))
      );
      final TaskRunnerFactory factory = injector.getInstance(TaskRunnerFactory.class);
      Assert.assertTrue(factory instanceof TierRoutingTaskRunnerFactory);
      Assert.assertNotNull(factory.build());
    }
    finally {
      if (prior == null) {
        System.clearProperty(propName);
      } else {
        System.setProperty(propName, prior);
      }
    }
  }

  @After
  public void generalVerify()
  {
    try {
      EasyMock.verify(client);
      EasyMock.verify(tierTaskDiscovery);
      EasyMock.verify(taskStorage);
      abstractTierRemoteTaskRunner.stop();
      noKnown();
    }
    finally {
      // Clear any potential interrupts
      Thread.interrupted();
    }
  }


  private SettableFuture<TaskStatus> submit()
  {
    final ListenableFuture<TaskStatus> future = abstractTierRemoteTaskRunner.run(task);
    Assert.assertEquals(SettableFuture.class, future.getClass());
    Assert.assertTrue(taskCapture.hasCaptured());
    Assert.assertEquals(task, taskCapture.getValue());
    return (SettableFuture<TaskStatus>) future;
  }

  private static void testOnlyTasks(final Collection<? extends TaskRunnerWorkItem> workItems, final Set<String> taskIds)
  {
    final Set<String> workIds = ImmutableSet.copyOf(
        Collections2.transform(
            workItems, new Function<TaskRunnerWorkItem, String>()
            {
              @Nullable
              @Override
              public String apply(TaskRunnerWorkItem taskRunnerWorkItem)
              {
                return taskRunnerWorkItem.getTaskId();
              }
            }
        )
    );
    Assert.assertEquals("exactly expected tasks not correct", taskIds, workIds);
  }
}
