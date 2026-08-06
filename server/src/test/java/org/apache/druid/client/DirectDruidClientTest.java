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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.Result;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.simulate.BlockingExecutorService;
import org.apache.druid.server.coordinator.simulate.WrappingScheduledExecutorService;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

public class DirectDruidClientTest
{
  @RegisterExtension
  public static QueryStackTests.ConglomerateExtension conglomerateRule = new QueryStackTests.ConglomerateExtension();

  @TempDir
  public File temporaryFolder;

  private final String hostName = "localhost:8080";
  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final ResponseContext responseContext = ResponseContext.createEmpty();

  private WrappingScheduledExecutorService queryCancellationExecutor;
  private BlockingExecutorService blockingExecutorService;

  @BeforeEach
  public void setup()
  {
    responseContext.initialize();
    blockingExecutorService = new BlockingExecutorService("test-druid-client-cancel-executor");
    queryCancellationExecutor = new WrappingScheduledExecutorService(
        "DirectDruidClientTest-%s",
        blockingExecutorService,
        false
    );
  }

  @AfterEach
  public void teardown() throws InterruptedException
  {
    blockingExecutorService.shutdownNow();
    queryCancellationExecutor.shutdown();
    queryCancellationExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testRun() throws Exception
  {
    final URL url = new URL(StringUtils.format("http://%s/druid/v2/", hostName));

    QueuedTestHttpClient queuedHttpClient = new QueuedTestHttpClient();
    DirectDruidClient client1 = makeDirectDruidClient(queuedHttpClient);

    DirectDruidClient client2 = makeDirectDruidClient(queuedHttpClient);

    // Queue first call: pending until we provide a result
    SettableFuture<InputStream> futureResult = SettableFuture.create();
    queuedHttpClient.enqueue(futureResult);
    // Queue second call: will fail with ReadTimeoutException
    SettableFuture<InputStream> futureException = SettableFuture.create();
    queuedHttpClient.enqueue(futureException);
    // Subsequent calls: no enqueue → default pending futures created in client

    QueryPlus queryPlus = getQueryPlus();

    Sequence s1 = client1.run(queryPlus, responseContext);
    List<Request> requests = queuedHttpClient.getRequests();
    Assertions.assertFalse(requests.isEmpty());
    Assertions.assertEquals(url, requests.get(0).getUrl());
    Assertions.assertEquals(HttpMethod.POST, requests.get(0).getMethod());
    Assertions.assertEquals(1, client1.getNumOpenConnections());

    // simulate read timeout on second request
    client1.run(queryPlus, responseContext);
    Assertions.assertEquals(2, client1.getNumOpenConnections());
    futureException.setException(new ReadTimeoutException());
    Assertions.assertEquals(1, client1.getNumOpenConnections());

    // subsequent connections should work (and remain open)
    client1.run(queryPlus, responseContext);
    client1.run(queryPlus, responseContext);
    client1.run(queryPlus, responseContext);
    Assertions.assertEquals(4, client1.getNumOpenConnections());

    // produce result for first connection
    futureResult.set(
        new ByteArrayInputStream(
            StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\", \"result\": 42.0}]")
        )
    );
    List<Result> results = s1.toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(DateTimes.of("2014-01-01T01:02:03Z"), results.get(0).getTimestamp());
    Assertions.assertEquals(3, client1.getNumOpenConnections());

    client2.run(queryPlus, responseContext);
    client2.run(queryPlus, responseContext);
    Assertions.assertEquals(2, client2.getNumOpenConnections());
  }

  @Test
  public void testCancel() throws MalformedURLException
  {
    QueryPlus queryPlus = getQueryPlus();
    TestHttpClient testHttpClient = new TestHttpClient(objectMapper, Futures.immediateCancelledFuture());

    // add a generic server and a cancel query URL
    QueryableIndex index = makeQueryableIndex();
    TestHttpClient.SimpleServerManager simpleServerManager = new TestHttpClient.SimpleServerManager(
        conglomerateRule.getConglomerate(), DataSegment.builder(SegmentId.dummy("test")).build(), index, false
    );
    testHttpClient.addServerAndRunner(
        new DruidServer("test1", hostName, null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        simpleServerManager
    );
    testHttpClient.addUrlAndRunner(
        new URL(StringUtils.format("http://%s/druid/v2/%s", hostName, queryPlus.getQuery().getId())),
        simpleServerManager
    );
    DirectDruidClient client = makeDirectDruidClient(testHttpClient);
    Sequence results = client.run(queryPlus, responseContext);

    Assertions.assertEquals(0, client.getNumOpenConnections());
    QueryInterruptedException actualException =
        Assertions.assertThrows(QueryInterruptedException.class, () -> results.toList());
    Assertions.assertEquals(hostName, actualException.getHost());
    Assertions.assertEquals("Query cancelled", actualException.getErrorCode());
    Assertions.assertEquals("Task was cancelled.", actualException.getCause().getMessage());

    Assertions.assertTrue(blockingExecutorService.hasPendingTasks());
    blockingExecutorService.finishNextPendingTask();
    Assertions.assertTrue(blockingExecutorService.hasPendingTasks());
    ISE observedException = Assertions.assertThrows(ISE.class, () -> blockingExecutorService.finishNextPendingTask());
    Assertions.assertTrue(observedException.getCause() instanceof CancellationException);

  }

  @Test
  public void testQueryInterruptionExceptionLogMessage()
  {
    SettableFuture<Object> interruptionFuture = SettableFuture.create();
    interruptionFuture.set(
        new ByteArrayInputStream(
            StringUtils.toUtf8("{\"error\":\"testing1\",\"errorMessage\":\"testing2\"}")
        )
    );
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient(interruptionFuture));

    interruptionFuture.set(
        new ByteArrayInputStream(StringUtils.toUtf8("{\"error\":\"testing1\",\"errorMessage\":\"testing2\"}"))
    );
    Sequence results = client.run(getQueryPlus(), responseContext);

    QueryInterruptedException actualException =
        Assertions.assertThrows(QueryInterruptedException.class, () -> results.toList());
    Assertions.assertEquals("testing1", actualException.getErrorCode());
    Assertions.assertEquals("testing2", actualException.getMessage());
    Assertions.assertEquals(hostName, actualException.getHost());
  }

  @Test
  public void testQueryTimeoutBeforeFuture() throws IOException
  {
    SettableFuture<Object> timeoutFuture = SettableFuture.create();
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient(timeoutFuture));

    QueryPlus queryPlus = getQueryPlus(Map.of(DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 250));
    Sequence results = client.run(queryPlus, responseContext);

    // Incomplete result set delivered via a pipe to simulate slow stream
    PipedInputStream in = new PipedInputStream();
    final PipedOutputStream out = new PipedOutputStream(in);
    timeoutFuture.set(in);

    QueryTimeoutException actualException = Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> {
          out.write(StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\"}"));
          Thread.sleep(250);
          out.write(StringUtils.toUtf8("]"));
          out.close();
          results.toList();
        }
    );
    Assertions.assertEquals("Query timeout", actualException.getErrorCode());
    Assertions.assertEquals(StringUtils.format("url[http://%s/druid/v2/] timed out", hostName), actualException.getMessage());
    Assertions.assertEquals(hostName, actualException.getHost());
  }

  @Test
  public void testQueryTimeoutFromFuture()
  {
    final SettableFuture<Object> timeoutFuture = SettableFuture.create();
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient(timeoutFuture));

    QueryPlus query = getQueryPlus(Map.of(DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 500));
    Sequence results = client.run(query, responseContext);
    QueryTimeoutException actualException = Assertions.assertThrows(QueryTimeoutException.class, results::toList);
    Assertions.assertEquals("Query timeout", actualException.getErrorCode());
    Assertions.assertEquals(StringUtils.format("Query [%s] timed out!", query.getQuery().getId()), actualException.getMessage());
    Assertions.assertEquals(hostName, actualException.getHost());
  }

  @Test
  public void testQueryTimeoutDuringRunThrowsExceptionImmediately()
  {
    SettableFuture<Object> timeoutFuture = SettableFuture.create();
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient(timeoutFuture));

    QueryPlus queryPlus = getQueryPlus(Map.of(DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis()));
    QueryTimeoutException actualException = Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertEquals("Query timeout", actualException.getErrorCode());
    Assertions.assertEquals(
        StringUtils.format(
            "Query[%s] url[http://%s/druid/v2/] timed out.",
            queryPlus.getQuery().getId(),
            hostName
        ), actualException.getMessage()
    );
  }

  @Test
  public void testQueryTimeoutDuringResponseHandling()
  {
    final TestHttpClient testHttpClient = new TestHttpClient(objectMapper, 110);
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient(testHttpClient, false));

    final QueryPlus queryPlus = getQueryPlus(Map.of(
        QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, 100,
        DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 100
    ));

    QueryTimeoutException actualException = Assertions.assertThrows(
        QueryTimeoutException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertEquals("Query timeout", actualException.getErrorCode());
    Assertions.assertEquals(
        StringUtils.format("Query[%s] url[http://%s/druid/v2/] timed out.",
                           queryPlus.getQuery().getId(),
                           hostName
        ), actualException.getMessage()
    );
  }

  @Test
  public void testConnectionCountAfterException()
  {
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient());

    Assertions.assertThrows(RuntimeException.class, () -> client.run(getQueryPlus(), responseContext));
    Assertions.assertEquals(0, client.getNumOpenConnections());
  }

  @Test
  public void testNodeMetricsEmittedOnSuccess()
  {
    StubServiceEmitter stubEmitter = StubServiceEmitter.createStarted();
    DirectDruidClient client = makeDirectDruidClient(initHttpClientWithSuccessfulQuery(), stubEmitter);

    client.run(getQueryPlus(), responseContext).toList();

    Assertions.assertEquals(1, stubEmitter.getMetricEventCount("query/node/time"));
    Assertions.assertEquals(1, stubEmitter.getMetricEventCount("query/node/bytes"));
  }

  @Test
  public void testNodeMetricsEmittedOnError()
  {
    // Only setupResponseReadFailure fires (checkQueryTimeout during handleResponse) — done() is never called.
    StubServiceEmitter stubEmitter = StubServiceEmitter.createStarted();
    final TestHttpClient testHttpClient = new TestHttpClient(objectMapper, 110);
    DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient(testHttpClient, false), stubEmitter);

    final QueryPlus queryPlus = getQueryPlus(Map.of(
        DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 50
    ));

    Assertions.assertThrows(QueryTimeoutException.class, () -> client.run(queryPlus, responseContext));

    Assertions.assertEquals(1, stubEmitter.getMetricEventCount("query/node/time"));
    Assertions.assertEquals(1, stubEmitter.getMetricEventCount("query/node/bytes"));
  }

  @Test
  public void testNodeMetricsEmittedExactlyOnceWhenDoneAndTimeoutBothFire() throws InterruptedException
  {
    // done() fires synchronously during run(), then results.toList() calls checkQueryTimeout() after the
    // timeout has already expired, triggering setupResponseReadFailure(). The compareAndSet guard must
    // prevent the second emitNodeMetrics() call from emitting.
    StubServiceEmitter stubEmitter = StubServiceEmitter.createStarted();
    DirectDruidClient client = makeDirectDruidClient(initHttpClientWithSuccessfulQuery(), stubEmitter);

    // Timeout far enough in the future that handleResponse + done() complete during run(), but we sleep
    // past it before consuming the sequence so that checkQueryTimeout() fires during toList().
    final QueryPlus queryPlus = getQueryPlus(Map.of(
        DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 500
    ));

    Sequence results = client.run(queryPlus, responseContext);
    Thread.sleep(600);

    Assertions.assertThrows(QueryTimeoutException.class, results::toList);

    Assertions.assertEquals(1, stubEmitter.getMetricEventCount("query/node/time"));
    Assertions.assertEquals(1, stubEmitter.getMetricEventCount("query/node/bytes"));
  }

  @Test
  public void testResourceLimitExceededException()
  {
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientWithSuccessfulQuery());

    final QueryPlus queryPlus = getQueryPlus(Map.of(
        QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, 100,
        DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE
    ));

    ResourceLimitExceededException actualException = Assertions.assertThrows(
        ResourceLimitExceededException.class,
        () -> client.run(queryPlus, responseContext)
    );

    Assertions.assertEquals(
        StringUtils.format(
            "Query[%s] url[http://localhost:8080/druid/v2/] total bytes gathered[127] exceeds maxScatterGatherBytes[100]",
            queryPlus.getQuery().getId()
        ),
        actualException.getMessage());
  }

  private DirectDruidClient makeDirectDruidClient(HttpClient httpClient)
  {
    return makeDirectDruidClient(httpClient, new NoopServiceEmitter());
  }

  private DirectDruidClient makeDirectDruidClient(HttpClient httpClient, ServiceEmitter emitter)
  {
    return new DirectDruidClient(
        conglomerateRule.getConglomerate(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        objectMapper,
        httpClient,
        "http",
        hostName,
        emitter,
        queryCancellationExecutor
    );
  }

  private HttpClient initHttpClientFromExistingClient()
  {
    return initHttpClientFromExistingClient(new TestHttpClient(objectMapper), true);
  }

  private HttpClient initHttpClientWithSuccessfulQuery()
  {
    return initHttpClientFromExistingClient(new TestHttpClient(objectMapper), false);
  }

  private HttpClient initHttpClientFromExistingClient(ListenableFuture future)
  {
    return initHttpClientFromExistingClient(new TestHttpClient(objectMapper, future), false);
  }

  private HttpClient initHttpClientFromExistingClient(TestHttpClient httpClient, boolean throwQueryError)
  {
    final QueryableIndex index = makeQueryableIndex();
    httpClient.addServerAndRunner(
        new DruidServer("test1", hostName, null, 0, null, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
        new TestHttpClient.SimpleServerManager(
            conglomerateRule.getConglomerate(), DataSegment.builder(SegmentId.dummy("test")).build(), index, throwQueryError
        )
    );
    return httpClient;
  }

  private QueryableIndex makeQueryableIndex()
  {
    try {
      return IndexBuilder.create()
                         .tmpDir(newFolder(temporaryFolder, "junit"))
                         .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                         .schema(
                             new IncrementalIndexSchema.Builder()
                                 .withTimestampSpec(NestedDataTestUtils.AUTO_SCHEMA.getTimestampSpec())
                                 .build()
                         )
                         .inputSource(
                             ResourceInputSource.of(
                                 NestedDataTestUtils.class.getClassLoader(),
                                 NestedDataTestUtils.ALL_TYPES_TEST_DATA_FILE
                             )
                         )
                         .inputFormat(TestIndex.DEFAULT_JSON_INPUT_FORMAT)
                         .inputTmpDir(newFolder(temporaryFolder, "junit"))
                         .buildMMappedIndex();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static QueryPlus getQueryPlus()
  {
    return getQueryPlus(Map.of(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE));
  }

  private static QueryPlus getQueryPlus(Map<String, Object> context)
  {
    return QueryPlus.wrap(Druids.newTimeBoundaryQueryBuilder().dataSource("test").context(context).randomQueryId().build());
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    if (subDirs.length == 0 || (subDirs.length == 1 && "junit".equals(subDirs[0]))) {
      return java.nio.file.Files.createTempDirectory(root.toPath(), "junit").toFile();
    }
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
