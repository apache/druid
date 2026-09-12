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
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelException;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.timeout.ReadTimeoutException;
import org.apache.druid.data.input.ResourceInputSource;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.ClientResponse;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.query.Druids;
import org.apache.druid.query.NestedDataTestUtils;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
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
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

public class DirectDruidClientTest
{
  @RegisterExtension
  public static QueryStackTests.ConglomerateExtension conglomerateRule = new QueryStackTests.ConglomerateExtension();

  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

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

  @Test
  public void testHtml503InInitialResponseIsCapacityExceeded()
  {
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(HttpResponseStatus.SERVICE_UNAVAILABLE, "text/html", "<html><body>503 Service Unavailable</body></html>")
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryCapacityExceededException e = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertTrue(e.getMessage().contains("status[503]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("contentType[text/html]"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("<html>"), e.getMessage());
  }

  @Test
  public void testHtml503InLaterChunkAfterEmptyInitialBodyIsCapacityExceeded()
  {
    // A chunked 503 whose initial HttpResponse carries an empty body: the HTML only shows up in a later chunk, and
    // must still be classified as capacity-exceeded rather than reaching the JSON parser or being reported as a
    // generic HTML-instead-of-JSON error.
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(HttpResponseStatus.SERVICE_UNAVAILABLE, null, "", "  \n", "<html><body>503</body></html>")
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryCapacityExceededException e = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertTrue(e.getMessage().contains("status[503]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("detected in chunk[2]"), e.getMessage());
  }

  @Test
  public void testEmptyBody503IsCapacityExceeded()
  {
    // A proxy answering 503 with no body at all. Netty skips handleChunk for an empty LastHttpContent, so the body
    // prefix never resolves and done() is the only place left to classify the response; without that it completes
    // an empty stream that JsonParserIterator later reports as a generic EOF instead of capacity-exceeded.
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(HttpResponseStatus.SERVICE_UNAVAILABLE, null)
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryCapacityExceededException e = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertTrue(e.getMessage().contains("status[503]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("bodyLength[0]"), e.getMessage());
  }

  @Test
  public void testWhitespaceOnlyBody429IsCapacityExceeded()
  {
    // Whitespace-only chunks never resolve the prefix either, so the same end-of-response classification applies.
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(HttpResponseStatus.TOO_MANY_REQUESTS, "text/plain", "  \n", "\t")
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryCapacityExceededException e = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertTrue(e.getMessage().contains("status[429]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("contentType[text/plain]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("bodyLength[4]"), e.getMessage());
  }

  @Test
  public void testEmptyBody503RoutedThroughExceptionCaughtIsCapacityExceeded()
  {
    // Same lifecycle as testHtml503InLaterChunkAfterFinishedInitialResponseIsCapacityExceeded: the future is already
    // completed by handleResponse, so the end-of-response classification can only reach the caller through
    // exceptionCaught and the vended stream, and must keep its type on the way.
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(HttpResponseStatus.SERVICE_UNAVAILABLE, null, true)
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryCapacityExceededException e = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> client.run(queryPlus, responseContext).toList()
    );
    Assertions.assertTrue(e.getMessage().contains("status[503]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("bodyLength[0]"), e.getMessage());
  }

  @Test
  public void testEmptyBody200IsNotShortCircuited()
  {
    // A successful response with no body keeps completing normally; only 429/503 are classified at end of response.
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(HttpResponseStatus.OK, null)
    );

    final QueryPlus queryPlus = getQueryPlus();
    Assertions.assertDoesNotThrow(() -> client.run(queryPlus, responseContext));
  }

  @Test
  public void testHtmlInLaterChunkOf200ResponseIsQueryInterrupted()
  {
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(HttpResponseStatus.OK, null, "", "<html><body>oops</body></html>")
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryInterruptedException e = Assertions.assertThrows(
        QueryInterruptedException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertTrue(e.getMessage().contains("returned HTML response instead of JSON"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("detected in chunk[1]"), e.getMessage());
  }

  @Test
  public void testPlainText503IsCapacityExceeded()
  {
    // Not every proxy error page is HTML: Envoy, for one, returns a plain-text body with a 503. The body itself is
    // never echoed into the message (see testNonJsonBodyMessageContainsNoRawBodyBytes); this checks the status and
    // Content-Type metadata that stands in for it.
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(HttpResponseStatus.SERVICE_UNAVAILABLE, "text/plain", "upstream connect error or disconnect/reset before headers")
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryCapacityExceededException e = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertTrue(e.getMessage().contains("status[503]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("contentType[text/plain]"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("upstream connect error"), e.getMessage());
  }

  @Test
  public void testNonJsonBodyMessageContainsNoRawBodyBytes()
  {
    // The exception message must never echo the upstream body itself: the broker-to-data-server response is not a
    // trusted boundary, and this message is logged by JsonParserIterator and can reach the query error/trailer. It
    // is limited to bounded sanitized metadata instead: HTTP status, Content-Type, and body length.
    final String body = "upstream down\r\nX-Injected: evil\nsecond line";
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(
            HttpResponseStatus.SERVICE_UNAVAILABLE,
            "text/plain",
            body
        )
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryCapacityExceededException e = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> client.run(queryPlus, responseContext)
    );
    Assertions.assertFalse(e.getMessage().contains("upstream down"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("X-Injected"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("second line"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("\r"), e.getMessage());
    Assertions.assertFalse(e.getMessage().contains("\n"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("status[503]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("contentType[text/plain]"), e.getMessage());
    Assertions.assertTrue(
        e.getMessage().contains("bodyLength[" + StringUtils.toUtf8(body).length + "]"),
        e.getMessage()
    );
  }

  @Test
  public void testJson503IsNotShortCircuited()
  {
    // A 503 carrying Druid's own JSON error body must take the normal JSON error path so the server's message
    // survives, instead of being replaced by a synthesized capacity-exceeded error.
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(
            HttpResponseStatus.SERVICE_UNAVAILABLE,
            "application/json",
            "{\"error\":\"Unknown exception\",\"errorMessage\":\"backend says no\",\"errorClass\":\"x\",\"host\":\"h\"}"
        )
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryException e = Assertions.assertThrows(
        QueryException.class,
        () -> client.run(queryPlus, responseContext).toList()
    );
    Assertions.assertFalse(e instanceof QueryCapacityExceededException, e.getClass().getName());
    Assertions.assertEquals("backend says no", e.getMessage());
  }

  @Test
  public void testSmile503IsNotShortCircuited() throws IOException
  {
    // Same as testJson503IsNotShortCircuited, but for the Smile ObjectMapper DirectDruidClientFactory actually
    // injects in production. A Smile-encoded structured error body starts with the Smile format header byte (0x3a),
    // not '{'/'[', and must not be misclassified as a non-JSON proxy error page.
    final ObjectMapper smileObjectMapper = new DefaultObjectMapper(new SmileFactory(), null);
    final byte[] smileBody = smileObjectMapper.writeValueAsBytes(
        new QueryException("Unknown exception", "backend says no", "x", "h")
    );
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(
            HttpResponseStatus.SERVICE_UNAVAILABLE,
            SmileMediaTypes.APPLICATION_JACKSON_SMILE,
            false,
            smileBody
        ),
        smileObjectMapper
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryException e = Assertions.assertThrows(
        QueryException.class,
        () -> client.run(queryPlus, responseContext).toList()
    );
    Assertions.assertFalse(e instanceof QueryCapacityExceededException, e.getClass().getName());
    Assertions.assertEquals("backend says no", e.getMessage());
  }

  @Test
  public void testHtml503InLaterChunkAfterFinishedInitialResponseIsCapacityExceeded()
  {
    // Reproduces the real NettyHttpClient lifecycle instead of the simplified one the other later-chunk tests use:
    // handleResponse always returns an already-finished ClientResponse, so the transport's future is completed
    // before any chunk is seen, and a later chunk's exception can only reach the caller through exceptionCaught.
    // Before the fix, that path re-wrapped the QueryCapacityExceededException thrown by handleChunk into a plain
    // RE (or, via the queued failure InputStream, an IOException), losing the original type entirely.
    final DirectDruidClient client = makeDirectDruidClient(
        new ScriptedHttpClient(
            HttpResponseStatus.SERVICE_UNAVAILABLE,
            null,
            true,
            StringUtils.toUtf8(""),
            StringUtils.toUtf8("<html><body>503</body></html>")
        )
    );

    final QueryPlus queryPlus = getQueryPlus();
    final QueryCapacityExceededException e = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> client.run(queryPlus, responseContext).toList()
    );
    Assertions.assertTrue(e.getMessage().contains("status[503]"), e.getMessage());
    Assertions.assertTrue(e.getMessage().contains("detected in chunk[1]"), e.getMessage());
  }

  @Test
  public void testLaterChunkQueryCapacityExceededReportsSameHostAsInitialResponse()
  {
    // DirectDruidClient rethrows a later-chunk QueryCapacityExceededException as itself (see the previous test), but
    // it constructs that exception with QueryCapacityExceededException.withErrorMessageAndResolvedHost(), whose host
    // is the data server's own locally resolved hostname (or null). An initial-response QueryCapacityExceededException
    // arriving as Druid's own structured JSON error body (the case testJson503IsNotShortCircuited also exercises, as
    // opposed to the failIfNonJsonBody HTML/non-JSON shortcut, which throws synchronously out of handleResponse and
    // never reaches JsonParserIterator at all) is deserialized by JsonParserIterator.init()'s START_OBJECT branch and
    // normalized by convertException to DirectDruidClient.host. Unless the later-chunk exception is also routed
    // through convertException, the two report different hosts for the same logical failure. This asserts they
    // match, both landing on DirectDruidClient's own configured host (hostName), not the data server's.
    final DirectDruidClient initialResponseClient = makeDirectDruidClient(
        new ScriptedHttpClient(
            HttpResponseStatus.SERVICE_UNAVAILABLE,
            "application/json",
            "{\"error\":\"Query capacity exceeded\",\"errorMessage\":\"too many queries\","
            + "\"errorClass\":\"org.apache.druid.query.QueryCapacityExceededException\",\"host\":\"data-server-01:8100\"}"
        )
    );
    final QueryCapacityExceededException initialResponseException = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> initialResponseClient.run(getQueryPlus(), responseContext).toList()
    );

    final DirectDruidClient laterChunkClient = makeDirectDruidClient(
        new ScriptedHttpClient(
            HttpResponseStatus.SERVICE_UNAVAILABLE,
            null,
            true,
            StringUtils.toUtf8(""),
            StringUtils.toUtf8("<html><body>503</body></html>")
        )
    );
    final QueryCapacityExceededException laterChunkException = Assertions.assertThrows(
        QueryCapacityExceededException.class,
        () -> laterChunkClient.run(getQueryPlus(), responseContext).toList()
    );

    Assertions.assertEquals(hostName, initialResponseException.getHost());
    Assertions.assertEquals(hostName, laterChunkException.getHost());
    Assertions.assertEquals(initialResponseException.getHost(), laterChunkException.getHost());
  }

  @Test
  public void testMidStreamTransportFailureIsNotRethrownRaw()
  {
    // A mid-stream disconnect is not a query error: it reaches DirectDruidClient through exceptionCaught as a plain
    // transport RuntimeException (Netty's ChannelException here), after handleResponse has already vended the stream.
    // Rethrowing the cause is scoped to QueryException precisely so this case keeps the pre-existing behaviour of
    // surfacing the "Query[id] url[url] failed with exception msg [...]" wrapper, which is the only place the query
    // id and the target url appear at all, instead of a bare ChannelException that carries neither.
    final DirectDruidClient client = makeDirectDruidClient(
        new TransportFailureHttpClient(new ChannelException("connection reset by peer"))
    );

    final RE e = Assertions.assertThrows(
        RE.class,
        () -> client.run(getQueryPlus(), responseContext).toList()
    );
    Assertions.assertTrue(
        e.getMessage().contains("failed with exception msg [connection reset by peer]"),
        e.getMessage()
    );
    Assertions.assertTrue(e.getMessage().contains(hostName), e.getMessage());
  }

  /**
   * An {@link HttpClient} that completes {@link HttpResponseHandler#handleResponse} normally, so the caller is handed
   * a stream, and then reports a transport failure through {@link HttpResponseHandler#exceptionCaught} the way
   * NettyHttpClient does when a connection drops mid-response.
   */
  private static class TransportFailureHttpClient implements HttpClient
  {
    private final RuntimeException transportFailure;

    TransportFailureHttpClient(RuntimeException transportFailure)
    {
      this.transportFailure = transportFailure;
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(Request request, HttpResponseHandler<Intermediate, Final> handler)
    {
      return go(request, handler, null);
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler,
        Duration readTimeout
    )
    {
      final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
      response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "application/json");
      final ClientResponse<Intermediate> clientResponse =
          handler.handleResponse(response, TestHttpClient.NOOP_TRAFFIC_COP);
      handler.exceptionCaught(clientResponse, transportFailure);
      @SuppressWarnings("unchecked")
      final Final alreadyCompletedResult = (Final) clientResponse.getObj();
      return Futures.immediateFuture(alreadyCompletedResult);
    }
  }

  /**
   * An {@link HttpClient} that feeds the handler a scripted response synchronously: the initial {@link HttpResponse}
   * carries only headers and every element of {@code bodies} is delivered as an {@link HttpContent}.
   * <p>
   * By default, an exception thrown out of {@code handleChunk} is simply allowed to propagate out of {@link #go},
   * which is adequate for testing the classification logic itself. Constructing with
   * {@code routeChunkExceptionsThroughExceptionCaught=true} instead mirrors what
   * {@code NettyHttpClient#messageReceived} actually does: since {@link DirectDruidClient#handleResponse} always
   * returns an already-{@link ClientResponse#isFinished() finished} response, the future is completed with that
   * object up front, and a later chunk's exception can only reach the handler via
   * {@link HttpResponseHandler#exceptionCaught}, not by replacing the future's result.
   */
  private static class ScriptedHttpClient implements HttpClient
  {
    private final HttpResponseStatus status;
    private final String contentType;
    private final boolean routeChunkExceptionsThroughExceptionCaught;
    private final byte[][] bodies;

    ScriptedHttpClient(HttpResponseStatus status, String contentType, String... bodies)
    {
      this(status, contentType, false, Arrays.stream(bodies).map(StringUtils::toUtf8).toArray(byte[][]::new));
    }

    ScriptedHttpClient(
        HttpResponseStatus status,
        String contentType,
        boolean routeChunkExceptionsThroughExceptionCaught,
        byte[]... bodies
    )
    {
      this.status = status;
      this.contentType = contentType;
      this.routeChunkExceptionsThroughExceptionCaught = routeChunkExceptionsThroughExceptionCaught;
      this.bodies = bodies;
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(Request request, HttpResponseHandler<Intermediate, Final> handler)
    {
      return go(request, handler, null);
    }

    @Override
    public <Intermediate, Final> ListenableFuture<Final> go(
        Request request,
        HttpResponseHandler<Intermediate, Final> handler,
        Duration readTimeout
    )
    {
      final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
      if (contentType != null) {
        response.headers().set(HttpHeaders.Names.CONTENT_TYPE, contentType);
      }
      ClientResponse<Intermediate> clientResponse = handler.handleResponse(response, TestHttpClient.NOOP_TRAFFIC_COP);
      final boolean initialResponseFinished = clientResponse.isFinished();
      final Intermediate initialResponseObj = clientResponse.getObj();
      // Netty 4 carries no body on the initial HttpResponse, so every element of bodies is delivered as an
      // HttpContent, including the first. chunkNum therefore starts at 0.
      for (int i = 0; i < bodies.length; i++) {
        final HttpContent chunk = new DefaultHttpContent(Unpooled.wrappedBuffer(bodies[i]));
        if (!routeChunkExceptionsThroughExceptionCaught) {
          clientResponse = handler.handleChunk(clientResponse, chunk, i);
          continue;
        }
        try {
          clientResponse = handler.handleChunk(clientResponse, chunk, i);
        }
        catch (RuntimeException e) {
          handler.exceptionCaught(clientResponse, e);
          if (initialResponseFinished) {
            // retVal was already completed by handleResponse; the caller only learns about this exception by
            // reading the already-vended stream, same as real NettyHttpClient.
            @SuppressWarnings("unchecked")
            final Final alreadyCompletedResult = (Final) initialResponseObj;
            return Futures.immediateFuture(alreadyCompletedResult);
          }
          throw e;
        }
      }
      if (!routeChunkExceptionsThroughExceptionCaught) {
        return Futures.immediateFuture(handler.done(clientResponse).getObj());
      }
      try {
        return Futures.immediateFuture(handler.done(clientResponse).getObj());
      }
      catch (RuntimeException e) {
        // NettyHttpClient's finishRequest() sits inside the same catch as handleChunk, so an exception from done()
        // also reaches the handler through exceptionCaught after the initial response has completed the future.
        handler.exceptionCaught(clientResponse, e);
        if (initialResponseFinished) {
          @SuppressWarnings("unchecked")
          final Final alreadyCompletedResult = (Final) initialResponseObj;
          return Futures.immediateFuture(alreadyCompletedResult);
        }
        throw e;
      }
    }
  }

  private DirectDruidClient makeDirectDruidClient(HttpClient httpClient)
  {
    return makeDirectDruidClient(httpClient, objectMapper, new NoopServiceEmitter());
  }

  private DirectDruidClient makeDirectDruidClient(HttpClient httpClient, ServiceEmitter emitter)
  {
    return makeDirectDruidClient(httpClient, objectMapper, emitter);
  }

  private DirectDruidClient makeDirectDruidClient(HttpClient httpClient, ObjectMapper clientObjectMapper)
  {
    return makeDirectDruidClient(httpClient, clientObjectMapper, new NoopServiceEmitter());
  }

  private DirectDruidClient makeDirectDruidClient(HttpClient httpClient, ObjectMapper clientObjectMapper, ServiceEmitter emitter)
  {
    return new DirectDruidClient(
        conglomerateRule.getConglomerate(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        clientObjectMapper,
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
                         .tmpDir(temporaryFolder.newFolder())
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
                         .inputTmpDir(temporaryFolder.newFolder())
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

}
