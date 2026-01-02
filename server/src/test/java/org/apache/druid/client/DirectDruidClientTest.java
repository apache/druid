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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
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
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.timeout.ReadTimeoutException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DirectDruidClientTest
{
  @ClassRule
  public static QueryStackTests.Junit4ConglomerateRule conglomerateRule = new QueryStackTests.Junit4ConglomerateRule();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final String hostName = "localhost:8080";
  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final ResponseContext responseContext = ResponseContext.createEmpty();

  private ScheduledExecutorService queryCancellationExecutor;

  @Before
  public void setup()
  {
    responseContext.initialize();
    queryCancellationExecutor = Execs.scheduledSingleThreaded("query-cancellation-executor");
  }

  @After
  public void teardown() throws InterruptedException
  {
    queryCancellationExecutor.shutdown();
    queryCancellationExecutor.awaitTermination(1, TimeUnit.SECONDS);
  }

  @Test
  public void testRun() throws Exception
  {
    final URL url = new URL(StringUtils.format("http://%s/druid/v2/", hostName));

    QueuedTestHttpClient sequenced = new QueuedTestHttpClient();
    DirectDruidClient client1 = makeDirectDruidClient(sequenced);

    DirectDruidClient client2 = makeDirectDruidClient(sequenced);

    // Queue first call: pending until we provide a result
    SettableFuture<InputStream> futureResult = SettableFuture.create();
    sequenced.enqueue(futureResult);
    // Queue second call: will fail with ReadTimeoutException
    SettableFuture<InputStream> futureException = SettableFuture.create();
    sequenced.enqueue(futureException);
    // Subsequent calls: no enqueue → default pending futures created in client

    QueryPlus queryPlus = getQueryPlus();

    Sequence s1 = client1.run(queryPlus, responseContext);
    List<Request> requests = sequenced.getRequests();
    Assert.assertFalse(requests.isEmpty());
    Assert.assertEquals(url, requests.get(0).getUrl());
    Assert.assertEquals(HttpMethod.POST, requests.get(0).getMethod());
    Assert.assertEquals(1, client1.getNumOpenConnections());

    // simulate read timeout on second request
    client1.run(queryPlus, responseContext);
    Assert.assertEquals(2, client1.getNumOpenConnections());
    futureException.setException(new ReadTimeoutException());
    Assert.assertEquals(1, client1.getNumOpenConnections());

    // subsequent connections should work (and remain open)
    client1.run(queryPlus, responseContext);
    client1.run(queryPlus, responseContext);
    client1.run(queryPlus, responseContext);
    Assert.assertEquals(4, client1.getNumOpenConnections());

    // produce result for first connection
    futureResult.set(
        new ByteArrayInputStream(
            StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\", \"result\": 42.0}]")
        )
    );
    List<Result> results = s1.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(DateTimes.of("2014-01-01T01:02:03Z"), results.get(0).getTimestamp());
    Assert.assertEquals(3, client1.getNumOpenConnections());

    client2.run(queryPlus, responseContext);
    client2.run(queryPlus, responseContext);
    Assert.assertEquals(2, client2.getNumOpenConnections());
  }

  @Test
  public void testCancel()
  {
    DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient(Futures.immediateCancelledFuture()));
    Sequence results = client.run(getQueryPlus(), responseContext);

    Assert.assertEquals(0, client.getNumOpenConnections());
    QueryInterruptedException actualException =
        Assert.assertThrows(QueryInterruptedException.class, () -> results.toList());
    Assert.assertEquals(hostName, actualException.getHost());
    Assert.assertEquals("Query cancelled", actualException.getErrorCode());
    Assert.assertEquals("Task was cancelled.", actualException.getCause().getMessage());
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
        Assert.assertThrows(QueryInterruptedException.class, () -> results.toList());
    Assert.assertEquals("testing1", actualException.getErrorCode());
    Assert.assertEquals("testing2", actualException.getMessage());
    Assert.assertEquals(hostName, actualException.getHost());
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

    QueryTimeoutException actualException = Assert.assertThrows(
        QueryTimeoutException.class,
        () -> {
          out.write(StringUtils.toUtf8("[{\"timestamp\":\"2014-01-01T01:02:03Z\"}"));
          Thread.sleep(250);
          out.write(StringUtils.toUtf8("]"));
          out.close();
          results.toList();
        }
    );
    Assert.assertEquals("Query timeout", actualException.getErrorCode());
    Assert.assertEquals(StringUtils.format("url[http://%s/druid/v2/] timed out", hostName), actualException.getMessage());
    Assert.assertEquals(hostName, actualException.getHost());
  }

  @Test
  public void testQueryTimeoutFromFuture()
  {
    final SettableFuture<Object> timeoutFuture = SettableFuture.create();
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient(timeoutFuture));

    QueryPlus query = getQueryPlus(Map.of(DirectDruidClient.QUERY_FAIL_TIME, System.currentTimeMillis() + 500));
    Sequence results = client.run(query, responseContext);
    QueryTimeoutException actualException = Assert.assertThrows(QueryTimeoutException.class, results::toList);
    Assert.assertEquals("Query timeout", actualException.getErrorCode());
    Assert.assertEquals(StringUtils.format("Query [%s] timed out!", query.getQuery().getId()), actualException.getMessage());
    Assert.assertEquals(hostName, actualException.getHost());
  }

  @Test
  public void testConnectionCountAfterException()
  {
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientFromExistingClient());

    Assert.assertThrows(RuntimeException.class, () -> client.run(getQueryPlus(), responseContext));
    Assert.assertEquals(0, client.getNumOpenConnections());
  }

  @Test
  public void testResourceLimitExceededException()
  {
    final DirectDruidClient client = makeDirectDruidClient(initHttpClientWithSuccessfulQuery());

    final QueryPlus queryPlus = getQueryPlus(Map.of(
        QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, 100,
        DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE
    ));

    ResourceLimitExceededException actualException = Assert.assertThrows(
        ResourceLimitExceededException.class,
        () -> client.run(queryPlus, responseContext)
    );

    Assert.assertEquals(
        StringUtils.format(
            "Query[%s] url[http://localhost:8080/druid/v2/] total bytes gathered[127] exceeds maxScatterGatherBytes[100]",
            queryPlus.getQuery().getId()
        ),
        actualException.getMessage());
  }

  private DirectDruidClient makeDirectDruidClient(HttpClient httpClient)
  {
    return new DirectDruidClient(
        conglomerateRule.getConglomerate(),
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        objectMapper,
        httpClient,
        "http",
        hostName,
        new NoopServiceEmitter(),
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
        new DruidServer("test1", hostName, null, 0, ServerType.HISTORICAL, DruidServer.DEFAULT_TIER, 0),
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
