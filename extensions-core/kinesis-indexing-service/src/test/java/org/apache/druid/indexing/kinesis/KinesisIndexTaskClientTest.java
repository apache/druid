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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner.Status;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@RunWith(Parameterized.class)
public class KinesisIndexTaskClientTest extends EasyMockSupport
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private static final ObjectMapper OBJECT_MAPPER = new DefaultObjectMapper();
  private static final String TEST_ID = "test-id";
  private static final List<String> TEST_IDS = Arrays.asList("test-id1", "test-id2", "test-id3", "test-id4");
  private static final String TEST_HOST = "test-host";
  private static final int TEST_PORT = 1234;
  private static final int TEST_TLS_PORT = -1;
  private static final String TEST_DATASOURCE = "test-datasource";
  private static final Duration TEST_HTTP_TIMEOUT = new Duration(5000);
  private static final long TEST_NUM_RETRIES = 0;
  private static final String URL_FORMATTER = "http://%s:%d/druid/worker/v1/chat/%s/%s";

  private final int numThreads;
  private HttpClient httpClient;
  private TaskInfoProvider taskInfoProvider;
  private StringFullResponseHolder responseHolder;
  private HttpResponse response;
  private HttpHeaders headers;
  private KinesisIndexTaskClient client;

  @Parameterized.Parameters(name = "numThreads = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{1}, new Object[]{8});
  }

  public KinesisIndexTaskClientTest(int numThreads)
  {
    this.numThreads = numThreads;
  }

  @Before
  public void setUp()
  {
    httpClient = createMock(HttpClient.class);
    taskInfoProvider = createMock(TaskInfoProvider.class);
    responseHolder = createMock(StringFullResponseHolder.class);
    response = createMock(HttpResponse.class);
    headers = createMock(HttpHeaders.class);

    client = new TestableKinesisIndexTaskClient(httpClient, OBJECT_MAPPER, taskInfoProvider);
    EasyMock.expect(taskInfoProvider.getTaskLocation(TEST_ID))
            .andReturn(new TaskLocation(TEST_HOST, TEST_PORT, TEST_TLS_PORT))
            .anyTimes();
    EasyMock.expect(taskInfoProvider.getTaskStatus(TEST_ID))
            .andReturn(Optional.of(TaskStatus.running(TEST_ID)))
            .anyTimes();

    for (String testId : TEST_IDS) {
      EasyMock.expect(taskInfoProvider.getTaskLocation(testId))
              .andReturn(new TaskLocation(TEST_HOST, TEST_PORT, TEST_TLS_PORT))
              .anyTimes();
      EasyMock.expect(taskInfoProvider.getTaskStatus(testId))
              .andReturn(Optional.of(TaskStatus.running(testId)))
              .anyTimes();
    }
  }

  @After
  public void tearDown()
  {
    client.close();
  }

  @Test
  public void testNoTaskLocation() throws IOException
  {
    EasyMock.reset(taskInfoProvider);
    EasyMock.expect(taskInfoProvider.getTaskLocation(TEST_ID)).andReturn(TaskLocation.unknown()).anyTimes();
    EasyMock.expect(taskInfoProvider.getTaskStatus(TEST_ID))
            .andReturn(Optional.of(TaskStatus.running(TEST_ID)))
            .anyTimes();
    replayAll();

    Assert.assertFalse(client.stop(TEST_ID, true));
    Assert.assertFalse(client.resume(TEST_ID));
    Assert.assertEquals(ImmutableMap.of(), client.pause(TEST_ID));
    Assert.assertEquals(ImmutableMap.of(), client.pause(TEST_ID));
    Assert.assertEquals(Status.NOT_STARTED, client.getStatus(TEST_ID));
    Assert.assertNull(client.getStartTime(TEST_ID));
    Assert.assertEquals(ImmutableMap.of(), client.getCurrentOffsets(TEST_ID, true));
    Assert.assertEquals(ImmutableMap.of(), client.getEndOffsets(TEST_ID));
    Assert.assertFalse(client.setEndOffsets(TEST_ID, Collections.emptyMap(), true));
    Assert.assertFalse(client.setEndOffsets(TEST_ID, Collections.emptyMap(), true));

    verifyAll();
  }

  @Test
  public void testTaskNotRunnableException()
  {
    expectedException.expect(IndexTaskClient.TaskNotRunnableException.class);
    expectedException.expectMessage("Aborting request because task [test-id] is not runnable");

    EasyMock.reset(taskInfoProvider);
    EasyMock.expect(taskInfoProvider.getTaskLocation(TEST_ID))
            .andReturn(new TaskLocation(TEST_HOST, TEST_PORT, TEST_TLS_PORT))
            .anyTimes();
    EasyMock.expect(taskInfoProvider.getTaskStatus(TEST_ID))
            .andReturn(Optional.of(TaskStatus.failure(TEST_ID)))
            .anyTimes();
    replayAll();

    client.getCurrentOffsets(TEST_ID, true);
    verifyAll();
  }

  @Test
  public void testInternalServerError()
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("org.apache.druid.java.util.common.IOE: Received status [500] and content []");

    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.INTERNAL_SERVER_ERROR).times(2);
    EasyMock.expect(responseHolder.getContent()).andReturn("");
    EasyMock.expect(
        httpClient.go(
            EasyMock.anyObject(Request.class),
            EasyMock.anyObject(StringFullResponseHandler.class),
            EasyMock.eq(TEST_HTTP_TIMEOUT)
        )
    ).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.getCurrentOffsets(TEST_ID, true);
    verifyAll();
  }

  @Test
  public void testBadRequest()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Received 400 Bad Request with body:");

    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.BAD_REQUEST).times(2);
    EasyMock.expect(responseHolder.getContent()).andReturn("");
    EasyMock.expect(
        httpClient.go(
            EasyMock.anyObject(Request.class),
            EasyMock.anyObject(StringFullResponseHandler.class),
            EasyMock.eq(TEST_HTTP_TIMEOUT)
        )
    ).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.getCurrentOffsets(TEST_ID, true);
    verifyAll();
  }

  @Test
  public void testTaskLocationMismatch()
  {
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).times(3)
            .andReturn(HttpResponseStatus.OK);
    EasyMock.expect(responseHolder.getResponse()).andReturn(response);
    EasyMock.expect(responseHolder.getContent()).andReturn("").times(2)
            .andReturn("{}");
    EasyMock.expect(response.headers()).andReturn(headers);
    EasyMock.expect(headers.get("X-Druid-Task-Id")).andReturn("a-different-task-id");
    EasyMock.expect(
        httpClient.go(
            EasyMock.anyObject(Request.class),
            EasyMock.anyObject(StringFullResponseHandler.class),
            EasyMock.eq(TEST_HTTP_TIMEOUT)
        )
    ).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(2);
    replayAll();

    Map<String, String> results = client.getCurrentOffsets(TEST_ID, true);
    verifyAll();

    Assert.assertEquals(0, results.size());
  }

  @Test
  public void testGetCurrentOffsets() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    EasyMock.expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}");
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    Map<String, String> results = client.getCurrentOffsets(TEST_ID, true);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/current"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals("1", results.get("0"));
    Assert.assertEquals("10", results.get("1"));
  }

  @Test
  public void testGetCurrentOffsetsWithRetry() throws Exception
  {
    client = new TestableKinesisIndexTaskClient(httpClient, OBJECT_MAPPER, taskInfoProvider, 3);

    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).times(6)
            .andReturn(HttpResponseStatus.OK).times(1);
    EasyMock.expect(responseHolder.getContent()).andReturn("").times(4)
            .andReturn("{\"0\":1, \"1\":10}");
    EasyMock.expect(responseHolder.getResponse()).andReturn(response).times(2);
    EasyMock.expect(response.headers()).andReturn(headers).times(2);
    EasyMock.expect(headers.get("X-Druid-Task-Id")).andReturn(TEST_ID).times(2);

    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(3);

    replayAll();

    Map<String, String> results = client.getCurrentOffsets(TEST_ID, true);
    verifyAll();

    Assert.assertEquals(3, captured.getValues().size());
    for (Request request : captured.getValues()) {
      Assert.assertEquals(HttpMethod.GET, request.getMethod());
      Assert.assertEquals(
          new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/current"),
          request.getUrl()
      );
      Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));
    }

    Assert.assertEquals(2, results.size());
    Assert.assertEquals("1", results.get("0"));
    Assert.assertEquals("10", results.get("1"));
  }

  @Test
  public void testGetCurrentOffsetsWithExhaustedRetries()
  {
    expectedException.expect(RuntimeException.class);
    expectedException.expectMessage("org.apache.druid.java.util.common.IOE: Received status [404]");

    client = new TestableKinesisIndexTaskClient(httpClient, OBJECT_MAPPER, taskInfoProvider, 2);

    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).anyTimes();
    EasyMock.expect(responseHolder.getContent()).andReturn("").anyTimes();
    EasyMock.expect(responseHolder.getResponse()).andReturn(response).anyTimes();
    EasyMock.expect(response.headers()).andReturn(headers).anyTimes();
    EasyMock.expect(headers.get("X-Druid-Task-Id")).andReturn(TEST_ID).anyTimes();

    EasyMock.expect(
        httpClient.go(
            EasyMock.anyObject(Request.class),
            EasyMock.anyObject(StringFullResponseHandler.class),
            EasyMock.eq(TEST_HTTP_TIMEOUT)
        )
    ).andReturn(Futures.immediateFuture(responseHolder)).anyTimes();
    replayAll();

    client.getCurrentOffsets(TEST_ID, true);
    verifyAll();
  }

  @Test
  public void testGetEndOffsets() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    EasyMock.expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}");
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    Map<String, String> results = client.getEndOffsets(TEST_ID);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/end"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals("1", results.get("0"));
    Assert.assertEquals("10", results.get("1"));
  }

  @Test
  public void testGetStartTime() throws Exception
  {
    client = new TestableKinesisIndexTaskClient(httpClient, OBJECT_MAPPER, taskInfoProvider, 2);
    DateTime now = DateTimes.nowUtc();

    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).times(3)
            .andReturn(HttpResponseStatus.OK);
    EasyMock.expect(responseHolder.getResponse()).andReturn(response);
    EasyMock.expect(response.headers()).andReturn(headers);
    EasyMock.expect(headers.get("X-Druid-Task-Id")).andReturn(null);
    EasyMock.expect(responseHolder.getContent()).andReturn(String.valueOf(now.getMillis())).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(2);
    replayAll();

    DateTime results = client.getStartTime(TEST_ID);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/time/start"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(now, results);
  }

  @Test
  public void testGetStatus() throws Exception
  {
    Status status = Status.READING;

    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    EasyMock.expect(responseHolder.getContent()).andReturn(StringUtils.format("\"%s\"", status.toString())).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    Status results = client.getStatus(TEST_ID);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/status"),
        request.getUrl()
    );
    Assert.assertTrue(null, request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(status, results);
  }

  @Test
  public void testPause() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).times(2);
    EasyMock.expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}").anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    Map<String, String> results = client.pause(TEST_ID);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/pause"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals("1", results.get("0"));
    Assert.assertEquals("10", results.get("1"));
  }

  @Test
  public void testPauseWithSubsequentGetOffsets() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    Capture<Request> captured2 = Capture.newInstance();
    Capture<Request> captured3 = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.ACCEPTED).times(2)
            .andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(responseHolder.getContent()).andReturn("\"PAUSED\"").times(2)
            .andReturn("{\"0\":1, \"1\":10}").anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured2),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured3),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );

    replayAll();

    Map<String, String> results = client.pause(TEST_ID);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/pause"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    request = captured2.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/status"),
        request.getUrl()
    );

    request = captured3.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/current"),
        request.getUrl()
    );

    Assert.assertEquals(2, results.size());
    Assert.assertEquals("1", results.get("0"));
    Assert.assertEquals("10", results.get("1"));
  }

  @Test
  public void testResume() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.resume(TEST_ID);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/resume"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));
  }

  @Test
  public void testSetEndOffsets() throws Exception
  {
    Map<String, String> endOffsets = ImmutableMap.of("0", "15", "1", "120");

    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.setEndOffsets(TEST_ID, endOffsets, true);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/end?finish=true"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));
    Assert.assertEquals("{\"0\":\"15\",\"1\":\"120\"}", StringUtils.fromUtf8(request.getContent().array()));
  }

  @Test
  public void testSetEndOffsetsAndResume() throws Exception
  {
    Map<String, String> endOffsets = ImmutableMap.of("0", "15", "1", "120");

    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.setEndOffsets(TEST_ID, endOffsets, true);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/end?finish=true"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));
    Assert.assertEquals("{\"0\":\"15\",\"1\":\"120\"}", StringUtils.fromUtf8(request.getContent().array()));
  }

  @Test
  public void testStop() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.stop(TEST_ID, false);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/stop"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));
  }

  @Test
  public void testStopAndPublish() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.stop(TEST_ID, true);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/stop?publish=true"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));
  }

  @Test
  public void testStopAsync() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<Boolean>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(URL_FORMATTER, TEST_HOST, TEST_PORT, testId, "stop")));
      futures.add(client.stopAsync(testId, false));
    }

    List<Boolean> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.POST, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertTrue(responses.get(i));
    }
  }

  @Test
  public void testResumeAsync() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<Boolean>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(URL_FORMATTER, TEST_HOST, TEST_PORT, testId, "resume")));
      futures.add(client.resumeAsync(testId));
    }

    List<Boolean> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.POST, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertTrue(responses.get(i));
    }
  }

  @Test
  public void testPauseAsync() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(responseHolder.getContent()).andReturn("{\"0\":\"1\"}").anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<Map<String, String>>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(URL_FORMATTER, TEST_HOST, TEST_PORT, testId, "pause")));
      futures.add(client.pauseAsync(testId));
    }

    List<Map<String, String>> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.POST, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(Maps.newLinkedHashMap(ImmutableMap.of("0", "1")), responses.get(i));
    }
  }

  @Test
  public void testGetStatusAsync() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(responseHolder.getContent()).andReturn("\"READING\"").anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<Status>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(URL_FORMATTER, TEST_HOST, TEST_PORT, testId, "status")));
      futures.add(client.getStatusAsync(testId));
    }

    List<Status> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.GET, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(Status.READING, responses.get(i));
    }
  }

  @Test
  public void testGetStartTimeAsync() throws Exception
  {
    final DateTime now = DateTimes.nowUtc();
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(responseHolder.getContent()).andReturn(String.valueOf(now.getMillis())).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<DateTime>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(URL_FORMATTER, TEST_HOST, TEST_PORT, testId, "time/start")));
      futures.add(client.getStartTimeAsync(testId));
    }

    List<DateTime> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.GET, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(now, responses.get(i));
    }
  }

  @Test
  public void testGetCurrentOffsetsAsync() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(responseHolder.getContent()).andReturn("{\"0\":\"1\"}").anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<Map<String, String>>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(URL_FORMATTER, TEST_HOST, TEST_PORT, testId, "offsets/current")));
      futures.add(client.getCurrentOffsetsAsync(testId, false));
    }

    List<Map<String, String>> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.GET, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(Maps.newLinkedHashMap(ImmutableMap.of("0", "1")), responses.get(i));
    }
  }

  @Test
  public void testGetEndOffsetsAsync() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(responseHolder.getContent()).andReturn("{\"0\":\"1\"}").anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<Map<String, String>>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(URL_FORMATTER, TEST_HOST, TEST_PORT, testId, "offsets/end")));
      futures.add(client.getEndOffsetsAsync(testId));
    }

    List<Map<String, String>> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.GET, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(Maps.newLinkedHashMap(ImmutableMap.of("0", "1")), responses.get(i));
    }
  }

  @Test
  public void testSetEndOffsetsAsync() throws Exception
  {
    Map<String, String> endOffsets = ImmutableMap.of("0", "15L", "1", "120L");
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<Boolean>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(
          URL_FORMATTER,
          TEST_HOST,
          TEST_PORT,
          testId,
          StringUtils.format("offsets/end?finish=%s", true)
      )));
      futures.add(client.setEndOffsetsAsync(testId, endOffsets, true));
    }

    List<Boolean> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.POST, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertTrue(responses.get(i));
    }
  }

  @Test
  public void testSetEndOffsetsAsyncWithResume() throws Exception
  {
    Map<String, String> endOffsets = ImmutableMap.of("0", "15L", "1", "120L");
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(httpClient.go(
        EasyMock.capture(captured),
        EasyMock.anyObject(StringFullResponseHandler.class),
        EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = new ArrayList<>();
    List<ListenableFuture<Boolean>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(
          new URL(
              StringUtils.format(
                  URL_FORMATTER,
                  TEST_HOST,
                  TEST_PORT,
                  testId,
                  "offsets/end?finish=true"
              )
          )
      );
      futures.add(client.setEndOffsetsAsync(testId, endOffsets, true));
    }

    List<Boolean> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.POST, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertTrue(responses.get(i));
    }
  }

  private class TestableKinesisIndexTaskClient extends KinesisIndexTaskClient
  {
    TestableKinesisIndexTaskClient(
        HttpClient httpClient,
        ObjectMapper jsonMapper,
        TaskInfoProvider taskInfoProvider
    )
    {
      this(httpClient, jsonMapper, taskInfoProvider, TEST_NUM_RETRIES);
    }

    TestableKinesisIndexTaskClient(
        HttpClient httpClient,
        ObjectMapper jsonMapper,
        TaskInfoProvider taskInfoProvider,
        long numRetries
    )
    {
      super(httpClient, jsonMapper, taskInfoProvider, TEST_DATASOURCE, numThreads, TEST_HTTP_TIMEOUT, numRetries);
    }

    @Override
    protected void checkConnection(String host, int port)
    {
    }
  }
}
