/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.FullResponseHandler;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.IAE;

import org.easymock.Capture;
import org.easymock.CaptureType;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.reset;

@RunWith(Parameterized.class)
public class KafkaIndexTaskClientTest extends EasyMockSupport
{
  private static final ObjectMapper objectMapper = new DefaultObjectMapper();
  private static final String TEST_ID = "test-id";
  private static final List<String> TEST_IDS = Lists.newArrayList("test-id1", "test-id2", "test-id3", "test-id4");
  private static final String TEST_HOST = "test-host";
  private static final int TEST_PORT = 1234;
  private static final String TEST_DATASOURCE = "test-datasource";
  private static final Duration TEST_HTTP_TIMEOUT = new Duration(5000);
  private static final long TEST_NUM_RETRIES = 0;
  private static final String URL_FORMATTER = "http://%s:%d/druid/worker/v1/chat/%s/%s";

  private int numThreads;
  private HttpClient httpClient;
  private TaskInfoProvider taskInfoProvider;
  private FullResponseHolder responseHolder;
  private HttpResponse response;
  private HttpHeaders headers;
  private KafkaIndexTaskClient client;

  @Parameterized.Parameters(name = "numThreads = {0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{1}, new Object[]{8});
  }

  public KafkaIndexTaskClientTest(int numThreads)
  {
    this.numThreads = numThreads;
  }

  @Before
  public void setUp() throws Exception
  {
    httpClient = createMock(HttpClient.class);
    taskInfoProvider = createMock(TaskInfoProvider.class);
    responseHolder = createMock(FullResponseHolder.class);
    response = createMock(HttpResponse.class);
    headers = createMock(HttpHeaders.class);

    client = new TestableKafkaIndexTaskClient(httpClient, objectMapper, taskInfoProvider);
    expect(taskInfoProvider.getTaskLocation(TEST_ID)).andReturn(new TaskLocation(TEST_HOST, TEST_PORT)).anyTimes();
    expect(taskInfoProvider.getTaskStatus(TEST_ID)).andReturn(Optional.of(TaskStatus.running(TEST_ID))).anyTimes();

    for (int i = 0; i < TEST_IDS.size(); i++) {
      expect(taskInfoProvider.getTaskLocation(TEST_IDS.get(i))).andReturn(new TaskLocation(TEST_HOST, TEST_PORT))
                                                               .anyTimes();
      expect(taskInfoProvider.getTaskStatus(TEST_IDS.get(i))).andReturn(Optional.of(TaskStatus.running(TEST_IDS.get(i))))
                                                             .anyTimes();
    }
  }

  @After
  public void tearDown() throws Exception
  {
    client.close();
  }

  @Test
  public void testNoTaskLocation() throws Exception
  {
    reset(taskInfoProvider);
    expect(taskInfoProvider.getTaskLocation(TEST_ID)).andReturn(TaskLocation.unknown()).anyTimes();
    expect(taskInfoProvider.getTaskStatus(TEST_ID)).andReturn(Optional.of(TaskStatus.running(TEST_ID))).anyTimes();
    replayAll();

    Assert.assertEquals(false, client.stop(TEST_ID, true));
    Assert.assertEquals(false, client.resume(TEST_ID));
    Assert.assertEquals(ImmutableMap.of(), client.pause(TEST_ID));
    Assert.assertEquals(ImmutableMap.of(), client.pause(TEST_ID, 10));
    Assert.assertEquals(KafkaIndexTask.Status.NOT_STARTED, client.getStatus(TEST_ID));
    Assert.assertEquals(null, client.getStartTime(TEST_ID));
    Assert.assertEquals(ImmutableMap.of(), client.getCurrentOffsets(TEST_ID, true));
    Assert.assertEquals(ImmutableMap.of(), client.getEndOffsets(TEST_ID));
    Assert.assertEquals(false, client.setEndOffsets(TEST_ID, ImmutableMap.<Integer, Long>of()));
    Assert.assertEquals(false, client.setEndOffsets(TEST_ID, ImmutableMap.<Integer, Long>of(), true));

    verifyAll();
  }

  @Test(expected = KafkaIndexTaskClient.TaskNotRunnableException.class)
  public void testTaskNotRunnableException() throws Exception
  {
    reset(taskInfoProvider);
    expect(taskInfoProvider.getTaskLocation(TEST_ID)).andReturn(new TaskLocation(TEST_HOST, TEST_PORT)).anyTimes();
    expect(taskInfoProvider.getTaskStatus(TEST_ID)).andReturn(Optional.of(TaskStatus.failure(TEST_ID))).anyTimes();
    replayAll();

    client.getCurrentOffsets(TEST_ID, true);
    verifyAll();
  }

  @Test(expected = RuntimeException.class)
  public void testInternalServerError() throws Exception
  {
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.INTERNAL_SERVER_ERROR).times(2);
    expect(
        httpClient.go(
            anyObject(Request.class),
            anyObject(FullResponseHandler.class),
            eq(TEST_HTTP_TIMEOUT)
        )
    ).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.getCurrentOffsets(TEST_ID, true);
    verifyAll();
  }

  @Test(expected = IAE.class)
  public void testBadRequest() throws Exception
  {
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.BAD_REQUEST).times(2);
    expect(responseHolder.getContent()).andReturn("");
    expect(
        httpClient.go(
            anyObject(Request.class),
            anyObject(FullResponseHandler.class),
            eq(TEST_HTTP_TIMEOUT)
        )
    ).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.getCurrentOffsets(TEST_ID, true);
    verifyAll();
  }

  @Test
  public void testTaskLocationMismatch() throws Exception
  {
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).times(3)
                                      .andReturn(HttpResponseStatus.OK);
    expect(responseHolder.getResponse()).andReturn(response);
    expect(responseHolder.getContent()).andReturn("")
                                       .andReturn("{}");
    expect(response.headers()).andReturn(headers);
    expect(headers.get("X-Druid-Task-Id")).andReturn("a-different-task-id");
    expect(
        httpClient.go(
            anyObject(Request.class),
            anyObject(FullResponseHandler.class),
            eq(TEST_HTTP_TIMEOUT)
        )
    ).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(2);
    replayAll();

    Map<Integer, Long> results = client.getCurrentOffsets(TEST_ID, true);
    verifyAll();

    Assert.assertEquals(0, results.size());
  }

  @Test
  public void testGetCurrentOffsets() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}");
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    Map<Integer, Long> results = client.getCurrentOffsets(TEST_ID, true);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/current"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(1, (long) results.get(0));
    Assert.assertEquals(10, (long) results.get(1));
  }

  @Test
  public void testGetCurrentOffsetsWithRetry() throws Exception
  {
    client = new TestableKafkaIndexTaskClient(httpClient, objectMapper, taskInfoProvider, 3);

    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).times(6)
                                      .andReturn(HttpResponseStatus.OK).times(1);
    expect(responseHolder.getContent()).andReturn("").times(2)
                                       .andReturn("{\"0\":1, \"1\":10}");
    expect(responseHolder.getResponse()).andReturn(response).times(2);
    expect(response.headers()).andReturn(headers).times(2);
    expect(headers.get("X-Druid-Task-Id")).andReturn(TEST_ID).times(2);

    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(3);

    replayAll();

    Map<Integer, Long> results = client.getCurrentOffsets(TEST_ID, true);
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
    Assert.assertEquals(1, (long) results.get(0));
    Assert.assertEquals(10, (long) results.get(1));
  }

  @Test(expected = RuntimeException.class)
  public void testGetCurrentOffsetsWithExhaustedRetries() throws Exception
  {
    client = new TestableKafkaIndexTaskClient(httpClient, objectMapper, taskInfoProvider, 2);

    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).anyTimes();
    expect(responseHolder.getContent()).andReturn("").anyTimes();
    expect(responseHolder.getResponse()).andReturn(response).anyTimes();
    expect(response.headers()).andReturn(headers).anyTimes();
    expect(headers.get("X-Druid-Task-Id")).andReturn(TEST_ID).anyTimes();

    expect(
        httpClient.go(
            anyObject(Request.class),
            anyObject(FullResponseHandler.class),
            eq(TEST_HTTP_TIMEOUT)
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}");
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    Map<Integer, Long> results = client.getEndOffsets(TEST_ID);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.GET, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/end"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(1, (long) results.get(0));
    Assert.assertEquals(10, (long) results.get(1));
  }

  @Test
  public void testGetStartTime() throws Exception
  {
    client = new TestableKafkaIndexTaskClient(httpClient, objectMapper, taskInfoProvider, 2);
    DateTime now = DateTime.now();

    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).times(3)
                                      .andReturn(HttpResponseStatus.OK);
    expect(responseHolder.getResponse()).andReturn(response);
    expect(response.headers()).andReturn(headers);
    expect(headers.get("X-Druid-Task-Id")).andReturn(null);
    expect(responseHolder.getContent()).andReturn(String.valueOf(now.getMillis())).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
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
    KafkaIndexTask.Status status = KafkaIndexTask.Status.READING;

    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(responseHolder.getContent()).andReturn(String.format("\"%s\"", status.toString())).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    KafkaIndexTask.Status results = client.getStatus(TEST_ID);
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).times(2);
    expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}").anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    Map<Integer, Long> results = client.pause(TEST_ID);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/pause"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(1, (long) results.get(0));
    Assert.assertEquals(10, (long) results.get(1));
  }

  @Test
  public void testPauseWithTimeout() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).times(2);
    expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}").anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    Map<Integer, Long> results = client.pause(TEST_ID, 101);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/pause?timeout=101"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(1, (long) results.get(0));
    Assert.assertEquals(10, (long) results.get(1));
  }

  @Test
  public void testPauseWithSubsequentGetOffsets() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    Capture<Request> captured2 = Capture.newInstance();
    Capture<Request> captured3 = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.ACCEPTED).times(2)
                                      .andReturn(HttpResponseStatus.OK).times(2);
    expect(responseHolder.getContent()).andReturn("\"PAUSED\"")
                                       .andReturn("{\"0\":1, \"1\":10}").anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    expect(httpClient.go(capture(captured2), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    expect(httpClient.go(capture(captured3), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );

    replayAll();

    Map<Integer, Long> results = client.pause(TEST_ID);
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
    Assert.assertEquals(1, (long) results.get(0));
    Assert.assertEquals(10, (long) results.get(1));
  }

  @Test
  public void testResume() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
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
    Map<Integer, Long> endOffsets = ImmutableMap.of(0, 15L, 1, 120L);

    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.setEndOffsets(TEST_ID, endOffsets);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/end"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));
    Assert.assertEquals("{\"0\":15,\"1\":120}", new String(request.getContent().array()));
  }

  @Test
  public void testSetEndOffsetsAndResume() throws Exception
  {
    Map<Integer, Long> endOffsets = ImmutableMap.of(0, 15L, 1, 120L);

    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    client.setEndOffsets(TEST_ID, endOffsets, true);
    verifyAll();

    Request request = captured.getValue();
    Assert.assertEquals(HttpMethod.POST, request.getMethod());
    Assert.assertEquals(
        new URL("http://test-host:1234/druid/worker/v1/chat/test-id/offsets/end?resume=true"),
        request.getUrl()
    );
    Assert.assertTrue(request.getHeaders().get("X-Druid-Task-Id").contains("test-id"));
    Assert.assertEquals("{\"0\":15,\"1\":120}", new String(request.getContent().array()));
  }

  @Test
  public void testStop() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "stop")));
      futures.add(client.stopAsync(TEST_IDS.get(i), false));
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "resume")));
      futures.add(client.resumeAsync(TEST_IDS.get(i)));
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(responseHolder.getContent()).andReturn("{\"0\":\"1\"}").anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<Map<Integer, Long>>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "pause")));
      futures.add(client.pauseAsync(TEST_IDS.get(i)));
    }

    List<Map<Integer, Long>> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.POST, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(Maps.newLinkedHashMap(ImmutableMap.of(0, 1L)), responses.get(i));
    }
  }

  @Test
  public void testPauseAsyncWithTimeout() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(responseHolder.getContent()).andReturn("{\"0\":\"1\"}").anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<Map<Integer, Long>>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "pause?timeout=9")));
      futures.add(client.pauseAsync(TEST_IDS.get(i), 9));
    }

    List<Map<Integer, Long>> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.POST, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(Maps.newLinkedHashMap(ImmutableMap.of(0, 1L)), responses.get(i));
    }
  }

  @Test
  public void testGetStatusAsync() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(responseHolder.getContent()).andReturn("\"READING\"").anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<KafkaIndexTask.Status>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "status")));
      futures.add(client.getStatusAsync(TEST_IDS.get(i)));
    }

    List<KafkaIndexTask.Status> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.GET, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(KafkaIndexTask.Status.READING, responses.get(i));
    }
  }

  @Test
  public void testGetStartTimeAsync() throws Exception
  {
    final DateTime now = DateTime.now();
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(responseHolder.getContent()).andReturn(String.valueOf(now.getMillis())).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<DateTime>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "time/start")));
      futures.add(client.getStartTimeAsync(TEST_IDS.get(i)));
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(responseHolder.getContent()).andReturn("{\"0\":\"1\"}").anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<Map<Integer, Long>>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "offsets/current")));
      futures.add(client.getCurrentOffsetsAsync(TEST_IDS.get(i), false));
    }

    List<Map<Integer, Long>> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.GET, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(Maps.newLinkedHashMap(ImmutableMap.of(0, 1L)), responses.get(i));
    }
  }

  @Test
  public void testGetEndOffsetsAsync() throws Exception
  {
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(responseHolder.getContent()).andReturn("{\"0\":\"1\"}").anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<Map<Integer, Long>>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "offsets/end")));
      futures.add(client.getEndOffsetsAsync(TEST_IDS.get(i)));
    }

    List<Map<Integer, Long>> responses = Futures.allAsList(futures).get();

    verifyAll();
    List<Request> requests = captured.getValues();

    Assert.assertEquals(numRequests, requests.size());
    Assert.assertEquals(numRequests, responses.size());
    for (int i = 0; i < numRequests; i++) {
      Assert.assertEquals(HttpMethod.GET, requests.get(i).getMethod());
      Assert.assertTrue("unexpectedURL", expectedUrls.contains(requests.get(i).getUrl()));
      Assert.assertEquals(Maps.newLinkedHashMap(ImmutableMap.of(0, 1L)), responses.get(i));
    }
  }

  @Test
  public void testSetEndOffsetsAsync() throws Exception
  {
    final Map<Integer, Long> endOffsets = ImmutableMap.of(0, 15L, 1, 120L);
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(new URL(String.format(URL_FORMATTER, TEST_HOST, TEST_PORT, TEST_IDS.get(i), "offsets/end")));
      futures.add(client.setEndOffsetsAsync(TEST_IDS.get(i), endOffsets));
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
    final Map<Integer, Long> endOffsets = ImmutableMap.of(0, 15L, 1, 120L);
    final int numRequests = TEST_IDS.size();
    Capture<Request> captured = Capture.newInstance(CaptureType.ALL);
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class), eq(TEST_HTTP_TIMEOUT))).andReturn(
        Futures.immediateFuture(responseHolder)
    ).times(numRequests);
    replayAll();

    List<URL> expectedUrls = Lists.newArrayList();
    List<ListenableFuture<Boolean>> futures = Lists.newArrayList();
    for (int i = 0; i < numRequests; i++) {
      expectedUrls.add(
          new URL(
              String.format(
                  URL_FORMATTER,
                  TEST_HOST,
                  TEST_PORT,
                  TEST_IDS.get(i),
                  "offsets/end?resume=true"
              )
          )
      );
      futures.add(client.setEndOffsetsAsync(TEST_IDS.get(i), endOffsets, true));
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

  private class TestableKafkaIndexTaskClient extends KafkaIndexTaskClient
  {
    public TestableKafkaIndexTaskClient(
        HttpClient httpClient,
        ObjectMapper jsonMapper,
        TaskInfoProvider taskInfoProvider
    )
    {
      this(httpClient, jsonMapper, taskInfoProvider, TEST_NUM_RETRIES);
    }

    public TestableKafkaIndexTaskClient(
        HttpClient httpClient,
        ObjectMapper jsonMapper,
        TaskInfoProvider taskInfoProvider,
        long numRetries
    )
    {
      super(httpClient, jsonMapper, taskInfoProvider, TEST_DATASOURCE, numThreads, TEST_HTTP_TIMEOUT, numRetries);
    }

    @Override
    void checkConnection(String host, int port) throws IOException { }
  }
}
