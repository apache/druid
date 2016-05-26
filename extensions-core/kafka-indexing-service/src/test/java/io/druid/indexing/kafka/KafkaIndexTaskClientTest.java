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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.metamx.common.IAE;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.FullResponseHandler;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.indexing.common.RetryPolicyConfig;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.Capture;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URL;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.reset;

@RunWith(EasyMockRunner.class)
public class KafkaIndexTaskClientTest extends EasyMockSupport
{
  private static final ObjectMapper objectMapper = new DefaultObjectMapper();
  private static final String TEST_ID = "test-id";
  private static final String TEST_HOST = "test-host";
  private static final int TEST_PORT = 1234;

  @Mock
  private HttpClient httpClient;

  @Mock
  private TaskInfoProvider taskInfoProvider;

  @Mock
  private FullResponseHolder responseHolder;

  @Mock
  private HttpResponse response;

  @Mock
  private HttpHeaders headers;

  private KafkaIndexTaskClient client;

  @Before
  public void setUp() throws Exception
  {
    client = new TestableKafkaIndexTaskClient(httpClient, objectMapper, taskInfoProvider);
    expect(taskInfoProvider.getTaskLocation(TEST_ID)).andReturn(new TaskLocation(TEST_HOST, TEST_PORT)).anyTimes();
    expect(taskInfoProvider.getTaskStatus(TEST_ID)).andReturn(Optional.of(TaskStatus.running(TEST_ID))).anyTimes();
  }

  @Test(expected = KafkaIndexTaskClient.NoTaskLocationException.class)
  public void testNoTaskLocationException() throws Exception
  {
    reset(taskInfoProvider);
    expect(taskInfoProvider.getTaskLocation(TEST_ID)).andReturn(TaskLocation.unknown()).anyTimes();
    expect(taskInfoProvider.getTaskStatus(TEST_ID)).andReturn(Optional.of(TaskStatus.running(TEST_ID))).anyTimes();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.BAD_REQUEST).times(2);
    expect(responseHolder.getContent()).andReturn("");
    expect(httpClient.go(anyObject(Request.class), anyObject(FullResponseHandler.class))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();
    client.getCurrentOffsets(TEST_ID, true);
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
    expect(httpClient.go(anyObject(Request.class), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(httpClient.go(anyObject(Request.class), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(httpClient.go(anyObject(Request.class), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    client = new RetryingTestableKafkaIndexTaskClient(httpClient, objectMapper, taskInfoProvider);

    Capture<Request> captured = Capture.newInstance();
    Capture<Request> captured2 = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.NOT_FOUND).times(2)
                                      .andReturn(HttpResponseStatus.OK).times(2);
    expect(responseHolder.getContent()).andReturn("")
                                       .andReturn("{\"0\":1, \"1\":10}");
    expect(responseHolder.getResponse()).andReturn(response);
    expect(response.headers()).andReturn(headers);
    expect(headers.get("X-Druid-Task-Id")).andReturn(TEST_ID);

    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    expect(httpClient.go(capture(captured2), anyObject(FullResponseHandler.class))).andReturn(
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

    request = captured2.getValue();
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
  public void testGetEndOffsets() throws Exception
  {
    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}");
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    DateTime now = DateTime.now();

    Capture<Request> captured = Capture.newInstance();
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(responseHolder.getContent()).andReturn(String.valueOf(now.getMillis())).anyTimes();
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replayAll();

    DateTime results = client.getStartTime(TEST_ID, false);
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
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    expect(httpClient.go(capture(captured2), anyObject(FullResponseHandler.class))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    expect(httpClient.go(capture(captured3), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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
    expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK);
    expect(httpClient.go(capture(captured), anyObject(FullResponseHandler.class))).andReturn(
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

  private class TestableKafkaIndexTaskClient extends KafkaIndexTaskClient
  {
    public TestableKafkaIndexTaskClient(
        HttpClient httpClient,
        ObjectMapper jsonMapper,
        TaskInfoProvider taskInfoProvider
    )
    {
      super(httpClient, jsonMapper, taskInfoProvider);
    }

    @Override
    RetryPolicyFactory createRetryPolicyFactory()
    {
      return new RetryPolicyFactory(
          new RetryPolicyConfig()
              .setMinWait(new Period("PT1S"))
              .setMaxRetryCount(0)
      );
    }

    @Override
    void checkConnection(String host, int port) throws IOException { }
  }

  private class RetryingTestableKafkaIndexTaskClient extends TestableKafkaIndexTaskClient
  {
    public RetryingTestableKafkaIndexTaskClient(
        HttpClient httpClient,
        ObjectMapper jsonMapper,
        TaskInfoProvider taskInfoProvider
    )
    {
      super(httpClient, jsonMapper, taskInfoProvider);
    }

    @Override
    RetryPolicyFactory createRetryPolicyFactory()
    {
      return new RetryPolicyFactory(
          new RetryPolicyConfig()
              .setMinWait(new Period("PT1S"))
              .setMaxRetryCount(1)
      );
    }
  }
}
