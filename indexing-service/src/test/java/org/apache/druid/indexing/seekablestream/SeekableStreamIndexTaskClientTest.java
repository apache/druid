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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.RE;
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
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SeekableStreamIndexTaskClientTest extends EasyMockSupport
{
  private static final String TEST_ID = "test-id";
  private static final List<String> TEST_IDS = Arrays.asList("test-id1", "test-id2", "test-id3", "test-id4");
  private static final String TEST_HOST = "test-host";
  private static final int TEST_PORT = 1234;
  private static final int TEST_TLS_PORT = -1;
  private static final String TEST_DATASOURCE = "test-datasource";
  private static final Duration TEST_HTTP_TIMEOUT = new Duration(5000);
  private static final long TEST_NUM_RETRIES = 0;
  private static final String URL_FORMATTER = "http://%s:%d/druid/worker/v1/chat/%s/%s";

  private MySeekableStreamIndexTaskClient taskClient;
  private HttpClient httpClient;
  private ObjectMapper jsonMapper = new DefaultObjectMapper();
  private TaskInfoProvider taskInfoProvider;
  private StringFullResponseHolder responseHolder;
  private HttpResponse response;
  private HttpHeaders headers;

  @Before
  public void setUp()
  {
    httpClient = createMock(HttpClient.class);
    taskInfoProvider = createMock(TaskInfoProvider.class);
    responseHolder = createMock(StringFullResponseHolder.class);
    response = createMock(HttpResponse.class);
    headers = createMock(HttpHeaders.class);

    taskClient = new MySeekableStreamIndexTaskClient(httpClient, jsonMapper, taskInfoProvider, TEST_DATASOURCE, 10, TEST_HTTP_TIMEOUT, TEST_NUM_RETRIES);

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
    taskClient.close();
  }

  @Test
  public void testPauseNoTaskLocationException()
  {
    EasyMock.reset(taskInfoProvider);
    EasyMock.expect(taskInfoProvider.getTaskLocation(TEST_ID))
            .andThrow(new IndexTaskClient.NoTaskLocationException("no task location"))
            .anyTimes();
    EasyMock.expect(taskInfoProvider.getTaskStatus(TEST_ID))
            .andReturn(Optional.of(TaskStatus.running(TEST_ID)))
            .anyTimes();

    replayAll();

    Map<Integer, Long> results = taskClient.pause(TEST_ID);

    verifyAll();

    Assert.assertEquals(results.isEmpty(), true);
  }

  @Test (expected = RE.class)
  public void testPauseIOException()
  {
    EasyMock.reset(taskInfoProvider);
    EasyMock.expect(taskInfoProvider.getTaskLocation(TEST_ID))
            .andReturn(new TaskLocation(TEST_HOST, TEST_PORT, TEST_TLS_PORT))
            .anyTimes();
    EasyMock.expect(taskInfoProvider.getTaskStatus(TEST_ID))
            .andReturn(Optional.of(TaskStatus.running(TEST_ID)))
            .anyTimes();

    replayAll();

    MySeekableStreamIndexTaskClientWithIOException taskClient1 = new MySeekableStreamIndexTaskClientWithIOException(
            httpClient,
            jsonMapper,
            taskInfoProvider,
            TEST_DATASOURCE,
            10,
            TEST_HTTP_TIMEOUT, TEST_NUM_RETRIES);

    taskClient1.pause(TEST_ID);

    verifyAll();
  }

  @Test
  public void testPauseSuccessfully() throws Exception
  {
    EasyMock.reset(taskInfoProvider);
    EasyMock.expect(taskInfoProvider.getTaskLocation(TEST_ID))
            .andReturn(new TaskLocation(TEST_HOST, TEST_PORT, TEST_TLS_PORT))
            .anyTimes();
    EasyMock.expect(taskInfoProvider.getTaskStatus(TEST_ID))
            .andReturn(Optional.of(TaskStatus.running(TEST_ID)))
            .anyTimes();

    Capture<Request> captured = Capture.newInstance();
    EasyMock.expect(responseHolder.getStatus()).andReturn(HttpResponseStatus.OK).anyTimes();
    EasyMock.expect(responseHolder.getContent()).andReturn("{\"0\":1, \"1\":10}").anyTimes();
    EasyMock.expect(httpClient.go(
            EasyMock.capture(captured),
            EasyMock.anyObject(StringFullResponseHandler.class),
            EasyMock.eq(TEST_HTTP_TIMEOUT)
    )).andReturn(
            Futures.immediateFuture(responseHolder)
    );

    replayAll();

    Map<Integer, Long> results = taskClient.pause(TEST_ID);

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
    List<ListenableFuture<Map<Integer, Long>>> futures = new ArrayList<>();
    for (String testId : TEST_IDS) {
      expectedUrls.add(new URL(StringUtils.format(URL_FORMATTER, TEST_HOST, TEST_PORT, testId, "pause")));
      futures.add(taskClient.pauseAsync(testId));
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

    taskClient.stopUnfinishedPauseTasks();
  }

  @Test
  public void testGetPauseFutureSize()
  {
    Assert.assertEquals(taskClient.getPauseFutureSize(), 0);
  }

  @Test
  public void testStopPauingTaskOk()
  {
    ListenableFuture future = Futures.immediateFuture(Collections.emptyMap());
    SeekableStreamIndexTaskClient.PauseCallable pauseCallable = taskClient.new PauseCallable(TEST_ID, taskClient);
    taskClient.stopPausingTask(future, pauseCallable);
    Assert.assertEquals(pauseCallable.getTaskId(), TEST_ID);
    Assert.assertEquals(taskClient.getWaitPausingTaskFinishedMap().get(TEST_ID), false);
  }

  private static class MySeekableStreamIndexTaskClient extends SeekableStreamIndexTaskClient
  {
    public MySeekableStreamIndexTaskClient(
          HttpClient httpClient,
          ObjectMapper jsonMapper,
          TaskInfoProvider taskInfoProvider,
          String dataSource,
          int numThreads,
          Duration httpTimeout,
          long numRetries
    )
    {
      super(httpClient, jsonMapper, taskInfoProvider, dataSource, numThreads, httpTimeout, numRetries);
    }

    @Override
    protected Class getPartitionType()
    {
      return Integer.class;
    }

    @Override
    protected Class getSequenceType()
    {
      return Long.class;
    }

    @Override
    protected void checkConnection(String host, int port) throws IOException
    {
    }
  }

  private static class MySeekableStreamIndexTaskClientWithIOException extends MySeekableStreamIndexTaskClient
  {
    public MySeekableStreamIndexTaskClientWithIOException(
            HttpClient httpClient,
            ObjectMapper jsonMapper,
            TaskInfoProvider taskInfoProvider,
            String dataSource,
            int numThreads,
            Duration httpTimeout,
            long numRetries
    )
    {
      super(httpClient, jsonMapper, taskInfoProvider, dataSource, numThreads, httpTimeout, numRetries);
    }

    @Override
    protected void checkConnection(String host, int port) throws IOException
    {
      throw new IOException("IOException");
    }
  }

}
