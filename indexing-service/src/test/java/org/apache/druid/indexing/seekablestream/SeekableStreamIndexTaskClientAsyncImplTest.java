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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.MockServiceClient;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.ServiceClosedException;
import org.apache.druid.rpc.ServiceLocation;
import org.apache.druid.rpc.ServiceLocations;
import org.apache.druid.rpc.ServiceNotAvailableException;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class SeekableStreamIndexTaskClientAsyncImplTest
{
  private static final String DATASOURCE = "the-datasource";
  private static final String TASK_ID = "the-task";
  private static final int MAX_ATTEMPTS = 2;

  private final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private final Duration httpTimeout = Duration.standardSeconds(1);

  private MockServiceClient serviceClient;
  private ServiceClientFactory serviceClientFactory;
  private SeekableStreamIndexTaskClient<Integer, Long> client;

  @Before
  public void setUp()
  {
    serviceClient = new MockServiceClient();
    serviceClientFactory = (serviceName, serviceLocator, retryPolicy) -> {
      Assert.assertEquals(TASK_ID, serviceName);
      return serviceClient;
    };
    client = new TestSeekableStreamIndexTaskClientAsyncImpl();
  }

  @After
  public void tearDown()
  {
    serviceClient.verify();
  }

  @Test
  public void test_getCheckpointsAsync() throws Exception
  {
    final Map<Integer, Map<Integer, Long>> checkpoints = ImmutableMap.of(0, ImmutableMap.of(2, 3L));

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/checkpoints").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(checkpoints)
    );

    Assert.assertEquals(checkpoints, client.getCheckpointsAsync(TASK_ID, false).get());
  }

  @Test
  public void test_getCurrentOffsetsAsync() throws Exception
  {
    final ImmutableMap<Integer, Long> offsets = ImmutableMap.of(2, 3L);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/offsets/current").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(offsets)
    );

    Assert.assertEquals(offsets, client.getCurrentOffsetsAsync(TASK_ID, false).get());
  }

  @Test
  public void test_getEndOffsetsAsync() throws Exception
  {
    final ImmutableMap<Integer, Long> offsets = ImmutableMap.of(2, 3L);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/offsets/end").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(offsets)
    );

    Assert.assertEquals(offsets, client.getEndOffsetsAsync(TASK_ID).get());
  }

  @Test
  public void test_getEndOffsetsAsync_notAvailable() throws Exception
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/offsets/end").timeout(httpTimeout),
        new ServiceNotAvailableException(TASK_ID)
    );

    Assert.assertEquals(Collections.emptyMap(), client.getEndOffsetsAsync(TASK_ID).get());
  }

  @Test
  public void test_stopAsync_publish_ok() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/stop?publish=true").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    );

    Assert.assertEquals(true, client.stopAsync(TASK_ID, true).get());
  }

  @Test
  public void test_stopAsync_noPublish_ok() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/stop").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    );

    Assert.assertEquals(true, client.stopAsync(TASK_ID, false).get());
  }

  @Test
  public void test_stopAsync_noPublish_httpError() throws Exception
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.POST, "/stop").timeout(httpTimeout),
        new HttpResponseException(
            new StringFullResponseHolder(
                new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.SERVICE_UNAVAILABLE),
                StandardCharsets.UTF_8
            )
        )
    );

    Assert.assertEquals(false, client.stopAsync(TASK_ID, false).get());
  }

  @Test
  public void test_stopAsync_noPublish_notAvailable() throws Exception
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.POST, "/stop").timeout(httpTimeout),
        new ServiceNotAvailableException(TASK_ID)
    );

    Assert.assertEquals(false, client.stopAsync(TASK_ID, false).get());
  }

  @Test
  public void test_stopAsync_noPublish_closed() throws Exception
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.POST, "/stop").timeout(httpTimeout),
        new ServiceClosedException(TASK_ID)
    );

    Assert.assertEquals(true, client.stopAsync(TASK_ID, false).get());
  }

  @Test
  public void test_stopAsync_noPublish_ioException()
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.POST, "/stop").timeout(httpTimeout),
        new IOException()
    );

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> client.stopAsync(TASK_ID, false).get()
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IOException.class));
  }

  @Test
  public void test_resumeAsync_ok() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/resume").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    );

    Assert.assertEquals(true, client.resumeAsync(TASK_ID).get());
  }

  @Test
  public void test_resumeAsync_ioException() throws Exception
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.POST, "/resume").timeout(httpTimeout),
        new IOException()
    );

    Assert.assertEquals(false, client.resumeAsync(TASK_ID).get());
  }

  @Test
  public void test_setEndOffsetsAsync() throws Exception
  {
    final Map<Integer, Long> offsets = ImmutableMap.of(1, 3L);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/offsets/end?finish=false")
            .content(MediaType.APPLICATION_JSON, jsonMapper.writeValueAsBytes(offsets))
            .timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    );

    Assert.assertEquals(true, client.setEndOffsetsAsync(TASK_ID, offsets, false).get());
  }

  @Test
  public void test_setEndOffsetsAsync_ioException() throws Exception
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.POST, "/resume").timeout(httpTimeout),
        new IOException()
    );

    Assert.assertEquals(false, client.resumeAsync(TASK_ID).get());
  }

  @Test
  public void test_getStatusAsync() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(SeekableStreamIndexTaskRunner.Status.READING)
    );

    Assert.assertEquals(SeekableStreamIndexTaskRunner.Status.READING, client.getStatusAsync(TASK_ID).get());
  }

  @Test
  public void test_getStatusAsync_notAvailable() throws Exception
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        new ServiceNotAvailableException(TASK_ID)
    );

    Assert.assertEquals(SeekableStreamIndexTaskRunner.Status.NOT_STARTED, client.getStatusAsync(TASK_ID).get());
  }

  @Test
  public void test_getStartTimeAsync() throws Exception
  {
    final DateTime startTime = DateTimes.of("2000");

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/time/start").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(startTime)
    );

    Assert.assertEquals(startTime, client.getStartTimeAsync(TASK_ID).get());
  }

  @Test
  public void test_getStartTimeAsync_noContent() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/time/start").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    );

    Assert.assertNull(client.getStartTimeAsync(TASK_ID).get());
  }

  @Test
  public void test_getStartTimeAsync_notAvailable() throws Exception
  {
    serviceClient.expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/time/start").timeout(httpTimeout),
        new ServiceNotAvailableException(TASK_ID)
    );

    Assert.assertNull(client.getStartTimeAsync(TASK_ID).get());
  }

  @Test
  public void test_pauseAsync_immediateOk() throws Exception
  {
    final Map<Integer, Long> offsets = ImmutableMap.of(1, 3L);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/pause").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(offsets)
    );

    Assert.assertEquals(offsets, client.pauseAsync(TASK_ID).get());
  }

  @Test
  public void test_pauseAsync_immediateBadStatus() throws Exception
  {
    final Map<Integer, Long> offsets = ImmutableMap.of(1, 3L);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/pause").timeout(httpTimeout),
        HttpResponseStatus.CONTINUE,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(offsets)
    );

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> client.pauseAsync(TASK_ID).get()
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(
        e.getCause().getMessage(),
        CoreMatchers.startsWith("Pause request for task [the-task] failed with response [100 Continue]")
    );
  }

  @Test
  public void test_pauseAsync_oneIteration() throws Exception
  {
    final Map<Integer, Long> offsets = ImmutableMap.of(1, 3L);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/pause").timeout(httpTimeout),
        HttpResponseStatus.ACCEPTED,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(SeekableStreamIndexTaskRunner.Status.PAUSED)
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/offsets/current").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(offsets)
    );

    Assert.assertEquals(offsets, client.pauseAsync(TASK_ID).get());
  }

  @Test
  public void test_pauseAsync_oneIterationWithError()
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/pause").timeout(httpTimeout),
        HttpResponseStatus.ACCEPTED,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    ).expectAndThrow(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        new IOException()
    );

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> client.pauseAsync(TASK_ID).get()
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IOException.class));
  }

  @Test
  public void test_pauseAsync_twoIterations() throws Exception
  {
    final Map<Integer, Long> offsets = ImmutableMap.of(1, 3L);

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/pause").timeout(httpTimeout),
        HttpResponseStatus.ACCEPTED,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(SeekableStreamIndexTaskRunner.Status.READING)
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(SeekableStreamIndexTaskRunner.Status.PAUSED)
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/offsets/current").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(offsets)
    );

    Assert.assertEquals(offsets, client.pauseAsync(TASK_ID).get());
  }

  @Test
  public void test_pauseAsync_threeIterations() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.POST, "/pause").timeout(httpTimeout),
        HttpResponseStatus.ACCEPTED,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(SeekableStreamIndexTaskRunner.Status.READING)
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(SeekableStreamIndexTaskRunner.Status.READING)
    ).expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/status").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(SeekableStreamIndexTaskRunner.Status.READING)
    );

    final ExecutionException e = Assert.assertThrows(
        ExecutionException.class,
        () -> client.pauseAsync(TASK_ID).get()
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(IllegalStateException.class));
    MatcherAssert.assertThat(
        e.getCause().getMessage(),
        CoreMatchers.startsWith("Task [the-task] failed to change its status from [READING] to [PAUSED]")
    );
  }

  @Test
  public void test_getMovingAveragesAsync() throws Exception
  {
    final Map<String, Object> retVal = ImmutableMap.of("foo", "xyz");

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/rowStats").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(retVal)
    );

    Assert.assertEquals(retVal, client.getMovingAveragesAsync(TASK_ID).get());
  }

  @Test
  public void test_getMovingAveragesAsync_empty() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/rowStats").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    );

    Assert.assertNull(client.getMovingAveragesAsync(TASK_ID).get());
  }

  @Test
  public void test_getMovingAveragesAsync_null() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/rowStats").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        null
    );

    Assert.assertNull(client.getMovingAveragesAsync(TASK_ID).get());
  }

  @Test
  public void test_getParseErrorsAsync() throws Exception
  {
    final List<ParseExceptionReport> retVal = ImmutableList.of(
        new ParseExceptionReport("xyz", "foo", Collections.emptyList(), 123L)
    );

    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/unparseableEvents").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        jsonMapper.writeValueAsBytes(retVal)
    );

    Assert.assertEquals(retVal, client.getParseErrorsAsync(TASK_ID).get());
  }

  @Test
  public void test_getParseErrorsAsync_empty() throws Exception
  {
    serviceClient.expectAndRespond(
        new RequestBuilder(HttpMethod.GET, "/unparseableEvents").timeout(httpTimeout),
        HttpResponseStatus.OK,
        Collections.emptyMap(),
        ByteArrays.EMPTY_ARRAY
    );

    Assert.assertNull(client.getParseErrorsAsync(TASK_ID).get());
  }

  @Test
  public void test_serviceLocator_unknownTask() throws Exception
  {
    final TaskInfoProvider taskInfoProvider = EasyMock.createStrictMock(TaskInfoProvider.class);
    EasyMock.expect(taskInfoProvider.getTaskStatus(TASK_ID))
            .andReturn(Optional.absent());
    EasyMock.replay(taskInfoProvider);

    try (final SeekableStreamIndexTaskClientAsyncImpl.SeekableStreamTaskLocator locator =
             new SeekableStreamIndexTaskClientAsyncImpl.SeekableStreamTaskLocator(taskInfoProvider, TASK_ID)) {
      Assert.assertEquals(
          ServiceLocations.closed(),
          locator.locate().get()
      );
    }

    EasyMock.verify(taskInfoProvider);
  }

  @Test
  public void test_serviceLocator_unknownLocation() throws Exception
  {
    final TaskInfoProvider taskInfoProvider = EasyMock.createStrictMock(TaskInfoProvider.class);
    EasyMock.expect(taskInfoProvider.getTaskStatus(TASK_ID))
            .andReturn(Optional.of(TaskStatus.running(TASK_ID)));
    EasyMock.expect(taskInfoProvider.getTaskLocation(TASK_ID))
            .andReturn(TaskLocation.unknown());
    EasyMock.replay(taskInfoProvider);

    try (final SeekableStreamIndexTaskClientAsyncImpl.SeekableStreamTaskLocator locator =
             new SeekableStreamIndexTaskClientAsyncImpl.SeekableStreamTaskLocator(taskInfoProvider, TASK_ID)) {
      Assert.assertEquals(
          ServiceLocations.forLocations(Collections.emptySet()),
          locator.locate().get()
      );
    }

    EasyMock.verify(taskInfoProvider);
  }

  @Test
  public void test_serviceLocator_found() throws Exception
  {
    final TaskInfoProvider taskInfoProvider = EasyMock.createStrictMock(TaskInfoProvider.class);
    EasyMock.expect(taskInfoProvider.getTaskStatus(TASK_ID))
            .andReturn(Optional.of(TaskStatus.running(TASK_ID)));
    EasyMock.expect(taskInfoProvider.getTaskLocation(TASK_ID))
            .andReturn(TaskLocation.create("foo", 80, -1));
    EasyMock.replay(taskInfoProvider);

    try (final SeekableStreamIndexTaskClientAsyncImpl.SeekableStreamTaskLocator locator =
             new SeekableStreamIndexTaskClientAsyncImpl.SeekableStreamTaskLocator(taskInfoProvider, TASK_ID)) {
      Assert.assertEquals(
          ServiceLocations.forLocation(new ServiceLocation("foo", 80, -1, "/druid/worker/v1/chat/" + TASK_ID)),
          locator.locate().get()
      );
    }

    EasyMock.verify(taskInfoProvider);
  }

  @Test
  public void test_serviceLocator_closed() throws Exception
  {
    final TaskInfoProvider taskInfoProvider = EasyMock.createStrictMock(TaskInfoProvider.class);
    EasyMock.expect(taskInfoProvider.getTaskStatus(TASK_ID))
            .andReturn(Optional.of(TaskStatus.success(TASK_ID)));
    EasyMock.replay(taskInfoProvider);

    try (final SeekableStreamIndexTaskClientAsyncImpl.SeekableStreamTaskLocator locator =
             new SeekableStreamIndexTaskClientAsyncImpl.SeekableStreamTaskLocator(taskInfoProvider, TASK_ID)) {
      Assert.assertEquals(
          ServiceLocations.closed(),
          locator.locate().get()
      );
    }

    EasyMock.verify(taskInfoProvider);
  }

  private class TestSeekableStreamIndexTaskClientAsyncImpl extends SeekableStreamIndexTaskClientAsyncImpl<Integer, Long>
  {
    public TestSeekableStreamIndexTaskClientAsyncImpl()
    {
      super(DATASOURCE, serviceClientFactory, null, jsonMapper, httpTimeout, MAX_ATTEMPTS);
    }

    @Override
    public Class<Integer> getPartitionType()
    {
      return Integer.class;
    }

    @Override
    public Class<Long> getSequenceType()
    {
      return Long.class;
    }
  }
}
