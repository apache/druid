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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import io.netty.channel.ChannelException;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Either;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.segment.realtime.firehose.ChatHandlerResource;
import org.easymock.EasyMock;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.function.Function;

public class IndexTaskClientTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final int numRetries = 2;

  @Test
  public void failOnMalformedURLException() throws IOException
  {
    try (IndexTaskClient indexTaskClient = buildIndexTaskClient(
        EasyMock.createNiceMock(HttpClient.class),
        id -> TaskLocation.create(id, -2, -2)
    )) {
      expectedException.expect(MalformedURLException.class);
      expectedException.expectMessage("Invalid port number :-2");

      indexTaskClient.submitRequestWithEmptyContent(
          "taskId",
          HttpMethod.GET,
          "test",
          null,
          true
      );
    }
  }

  @Test
  public void retryOnChannelException() throws IOException
  {
    final HttpClient httpClient = EasyMock.createNiceMock(HttpClient.class);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(Futures.immediateFailedFuture(new ChannelException("IndexTaskClientTest")))
            .times(2);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
                Futures.immediateFuture(
                    Either.value(
                        new StringFullResponseHolder(
                            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK), ""
                        )
                    )
                )
            )
            .once();
    EasyMock.replay(httpClient);
    try (IndexTaskClient indexTaskClient = buildIndexTaskClient(httpClient, id -> TaskLocation.create(id, 8000, -1))) {
      final StringFullResponseHolder response = indexTaskClient.submitRequestWithEmptyContent(
          "taskId",
          HttpMethod.GET,
          "test",
          null,
          true
      );
      Assert.assertEquals(HttpResponseStatus.OK, response.getStatus());
    }
  }

  @Test
  public void retryOnServerError() throws IOException
  {
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
                Futures.immediateFuture(
                    Either.error(
                        new StringFullResponseHolder(
                            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR), "Error"
                        )
                    )
                )
            )
            .times(2);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
                Futures.immediateFuture(
                    Either.value(
                        new StringFullResponseHolder(
                            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK), ""
                        )
                    )
                )
            )
            .once();
    EasyMock.replay(httpClient);
    try (IndexTaskClient indexTaskClient = buildIndexTaskClient(httpClient, id -> TaskLocation.create(id, 8000, -1))) {
      final StringFullResponseHolder response = indexTaskClient.submitRequestWithEmptyContent(
          "taskId",
          HttpMethod.GET,
          "test",
          null,
          true
      );
      Assert.assertEquals(HttpResponseStatus.OK, response.getStatus());
    }
    EasyMock.verify(httpClient);
  }

  @Test
  public void retryIfNotFoundWithIncorrectTaskId() throws IOException
  {
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    final String taskId = "taskId";
    final String incorrectTaskId = "incorrectTaskId";
    final DefaultHttpResponse incorrectResponse =
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
    incorrectResponse.headers().add(ChatHandlerResource.TASK_ID_HEADER, incorrectTaskId);
    final DefaultHttpResponse correctResponse =
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    correctResponse.headers().add(ChatHandlerResource.TASK_ID_HEADER, taskId);

    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
                Futures.immediateFuture(
                    Either.error(
                        new StringFullResponseHolder(incorrectResponse, "")
                    )
                )
            )
            .times(2);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
                Futures.immediateFuture(
                    Either.value(
                        new StringFullResponseHolder(correctResponse, "")
                    )
                )
            )
            .once();
    EasyMock.replay(httpClient);
    try (IndexTaskClient indexTaskClient = buildIndexTaskClient(httpClient, id -> TaskLocation.create(id, 8000, -1))) {
      final StringFullResponseHolder response = indexTaskClient.submitRequestWithEmptyContent(
          taskId,
          HttpMethod.GET,
          "test",
          null,
          true
      );
      Assert.assertEquals(HttpResponseStatus.OK, response.getStatus());
    }
    EasyMock.verify(httpClient);
  }

  @Test
  public void dontRetryOnBadRequest()
  {
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
                Futures.immediateFuture(
                    Either.error(
                        new StringFullResponseHolder(
                            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.BAD_REQUEST),
                            "Error"
                        )
                    )
                )
            )
            .times(1);
    EasyMock.replay(httpClient);
    try (IndexTaskClient indexTaskClient = buildIndexTaskClient(httpClient, id -> TaskLocation.create(id, 8000, -1))) {
      final IllegalArgumentException e = Assert.assertThrows(
          IllegalArgumentException.class,
          () -> indexTaskClient.submitRequestWithEmptyContent("taskId", HttpMethod.GET, "test", null, true)
      );

      Assert.assertEquals(
          "Received server error with status [400 Bad Request]; first 1KB of body: Error",
          e.getMessage()
      );
    }

    EasyMock.verify(httpClient);
  }

  @Test
  public void dontRetryIfRetryFalse()
  {
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
                Futures.immediateFuture(
                    Either.error(
                        new StringFullResponseHolder(
                            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR),
                            "Error"
                        )
                    )
                )
            )
            .times(1);
    EasyMock.replay(httpClient);
    try (IndexTaskClient indexTaskClient = buildIndexTaskClient(httpClient, id -> TaskLocation.create(id, 8000, -1))) {
      final IOException e = Assert.assertThrows(
          IOException.class,
          () -> indexTaskClient.submitRequestWithEmptyContent("taskId", HttpMethod.GET, "test", null, false)
      );

      Assert.assertEquals(
          "Received server error with status [500 Internal Server Error]; first 1KB of body: Error",
          e.getMessage()
      );
    }

    EasyMock.verify(httpClient);
  }

  @Test
  public void dontRetryIfNotFoundWithCorrectTaskId()
  {
    final String taskId = "taskId";
    final HttpClient httpClient = EasyMock.createMock(HttpClient.class);
    final DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
    response.headers().add(ChatHandlerResource.TASK_ID_HEADER, taskId);
    EasyMock.expect(httpClient.go(EasyMock.anyObject(), EasyMock.anyObject(), EasyMock.anyObject()))
            .andReturn(
                Futures.immediateFuture(
                    Either.error(
                        new StringFullResponseHolder(response, "Error")
                    )
                )
            )
            .times(1);
    EasyMock.replay(httpClient);
    try (IndexTaskClient indexTaskClient = buildIndexTaskClient(httpClient, id -> TaskLocation.create(id, 8000, -1))) {
      final IOException e = Assert.assertThrows(
          IOException.class,
          () -> indexTaskClient.submitRequestWithEmptyContent(taskId, HttpMethod.GET, "test", null, false)
      );

      Assert.assertEquals(
          "Received server error with status [404 Not Found]; first 1KB of body: Error",
          e.getMessage()
      );
    }

    EasyMock.verify(httpClient);
  }

  private IndexTaskClient buildIndexTaskClient(
      HttpClient httpClient,
      Function<String, TaskLocation> taskLocationProvider
  )
  {
    final TaskInfoProvider taskInfoProvider = new TaskInfoProvider()
    {
      @Override
      public TaskLocation getTaskLocation(String id)
      {
        return taskLocationProvider.apply(id);
      }

      @Override
      public Optional<TaskStatus> getTaskStatus(String id)
      {
        return Optional.of(TaskStatus.running(id));
      }
    };
    return new TestIndexTaskClient(
        httpClient,
        objectMapper,
        taskInfoProvider,
        new Duration(1000),
        "indexTaskClientTest",
        1,
        numRetries
    );
  }

  private static class TestIndexTaskClient extends IndexTaskClient
  {
    private TestIndexTaskClient(
        HttpClient httpClient,
        ObjectMapper objectMapper,
        TaskInfoProvider taskInfoProvider,
        Duration httpTimeout,
        String callerId,
        int numThreads,
        long numRetries
    )
    {
      super(httpClient, objectMapper, taskInfoProvider, httpTimeout, callerId, numThreads, numRetries);
    }

    @Override
    protected void checkConnection(String host, int port)
    {
      // do nothing
    }
  }
}
