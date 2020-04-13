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
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.easymock.EasyMock;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
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
                    new StringFullResponseHolder(
                        HttpResponseStatus.OK,
                        new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
                        StandardCharsets.UTF_8
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

  private IndexTaskClient buildIndexTaskClient(HttpClient httpClient, Function<String, TaskLocation> taskLocationProvider)
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
