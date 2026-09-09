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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.RequestBuilder;
import org.apache.druid.rpc.ServiceClient;
import org.apache.druid.rpc.ServiceClientImpl;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RemoteTaskActionClientTest
{
  private ServiceClient directOverlordClient;
  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @BeforeEach
  public void setUp()
  {
    directOverlordClient = EasyMock.createMock(ServiceClient.class);
  }

  @Test
  public void testSubmitSimple() throws Exception
  {
    // return OK response and a list with size equals 1
    final Map<String, Object> expectedResponse = new HashMap<>();
    final List<TaskLock> expectedLocks = Collections.singletonList(new TimeChunkLock(
        TaskLockType.SHARED,
        "groupId",
        "dataSource",
        Intervals.of("2019/2020"),
        "version",
        0
    ));
    expectedResponse.put("result", expectedLocks);

    final ByteArrayInputStream responseStream =
        new ByteArrayInputStream(objectMapper.writeValueAsBytes(expectedResponse));

    final Task task = NoopTask.create();
    final LockListAction action = new LockListAction();

    EasyMock.expect(
                directOverlordClient.request(
                    EasyMock.eq(
                        new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/action")
                            .jsonContent(objectMapper, new TaskActionHolder(task, action))),
                    EasyMock.anyObject(InputStreamResponseHandler.class)
                )
            )
            .andReturn(responseStream);

    EasyMock.replay(directOverlordClient);

    RemoteTaskActionClient client = new RemoteTaskActionClient(task, directOverlordClient, objectMapper);
    final List<TaskLock> locks = client.submit(action);

    Assertions.assertEquals(expectedLocks, locks);
    EasyMock.verify(directOverlordClient);
  }

  @Test
  public void testSubmitWithIllegalStatusCode() throws Exception
  {
    // return status code 400
    final HttpResponse response = EasyMock.createNiceMock(HttpResponse.class);
    EasyMock.expect(response.getStatus()).andReturn(HttpResponseStatus.BAD_REQUEST).anyTimes();
    EasyMock.expect(response.getContent()).andReturn(new BigEndianHeapChannelBuffer(0));
    EasyMock.replay(response);

    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        response,
        StandardCharsets.UTF_8
    ).addChunk("testSubmitWithIllegalStatusCode");

    final Task task = NoopTask.create();
    final LockListAction action = new LockListAction();
    EasyMock.expect(
                directOverlordClient.request(
                    EasyMock.eq(
                        new RequestBuilder(HttpMethod.POST, "/druid/indexer/v1/action")
                            .jsonContent(objectMapper, new TaskActionHolder(task, action))
                    ),
                    EasyMock.anyObject(InputStreamResponseHandler.class)
                )
            )
            .andThrow(new ExecutionException(new HttpResponseException(responseHolder)));

    EasyMock.replay(directOverlordClient);

    RemoteTaskActionClient client = new RemoteTaskActionClient(task, directOverlordClient, objectMapper);
    final IOException exception = Assertions.assertThrows(
        IOException.class,
        () -> client.submit(action)
    );
    Assertions.assertEquals(
        "Error with status[400 Bad Request] and message[testSubmitWithIllegalStatusCode]. "
        + "Check overlord logs for details.",
        exception.getMessage()
    );

    EasyMock.verify(directOverlordClient, response);
  }

  @Test
  public void test_defaultTaskActionRetryPolicy_hasMaxRetryDurationOf10Minutes()
  {
    final RetryPolicyConfig defaultRetryConfig = new RetryPolicyConfig();

    // Build a policy with the default values for min and max wait
    // Value of max attempts is irrelevant since we just want to compute back off times
    final StandardRetryPolicy retryPolicy = StandardRetryPolicy
        .builder()
        .maxAttempts(1)
        .minWaitMillis(defaultRetryConfig.getMinWait().toStandardDuration().getMillis())
        .maxWaitMillis(defaultRetryConfig.getMaxWait().toStandardDuration().getMillis())
        .build();

    final long maxWait10Minutes = 10 * 60 * 1000;
    long totalWaitTimeMillis = 0;
    int attempt = 0;
    for (; totalWaitTimeMillis < maxWait10Minutes; ++attempt) {
      totalWaitTimeMillis += ServiceClientImpl.computeBackoffMs(retryPolicy, attempt);
    }

    Assertions.assertEquals(13, defaultRetryConfig.getMaxRetryCount());
  }
}
