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
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.easymock.EasyMock;
import org.jboss.netty.buffer.BigEndianHeapChannelBuffer;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RemoteTaskActionClientTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  private DruidLeaderClient druidLeaderClient;
  private final ObjectMapper objectMapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);
  }

  @Test
  public void testSubmitSimple() throws Exception
  {
    Request request = new Request(HttpMethod.POST, new URL("http://localhost:1234/xx"));
    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/action"))
            .andReturn(request);

    // return status code 200 and a list with size equals 1
    Map<String, Object> responseBody = new HashMap<>();
    final List<TaskLock> expectedLocks = Collections.singletonList(new TimeChunkLock(
        TaskLockType.SHARED,
        "groupId",
        "dataSource",
        Intervals.of("2019/2020"),
        "version",
        0
    ));
    responseBody.put("result", expectedLocks);
    String strResult = objectMapper.writeValueAsString(responseBody);
    final HttpResponse response = EasyMock.createNiceMock(HttpResponse.class);
    EasyMock.expect(response.getContent()).andReturn(new BigEndianHeapChannelBuffer(0));
    EasyMock.replay(response);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        HttpResponseStatus.OK,
        response,
        StandardCharsets.UTF_8
    ).addChunk(strResult);

    // set up mocks
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);

    Task task = NoopTask.create("id", 0);
    RemoteTaskActionClient client = new RemoteTaskActionClient(
        task,
        druidLeaderClient,
        new RetryPolicyFactory(new RetryPolicyConfig()),
        objectMapper
    );
    final List<TaskLock> locks = client.submit(new LockListAction());

    Assert.assertEquals(expectedLocks, locks);
    EasyMock.verify(druidLeaderClient);
  }

  @Test
  public void testSubmitWithIllegalStatusCode() throws Exception
  {
    // return status code 400 and a list with size equals 1
    Request request = new Request(HttpMethod.POST, new URL("http://localhost:1234/xx"));
    EasyMock.expect(druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/action"))
            .andReturn(request);

    // return status code 200 and a list with size equals 1
    final HttpResponse response = EasyMock.createNiceMock(HttpResponse.class);
    EasyMock.expect(response.getContent()).andReturn(new BigEndianHeapChannelBuffer(0));
    EasyMock.replay(response);
    StringFullResponseHolder responseHolder = new StringFullResponseHolder(
        HttpResponseStatus.BAD_REQUEST,
        response,
        StandardCharsets.UTF_8
    ).addChunk("testSubmitWithIllegalStatusCode");

    // set up mocks
    EasyMock.expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    EasyMock.replay(druidLeaderClient);

    Task task = NoopTask.create("id", 0);
    RemoteTaskActionClient client = new RemoteTaskActionClient(
        task,
        druidLeaderClient,
        new RetryPolicyFactory(objectMapper.readValue("{\"maxRetryCount\":0}", RetryPolicyConfig.class)),
        objectMapper
    );
    expectedException.expect(IOException.class);
    expectedException.expectMessage(
        "Error with status[400 Bad Request] and message[testSubmitWithIllegalStatusCode]. "
        + "Check overlord logs for details."
    );
    client.submit(new LockListAction());
  }
}
