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
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.FullResponseHolder;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexing.common.RetryPolicyConfig;
import org.apache.druid.indexing.common.RetryPolicyFactory;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

public class RemoteTaskActionClientTest
{
  private DruidLeaderClient druidLeaderClient;
  List<TaskLock> result = null;
  private ObjectMapper objectMapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    druidLeaderClient = EasyMock.createMock(DruidLeaderClient.class);

    long now = System.currentTimeMillis();

    result = Collections.singletonList(new TaskLock(
        TaskLockType.SHARED,
        "groupId",
        "dataSource",
        Intervals.utc(now - 30 * 1000, now),
        "version",
        0
    ));
  }

  @Test
  public void testSubmitSimple() throws Exception
  {
    Request request = new Request(HttpMethod.POST, new URL("http://localhost:1234/xx"));
    expect(druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/action"))
        .andReturn(request);

    // return status code 200 and a list with size equals 1
    Map<String, Object> responseBody = new HashMap<String, Object>();
    responseBody.put("result", result);
    String strResult = objectMapper.writeValueAsString(responseBody);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.OK,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );

    // set up mocks
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);


    Task task = new NoopTask("id", null, 0, 0, null, null, null);
    RemoteTaskActionClient client = new RemoteTaskActionClient(
        task, druidLeaderClient, new RetryPolicyFactory(
        new RetryPolicyConfig()
    ), objectMapper
    );
    try {
      result = client.submit(new LockListAction());
    }
    catch (IOException e) {
      Assert.fail("unexpected IOException");
    }

    Assert.assertEquals(1, result.size());
    EasyMock.verify(druidLeaderClient);
  }

  @Test(expected = IOException.class)
  public void testSubmitWithIllegalStatusCode() throws Exception
  {
    // return status code 400 and a list with size equals 1
    Request request = new Request(HttpMethod.POST, new URL("http://localhost:1234/xx"));
    expect(druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/action"))
        .andReturn(request);

    // return status code 200 and a list with size equals 1
    Map<String, Object> responseBody = new HashMap<String, Object>();
    responseBody.put("result", result);
    String strResult = objectMapper.writeValueAsString(responseBody);
    FullResponseHolder responseHolder = new FullResponseHolder(
        HttpResponseStatus.BAD_REQUEST,
        EasyMock.createNiceMock(HttpResponse.class),
        new StringBuilder().append(strResult)
    );

    // set up mocks
    expect(druidLeaderClient.go(request)).andReturn(responseHolder);
    replay(druidLeaderClient);


    Task task = new NoopTask("id", null, 0, 0, null, null, null);
    RemoteTaskActionClient client = new RemoteTaskActionClient(
        task, druidLeaderClient, new RetryPolicyFactory(
        objectMapper.readValue("{\"maxRetryCount\":0}", RetryPolicyConfig.class)
    ), objectMapper
    );
    result = client.submit(new LockListAction());
  }
}
