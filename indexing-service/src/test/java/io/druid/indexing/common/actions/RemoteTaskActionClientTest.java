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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.Futures;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.indexing.common.RetryPolicyConfig;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.jackson.DefaultObjectMapper;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;

public class RemoteTaskActionClientTest
{

  private HttpClient httpClient;
  private ServerDiscoverySelector selector;
  private Server server;
  List<TaskLock> result = null;
  private ObjectMapper objectMapper = new DefaultObjectMapper();

  @Before
  public void setUp()
  {
    httpClient = createMock(HttpClient.class);
    selector = createMock(ServerDiscoverySelector.class);

    server = new Server()
    {

      @Override
      public String getScheme()
      {
        return "http";
      }

      @Override
      public int getPort()
      {
        return 8080;
      }

      @Override
      public String getHost()
      {
        return "localhost";
      }

      @Override
      public String getAddress()
      {
        return "localhost";
      }
    };

    long now = System.currentTimeMillis();

    result = Collections.singletonList(new TaskLock(
        "groupId",
        "dataSource",
        new Interval(now - 30 * 1000, now),
        "version"
    ));
  }

  @Test
  public void testSubmitSimple() throws JsonProcessingException
  {
    // return status code 200 and a list with size equals 1
    Map<String, Object> responseBody = new HashMap<String, Object>();
    responseBody.put("result", result);
    String strResult = objectMapper.writeValueAsString(responseBody);
    StatusResponseHolder responseHolder = new StatusResponseHolder(
        HttpResponseStatus.OK,
        new StringBuilder().append(strResult)
    );

    // set up mocks
    expect(selector.pick()).andReturn(server);
    replay(selector);

    expect(httpClient.go(anyObject(Request.class), anyObject(StatusResponseHandler.class))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replay(httpClient);

    Task task = new NoopTask("id", 0, 0, null, null, null);
    RemoteTaskActionClient client = new RemoteTaskActionClient(
        task, httpClient, selector, new RetryPolicyFactory(
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
    EasyMock.verify(selector, httpClient);
  }

  @Test(expected = IOException.class)
  public void testSubmitWithIllegalStatusCode() throws IOException
  {
    // return status code 400 and a list with size equals 1
    Map<String, Object> responseBody = new HashMap<String, Object>();
    responseBody.put("result", result);
    String strResult = objectMapper.writeValueAsString(responseBody);
    StatusResponseHolder responseHolder = new StatusResponseHolder(
        HttpResponseStatus.BAD_REQUEST,
        new StringBuilder().append(strResult)
    );

    // set up mocks
    expect(selector.pick()).andReturn(server);
    replay(selector);

    expect(httpClient.go(anyObject(Request.class), anyObject(StatusResponseHandler.class))).andReturn(
        Futures.immediateFuture(responseHolder)
    );
    replay(httpClient);

    Task task = new NoopTask("id", 0, 0, null, null, null);
    RemoteTaskActionClient client = new RemoteTaskActionClient(
        task, httpClient, selector, new RetryPolicyFactory(
        objectMapper.readValue("{\"maxRetryCount\":0}", RetryPolicyConfig.class)
    ), objectMapper
    );
    result = client.submit(new LockListAction());
  }
}
