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

package org.apache.druid.indexing.overlord.hrtr;

import com.google.common.base.Optional;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.function.Function;

public class HttpRemoteTaskRunnerResourceTest
{
  @Test
  public void testGetAllKnownTasksForbidden()
  {
    HttpEndpointTestBuilder.builder()
                           .expectStatus(Response.Status.FORBIDDEN)
                           .verify(HttpRemoteTaskRunnerResource::getAllKnownTasks);
  }

  @Test
  public void testGetPendingTaskQueueForbidden()
  {
    HttpEndpointTestBuilder.builder()
                           .expectStatus(Response.Status.FORBIDDEN)
                           .verify(HttpRemoteTaskRunnerResource::getPendingTasksQueue);
  }

  @Test
  public void testGetWorkerSyncerDebugInfoForbidden()
  {
    HttpEndpointTestBuilder.builder()
                           .expectStatus(Response.Status.FORBIDDEN)
                           .verify(HttpRemoteTaskRunnerResource::getWorkerSyncerDebugInfo);
  }

  @Test
  public void testGetBlacklistedWorkersForbidden()
  {
    HttpEndpointTestBuilder.builder()
                           .expectStatus(Response.Status.FORBIDDEN)
                           .verify(HttpRemoteTaskRunnerResource::getBlacklistedWorkers);

  }

  @Test
  public void testGetLazyWorkersForbidden()
  {
    HttpEndpointTestBuilder.builder()
                           .expectStatus(Response.Status.FORBIDDEN)
                           .verify(HttpRemoteTaskRunnerResource::getLazyWorkers);

  }

  @Test
  public void testGetWorkersWithUnacknowledgedTasksForbidden()
  {
    HttpEndpointTestBuilder.builder()
                           .expectStatus(Response.Status.FORBIDDEN)
                           .verify(HttpRemoteTaskRunnerResource::getWorkersWithUnacknowledgedTasks);
  }

  @Test
  public void testGetWorkersEilgibleToRunTasksForbidden()
  {
    HttpEndpointTestBuilder.builder()
                           .expectStatus(Response.Status.FORBIDDEN)
                           .verify(HttpRemoteTaskRunnerResource::getWorkersEilgibleToRunTasks);

  }

  static class HttpEndpointTestBuilder
  {
    private int statusCode;
    private String errorMessage = "HttpRemoteTaskRunner is NULL.";

    public static HttpEndpointTestBuilder builder()
    {
      return new HttpEndpointTestBuilder();
    }

    public HttpEndpointTestBuilder expectStatus(Response.Status status)
    {
      statusCode = status.getStatusCode();
      return this;
    }

    public HttpEndpointTestBuilder expectError(String message)
    {
      this.errorMessage = message;
      return this;
    }

    public void verify(Function<HttpRemoteTaskRunnerResource, Response> targetApi)
    {
      TaskMaster master = EasyMock.createMock(TaskMaster.class);
      EasyMock.expect(master.getTaskRunner()).andReturn(Optional.absent());
      EasyMock.replay(master);

      HttpRemoteTaskRunnerResource resource = new HttpRemoteTaskRunnerResource(master);

      Response response = targetApi.apply(resource);

      Assert.assertEquals(this.statusCode, response.getStatus());
      if (errorMessage != null) {
        Assert.assertEquals(errorMessage, ((Map) response.getEntity()).get("error"));
      }
    }
  }
}
