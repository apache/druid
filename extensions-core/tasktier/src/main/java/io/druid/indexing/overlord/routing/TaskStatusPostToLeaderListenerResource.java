/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.routing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import io.druid.indexing.common.TaskStatus;
import io.druid.server.DruidNode;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URL;

@Path(TaskStatusPostToLeaderListenerResource.PATH)
public class TaskStatusPostToLeaderListenerResource
{
  private final TaskStatusReporter upstreamReporter;

  @Inject
  public TaskStatusPostToLeaderListenerResource(
      @Named(TaskTierModule.UPSTREAM_TASK_REPORTER_NAME) TaskStatusReporter upstreamReporter
  )
  {
    this.upstreamReporter = upstreamReporter;
  }

  public static final String PATH = "/druid/indexer/tier/v1/report";

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response doPost(@NotNull TaskStatus status)
  {
    try {
      Preconditions.checkNotNull(status, "status");
      if (upstreamReporter.reportStatus(status)) {
        return Response.status(Response.Status.ACCEPTED).entity(status).build();
      }
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).entity(status).build();
    }
    catch (RuntimeException e) {
      return Response.serverError()
                     .entity(ImmutableMap.of("error", e.getMessage() == null ? "null" : e.getMessage()))
                     .build();
    }
  }

  public static URL makeReportUrl(DruidNode node) throws MalformedURLException
  {
    return new URL("http", node.getHost(), node.getPort(), PATH);
  }
}
