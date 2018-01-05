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

package io.druid.indexing.overlord.hrtr;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Collection of http endpoits to introspect state of HttpRemoteTaskRunner instance for debugging.
 */
@Path("/druid-internal/v1/httpRemoteTaskRunner")
@ResourceFilters(StateResourceFilter.class)
public class HttpRemoteTaskRunnerResource
{
  private final TaskMaster taskMaster;

  @Inject
  public HttpRemoteTaskRunnerResource(TaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getDebugInfo()
  {
    HttpRemoteTaskRunner httpRemoteTaskRunner = getHttpRemoteTaskRunner();
    if (httpRemoteTaskRunner == null) {
      return Response.status(Response.Status.FORBIDDEN).entity("HttpRemoteTaskRunner is NULL.").build();
    }

    return Response.ok().entity(httpRemoteTaskRunner.getDebugInfo()).build();
  }

  @GET
  @Path("/blacklistedWorkers")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBlacklistedWorkers()
  {
    HttpRemoteTaskRunner httpRemoteTaskRunner = getHttpRemoteTaskRunner();
    if (httpRemoteTaskRunner == null) {
      return Response.status(Response.Status.FORBIDDEN).entity("HttpRemoteTaskRunner is NULL.").build();
    }

    return Response.ok().entity(httpRemoteTaskRunner.getBlacklistedWorkers()).build();
  }

  private HttpRemoteTaskRunner getHttpRemoteTaskRunner()
  {
    Optional<TaskRunner> taskRunnerOpt = taskMaster.getTaskRunner();
    if (taskRunnerOpt.isPresent() && taskRunnerOpt.get() instanceof HttpRemoteTaskRunner) {
      return (HttpRemoteTaskRunner) taskRunnerOpt.get();
    } else {
      return null;
    }
  }
}
