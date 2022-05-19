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
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * Collection of http endpoits to introspect state of HttpRemoteTaskRunner instance for debugging.
 * Also, generic TaskRunner state can be introspected by the endpoints in
 * {@link org.apache.druid.indexing.overlord.http.OverlordResource}
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
  @Path("/knownTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllKnownTasks()
  {
    HttpRemoteTaskRunner httpRemoteTaskRunner = getHttpRemoteTaskRunner();
    if (httpRemoteTaskRunner == null) {
      return Response.status(Response.Status.FORBIDDEN).entity("HttpRemoteTaskRunner is NULL.").build();
    }

    return Response.ok().entity(httpRemoteTaskRunner.getKnownTasks()).build();
  }

  @GET
  @Path("/pendingTasksQueue")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPendingTasksQueue()
  {
    HttpRemoteTaskRunner httpRemoteTaskRunner = getHttpRemoteTaskRunner();
    if (httpRemoteTaskRunner == null) {
      return Response.status(Response.Status.FORBIDDEN).entity("HttpRemoteTaskRunner is NULL.").build();
    }

    return Response.ok().entity(httpRemoteTaskRunner.getPendingTasksList()).build();
  }

  @GET
  @Path("/workerSyncerDebugInfo")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWorkerSyncerDebugInfo()
  {
    HttpRemoteTaskRunner httpRemoteTaskRunner = getHttpRemoteTaskRunner();
    if (httpRemoteTaskRunner == null) {
      return Response.status(Response.Status.FORBIDDEN).entity("HttpRemoteTaskRunner is NULL.").build();
    }

    return Response.ok().entity(httpRemoteTaskRunner.getWorkerSyncerDebugInfo()).build();
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

  @GET
  @Path("/lazyWorkers")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLazyWorkers()
  {
    HttpRemoteTaskRunner httpRemoteTaskRunner = getHttpRemoteTaskRunner();
    if (httpRemoteTaskRunner == null) {
      return Response.status(Response.Status.FORBIDDEN).entity("HttpRemoteTaskRunner is NULL.").build();
    }

    return Response.ok().entity(httpRemoteTaskRunner.getLazyWorkers()).build();
  }

  @GET
  @Path("/workersWithUnacknowledgedTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWorkersWithUnacknowledgedTasks()
  {
    HttpRemoteTaskRunner httpRemoteTaskRunner = getHttpRemoteTaskRunner();
    if (httpRemoteTaskRunner == null) {
      return Response.status(Response.Status.FORBIDDEN).entity("HttpRemoteTaskRunner is NULL.").build();
    }

    return Response.ok().entity(httpRemoteTaskRunner.getWorkersWithUnacknowledgedTasks()).build();
  }

  @GET
  @Path("/workersEilgibleToRunTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWorkersEilgibleToRunTasks()
  {
    HttpRemoteTaskRunner httpRemoteTaskRunner = getHttpRemoteTaskRunner();
    if (httpRemoteTaskRunner == null) {
      return Response.status(Response.Status.FORBIDDEN).entity("HttpRemoteTaskRunner is NULL.").build();
    }

    return Response.ok().entity(httpRemoteTaskRunner.getWorkersEligibleToRunTasks()).build();
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
