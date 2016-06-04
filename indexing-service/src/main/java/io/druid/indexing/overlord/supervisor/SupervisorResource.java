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

package io.druid.indexing.overlord.supervisor;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.indexing.overlord.TaskMaster;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 * Endpoints for submitting and starting a {@link SupervisorSpec}, getting running supervisors, stopping supervisors,
 * and getting supervisor history.
 */
@Path("/druid/indexer/v1/supervisor")
public class SupervisorResource
{
  private final TaskMaster taskMaster;

  @Inject
  public SupervisorResource(TaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response specPost(final SupervisorSpec spec)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            if (manager.hasSupervisor(spec.getId())) {
              manager.stopAndRemoveSupervisor(spec.getId(), false);
            }

            manager.createAndStartSupervisor(spec);
            return Response.ok(ImmutableMap.of("id", spec.getId())).build();
          }
        }
    );
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetAll()
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            return Response.ok(manager.getSupervisorIds()).build();
          }
        }
    );
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGet(@PathParam("id") final String id)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            Optional<SupervisorSpec> spec = manager.getSupervisorSpec(id);
            if (!spec.isPresent()) {
              return Response.status(Response.Status.NOT_FOUND)
                             .entity(ImmutableMap.of("error", String.format("[%s] does not exist", id)))
                             .build();
            }

            return Response.ok(spec.get()).build();
          }
        }
    );
  }

  @GET
  @Path("/{id}/status")
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetStatus(@PathParam("id") final String id)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            Optional<SupervisorReport> spec = manager.getSupervisorStatus(id);
            if (!spec.isPresent()) {
              return Response.status(Response.Status.NOT_FOUND)
                             .entity(ImmutableMap.of("error", String.format("[%s] does not exist", id)))
                             .build();
            }

            return Response.ok(spec.get()).build();
          }
        }
    );
  }

  @POST
  @Path("/{id}/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  public Response shutdown(@PathParam("id") final String id)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            if (!manager.hasSupervisor(id)) {
              return Response.status(Response.Status.NOT_FOUND)
                             .entity(ImmutableMap.of("error", String.format("[%s] does not exist", id)))
                             .build();
            }

            manager.stopAndRemoveSupervisor(id, true);
            return Response.ok(ImmutableMap.of("id", id)).build();
          }
        }
    );
  }

  @GET
  @Path("/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetAllHistory()
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            return Response.ok(manager.getSupervisorHistory()).build();
          }
        }
    );
  }

  @GET
  @Path("/{id}/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetHistory(@PathParam("id") final String id)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            Map<String, List<VersionedSupervisorSpec>> history = manager.getSupervisorHistory();
            if (history.containsKey(id)) {
              return Response.ok(history.get(id)).build();
            } else {
              return Response.status(Response.Status.NOT_FOUND)
                             .entity(
                                 ImmutableMap.of(
                                     "error",
                                     String.format(
                                         "No history for [%s] (history available for %s)",
                                         id,
                                         history.keySet()
                                     )
                                 )
                             )
                             .build();
            }
          }
        }
    );
  }

  private Response asLeaderWithSupervisorManager(Function<SupervisorManager, Response> f)
  {
    Optional<SupervisorManager> supervisorManager = taskMaster.getSupervisorManager();
    if (supervisorManager.isPresent()) {
      return f.apply(supervisorManager.get());
    } else {
      // Encourage client to try again soon, when we'll likely have a redirect set up
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }
}
