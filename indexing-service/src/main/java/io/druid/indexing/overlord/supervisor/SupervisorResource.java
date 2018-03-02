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
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.http.security.SupervisorResourceFilter;
import io.druid.java.util.common.StringUtils;
import io.druid.server.security.Access;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.ForbiddenException;
import io.druid.server.security.ResourceAction;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Endpoints for submitting and starting a {@link SupervisorSpec}, getting running supervisors, stopping supervisors,
 * and getting supervisor history.
 */
@Path("/druid/indexer/v1/supervisor")
public class SupervisorResource
{
  private final TaskMaster taskMaster;
  private final AuthConfig authConfig;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public SupervisorResource(
      TaskMaster taskMaster,
      AuthConfig authConfig,
      AuthorizerMapper authorizerMapper
  )
  {
    this.taskMaster = taskMaster;
    this.authConfig = authConfig;
    this.authorizerMapper = authorizerMapper;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response specPost(final SupervisorSpec spec, @Context final HttpServletRequest req)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            Preconditions.checkArgument(
                spec.getDataSources() != null && spec.getDataSources().size() > 0,
                "No dataSources found to perform authorization checks"
            );

            Access authResult = AuthorizationUtils.authorizeAllResourceActions(
                req,
                Iterables.transform(spec.getDataSources(), AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR),
                authorizerMapper
            );

            if (!authResult.isAllowed()) {
              throw new ForbiddenException(authResult.toString());
            }

            manager.createOrUpdateAndStartSupervisor(spec);
            return Response.ok(ImmutableMap.of("id", spec.getId())).build();
          }
        }
    );
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetAll(@Context final HttpServletRequest req)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(final SupervisorManager manager)
          {
            Set<String> authorizedSupervisorIds = filterAuthorizedSupervisorIds(
                req,
                manager,
                manager.getSupervisorIds()
            );
            return Response.ok(authorizedSupervisorIds).build();
          }
        }
    );
  }

  @GET
  @Path("/{id}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
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
                             .entity(ImmutableMap.of("error", StringUtils.format("[%s] does not exist", id)))
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
  @ResourceFilters(SupervisorResourceFilter.class)
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
                             .entity(ImmutableMap.of("error", StringUtils.format("[%s] does not exist", id)))
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
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response shutdown(@PathParam("id") final String id)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            if (manager.stopAndRemoveSupervisor(id)) {
              return Response.ok(ImmutableMap.of("id", id)).build();
            } else {
              return Response.status(Response.Status.NOT_FOUND)
                             .entity(ImmutableMap.of("error", StringUtils.format("[%s] does not exist", id)))
                             .build();
            }
          }
        }
    );
  }

  @GET
  @Path("/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetAllHistory(@Context final HttpServletRequest req)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(final SupervisorManager manager)
          {
            final Map<String, List<VersionedSupervisorSpec>> supervisorHistory = manager.getSupervisorHistory();

            final Set<String> authorizedSupervisorIds = filterAuthorizedSupervisorIds(
                req,
                manager,
                supervisorHistory.keySet()
            );

            final Map<String, List<VersionedSupervisorSpec>> authorizedSupervisorHistory = Maps.filterKeys(
                supervisorHistory,
                new Predicate<String>()
                {
                  @Override
                  public boolean apply(String id)
                  {
                    return authorizedSupervisorIds.contains(id);
                  }
                }
            );
            return Response.ok(authorizedSupervisorHistory).build();
          }
        }
    );
  }

  @GET
  @Path("/{id}/history")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
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
                                     StringUtils.format(
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

  @POST
  @Path("/{id}/reset")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response reset(@PathParam("id") final String id)
  {
    return asLeaderWithSupervisorManager(
        new Function<SupervisorManager, Response>()
        {
          @Override
          public Response apply(SupervisorManager manager)
          {
            if (manager.resetSupervisor(id, null)) {
              return Response.ok(ImmutableMap.of("id", id)).build();
            } else {
              return Response.status(Response.Status.NOT_FOUND)
                             .entity(ImmutableMap.of("error", StringUtils.format("[%s] does not exist", id)))
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

  private Set<String> filterAuthorizedSupervisorIds(
      final HttpServletRequest req,
      SupervisorManager manager,
      Collection<String> supervisorIds
  )
  {
    Function<String, Iterable<ResourceAction>> raGenerator = supervisorId -> {
      Optional<SupervisorSpec> supervisorSpecOptional = manager.getSupervisorSpec(supervisorId);
      if (supervisorSpecOptional.isPresent()) {
        return Iterables.transform(
            supervisorSpecOptional.get().getDataSources(),
            AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR
        );
      } else {
        return null;
      }
    };

    return Sets.newHashSet(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            supervisorIds,
            raGenerator,
            authorizerMapper
        )
    );
  }
}
