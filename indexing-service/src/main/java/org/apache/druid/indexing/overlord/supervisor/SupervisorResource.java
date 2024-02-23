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

package org.apache.druid.indexing.overlord.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.http.security.SupervisorResourceFilter;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.incremental.ParseExceptionReport;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Endpoints for submitting and starting a {@link SupervisorSpec}, getting running supervisors, stopping supervisors,
 * and getting supervisor history.
 */
@Path("/druid/indexer/v1/supervisor")
public class SupervisorResource
{
  private static final Function<VersionedSupervisorSpec, Iterable<ResourceAction>> SPEC_DATASOURCE_READ_RA_GENERATOR =
      supervisorSpec -> {
        if (supervisorSpec.getSpec() == null) {
          return null;
        }
        if (supervisorSpec.getSpec().getDataSources() == null) {
          return new ArrayList<>();
        }
        return Iterables.transform(
            supervisorSpec.getSpec().getDataSources(),
            AuthorizationUtils.DATASOURCE_READ_RA_GENERATOR
        );
      };

  private final TaskMaster taskMaster;
  private final AuthorizerMapper authorizerMapper;
  private final ObjectMapper objectMapper;
  private final AuditManager auditManager;
  private final AuthConfig authConfig;

  @Inject
  public SupervisorResource(
      TaskMaster taskMaster,
      AuthorizerMapper authorizerMapper,
      ObjectMapper objectMapper,
      AuthConfig authConfig,
      AuditManager auditManager
  )
  {
    this.taskMaster = taskMaster;
    this.authorizerMapper = authorizerMapper;
    this.objectMapper = objectMapper;
    this.authConfig = authConfig;
    this.auditManager = auditManager;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response specPost(final SupervisorSpec spec, @Context final HttpServletRequest req)
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          Preconditions.checkArgument(
              spec.getDataSources() != null && spec.getDataSources().size() > 0,
              "No dataSources found to perform authorization checks"
          );
          final Set<ResourceAction> resourceActions;
          try {
            resourceActions = getNeededResourceActionsForTask(spec);
          }
          catch (UOE e) {
            return Response.status(Response.Status.BAD_REQUEST)
                           .entity(
                               ImmutableMap.of(
                                   "error",
                                   e.getMessage()
                               )
                           )
                           .build();
          }

          Access authResult = AuthorizationUtils.authorizeAllResourceActions(
              req,
              resourceActions,
              authorizerMapper
          );

          if (!authResult.isAllowed()) {
            throw new ForbiddenException(authResult.toString());
          }

          manager.createOrUpdateAndStartSupervisor(spec);

          final String auditPayload
              = StringUtils.format("Update supervisor[%s] for datasource[%s]", spec.getId(), spec.getDataSources());
          auditManager.doAudit(
              AuditEntry.builder()
                        .key(spec.getId())
                        .type("supervisor")
                        .auditInfo(AuthorizationUtils.buildAuditInfo(req))
                        .request(AuthorizationUtils.buildRequestInfo("overlord", req))
                        .payload(auditPayload)
                        .build()
          );

          return Response.ok(ImmutableMap.of("id", spec.getId())).build();
        }
    );
  }

  private Set<ResourceAction> getNeededResourceActionsForTask(final SupervisorSpec spec)
  {
    final Set<ResourceAction> resourceActions =
        spec.getDataSources().stream()
            .map(dataSource -> new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE))
            .collect(Collectors.toSet());
    if (authConfig.isEnableInputSourceSecurity()) {
      resourceActions.addAll(spec.getInputSourceResources());
    }
    return resourceActions;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetAll(
      @QueryParam("full") String full,
      @QueryParam("state") Boolean state,
      @QueryParam("system") String system,
      @Context final HttpServletRequest req
  )
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          Set<String> authorizedSupervisorIds = filterAuthorizedSupervisorIds(
              req,
              manager,
              manager.getSupervisorIds()
          );
          final boolean includeFull = full != null;
          final boolean includeState = state != null && state;
          final boolean includeSystem = system != null;

          if (includeFull || includeState || includeSystem) {
            List<SupervisorStatus> allStates = authorizedSupervisorIds
                .stream()
                .map(x -> {
                  Optional<SupervisorStateManager.State> theState =
                      manager.getSupervisorState(x);
                  SupervisorStatus.Builder theBuilder = new SupervisorStatus.Builder();
                  theBuilder.withId(x);
                  if (theState.isPresent()) {
                    theBuilder.withState(theState.get().getBasicState().toString())
                              .withDetailedState(theState.get().toString())
                              .withHealthy(theState.get().isHealthy());
                  }
                  if (includeFull) {
                    Optional<SupervisorSpec> theSpec = manager.getSupervisorSpec(x);
                    if (theSpec.isPresent()) {
                      theBuilder.withSpec(manager.getSupervisorSpec(x).get());
                    }
                  }
                  if (includeSystem) {
                    Optional<SupervisorSpec> theSpec = manager.getSupervisorSpec(x);
                    if (theSpec.isPresent()) {
                      try {
                        // serializing SupervisorSpec here, so that callers of `druid/indexer/v1/supervisor?system`
                        // which are outside the overlord process can deserialize the response and get a json
                        // payload of SupervisorSpec object when they don't have guice bindings for all the fields
                        // for example, broker does not have bindings for all fields of `KafkaSupervisorSpec` or
                        // `KinesisSupervisorSpec`
                        theBuilder.withSpecString(objectMapper.writeValueAsString(manager.getSupervisorSpec(x).get()));
                      }
                      catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                      }
                      theBuilder.withType(manager.getSupervisorSpec(x).get().getType())
                                .withSource(manager.getSupervisorSpec(x).get().getSource())
                                .withSuspended(manager.getSupervisorSpec(x).get().isSuspended());
                    }
                  }
                  return theBuilder.build();
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
            return Response.ok(allStates).build();
          }

          return Response.ok(authorizedSupervisorIds).build();
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
        manager -> {
          Optional<SupervisorSpec> spec = manager.getSupervisorSpec(id);
          if (!spec.isPresent()) {
            return Response.status(Response.Status.NOT_FOUND)
                           .entity(ImmutableMap.of("error", StringUtils.format("[%s] does not exist", id)))
                           .build();
          }

          return Response.ok(spec.get()).build();
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
        manager -> {
          Optional<SupervisorReport> spec = manager.getSupervisorStatus(id);
          if (!spec.isPresent()) {
            return Response.status(Response.Status.NOT_FOUND)
                           .entity(ImmutableMap.of("error", StringUtils.format("[%s] does not exist", id)))
                           .build();
          }

          return Response.ok(spec.get()).build();
        }
    );
  }

  @GET
  @Path("/{id}/health")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response specGetHealth(@PathParam("id") final String id)
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          Optional<Boolean> healthy = manager.isSupervisorHealthy(id);
          if (!healthy.isPresent()) {
            return Response.status(Response.Status.NOT_FOUND)
                           .entity(ImmutableMap.of(
                               "error",
                               StringUtils.format("[%s] does not exist or health check not implemented", id)
                           ))
                           .build();
          }

          return Response.status(healthy.get() ? Response.Status.OK : Response.Status.SERVICE_UNAVAILABLE)
                         .entity(ImmutableMap.of("healthy", healthy.get()))
                         .build();
        }
    );
  }

  @GET
  @Path("/{id}/stats")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response getAllTaskStats(
      @PathParam("id") final String id
  )
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          Optional<Map<String, Map<String, Object>>> stats = manager.getSupervisorStats(id);
          if (!stats.isPresent()) {
            return Response.status(Response.Status.NOT_FOUND)
                           .entity(
                               ImmutableMap.of(
                                   "error",
                                   StringUtils.format("[%s] does not exist", id)
                               )
                           )
                           .build();
          }

          return Response.ok(stats.get()).build();
        }
    );
  }

  @GET
  @Path("/{id}/parseErrors")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response getAllTaskParseErrors(
      @PathParam("id") final String id
  )
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          Optional<List<ParseExceptionReport>> parseErrors = manager.getSupervisorParseErrors(id);
          if (!parseErrors.isPresent()) {
            return Response.status(Response.Status.NOT_FOUND)
                           .entity(
                               ImmutableMap.of(
                                   "error",
                                   StringUtils.format("[%s] does not exist", id)
                               )
                           )
                           .build();
          }

          return Response.ok(parseErrors.get()).build();
        }
    );
  }

  @POST
  @Path("/{id}/resume")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response specResume(@PathParam("id") final String id)
  {
    return specSuspendOrResume(id, false);
  }

  @POST
  @Path("/{id}/suspend")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response specSuspend(@PathParam("id") final String id)
  {
    return specSuspendOrResume(id, true);
  }

  @Deprecated
  @POST
  @Path("/{id}/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response shutdown(@PathParam("id") final String id)
  {
    return terminate(id);
  }

  @POST
  @Path("/{id}/terminate")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response terminate(@PathParam("id") final String id)
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          if (manager.stopAndRemoveSupervisor(id)) {
            return Response.ok(ImmutableMap.of("id", id)).build();
          } else {
            return Response.status(Response.Status.NOT_FOUND)
                           .entity(ImmutableMap.of("error", StringUtils.format("[%s] does not exist", id)))
                           .build();
          }
        }
    );
  }

  @POST
  @Path("/suspendAll")
  @Produces(MediaType.APPLICATION_JSON)
  public Response suspendAll(@Context final HttpServletRequest req)
  {
    return suspendOrResumeAll(req, true);
  }

  @POST
  @Path("/resumeAll")
  @Produces(MediaType.APPLICATION_JSON)
  public Response resumeAll(@Context final HttpServletRequest req)
  {
    return suspendOrResumeAll(req, false);
  }

  @POST
  @Path("/terminateAll")
  @Produces(MediaType.APPLICATION_JSON)
  public Response terminateAll(@Context final HttpServletRequest req)
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          Set<String> authorizedSupervisorIds = filterAuthorizedSupervisorIds(
              req,
              manager,
              manager.getSupervisorIds()
          );

          for (final String supervisorId : authorizedSupervisorIds) {
            manager.stopAndRemoveSupervisor(supervisorId);
          }

          return Response.ok(ImmutableMap.of("status", "success")).build();
        }
    );
  }

  @GET
  @Path("/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetAllHistory(@Context final HttpServletRequest req)
  {
    return asLeaderWithSupervisorManager(
        manager -> Response.ok(
            AuthorizationUtils.filterAuthorizedResources(
                req,
                manager.getSupervisorHistory(),
                SPEC_DATASOURCE_READ_RA_GENERATOR,
                authorizerMapper
            )
        ).build()
    );
  }

  @GET
  @Path("/{id}/history")
  @Produces(MediaType.APPLICATION_JSON)
  public Response specGetHistory(
      @Context final HttpServletRequest req,
      @PathParam("id") final String id
  )
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          List<VersionedSupervisorSpec> historyForId = manager.getSupervisorHistoryForId(id);
          if (!historyForId.isEmpty()) {
            final List<VersionedSupervisorSpec> authorizedHistoryForId =
                Lists.newArrayList(
                    AuthorizationUtils.filterAuthorizedResources(
                        req,
                        historyForId,
                        SPEC_DATASOURCE_READ_RA_GENERATOR,
                        authorizerMapper
                    )
                );
            if (authorizedHistoryForId.size() > 0) {
              return Response.ok(authorizedHistoryForId).build();
            }
          }

          return Response.status(Response.Status.NOT_FOUND)
                         .entity(
                             ImmutableMap.of(
                                 "error",
                                 StringUtils.format("No history for [%s].", id)
                             )
                         )
                         .build();

        }
    );
  }

  @POST
  @Path("/{id}/reset")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response reset(@PathParam("id") final String id)
  {
    return handleResetRequest(id, null);
  }

  @POST
  @Path("/{id}/resetOffsets")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response resetOffsets(
      @PathParam("id") final String id,
      final DataSourceMetadata resetDataSourceMetadata
  )
  {
    return handleResetRequest(id, resetDataSourceMetadata);
  }

  private Response handleResetRequest(
      final String id,
      @Nullable final DataSourceMetadata resetDataSourceMetadata
  )
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          if (manager.resetSupervisor(id, resetDataSourceMetadata)) {
            return Response.ok(ImmutableMap.of("id", id)).build();
          } else {
            return Response.status(Response.Status.NOT_FOUND)
                           .entity(ImmutableMap.of("error", StringUtils.format("[%s] does not exist", id)))
                           .build();
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

  private Response specSuspendOrResume(final String id, boolean suspend)
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          if (manager.suspendOrResumeSupervisor(id, suspend)) {
            Optional<SupervisorSpec> spec = manager.getSupervisorSpec(id);
            return Response.ok(spec.get()).build();
          } else {
            Optional<SupervisorSpec> spec = manager.getSupervisorSpec(id);
            Response.Status status;
            String errMsg;
            if (spec.isPresent()) {
              status = Response.Status.BAD_REQUEST;
              errMsg = StringUtils.format("[%s] is already %s", id, suspend ? "suspended" : "running");
            } else {
              status = Response.Status.NOT_FOUND;
              errMsg = StringUtils.format("[%s] does not exist", id);
            }
            return Response.status(status)
                           .entity(ImmutableMap.of("error", errMsg))
                           .build();
          }
        }
    );
  }

  private Response suspendOrResumeAll(final HttpServletRequest req, final boolean suspend)
  {
    return asLeaderWithSupervisorManager(
        manager -> {
          Set<String> authorizedSupervisorIds = filterAuthorizedSupervisorIds(
              req,
              manager,
              manager.getSupervisorIds()
          );

          for (final String supervisorId : authorizedSupervisorIds) {
            manager.suspendOrResumeSupervisor(supervisorId, suspend);
          }

          return Response.ok(ImmutableMap.of("status", "success")).build();
        }
    );
  }
}
