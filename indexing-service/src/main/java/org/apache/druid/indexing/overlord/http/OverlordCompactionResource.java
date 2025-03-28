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

package org.apache.druid.indexing.overlord.http;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.NotFound;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorResource;
import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStatus;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.http.ServletResourceUtils;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

/**
 * New compaction APIs exposed by the Overlord.
 * <p>
 * If {@link #isCompactionSupervisorEnabled()} is true, then the APIs are served
 * by the Overlord locally, either using the {@link CompactionScheduler} or the
 * {@link SupervisorResource}. Otherwise, the APIs are redirected to the
 * coordinator.
 */
@Path("/druid/indexer/v1/compaction")
public class OverlordCompactionResource
{
  private final CompactionScheduler scheduler;
  private final CoordinatorClient coordinatorClient;
  private final SupervisorResource supervisorResource;

  @Inject
  public OverlordCompactionResource(
      CompactionScheduler scheduler,
      CoordinatorClient coordinatorClient,
      SupervisorResource supervisorResource
  )
  {
    this.scheduler = scheduler;
    this.coordinatorClient = coordinatorClient;
    this.supervisorResource = supervisorResource;
  }

  @GET
  @Path("/isSupervisorEnabled")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response isCompactionSupervisorEnabled()
  {
    return Response.ok(scheduler.isEnabled()).build();
  }

  @GET
  @Path("/status/datasources")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllCompactionSnapshots()
  {
    if (scheduler.isEnabled()) {
      return Response.ok(
          new CompactionStatusResponse(List.copyOf(scheduler.getAllCompactionSnapshots().values()))
      ).build();
    } else {
      return buildResponse(coordinatorClient.getCompactionSnapshots(null));
    }
  }

  @GET
  @Path("/status/datasources/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDatasourceCompactionSnapshot(
      @PathParam("dataSource") String dataSource
  )
  {
    if (isEmpty(dataSource)) {
      return invalidInputResponse("No DataSource specified");
    }

    if (scheduler.isEnabled()) {
      AutoCompactionSnapshot snapshot = scheduler.getCompactionSnapshot(dataSource);
      if (snapshot == null) {
        return ServletResourceUtils.buildErrorResponseFrom(NotFound.exception("Unknown DataSource"));
      } else {
        return Response.ok(snapshot).build();
      }
    } else {
      return buildResponse(
          Futures.transform(
              coordinatorClient.getCompactionSnapshots(dataSource),
              statusResponse -> Iterators.getOnlyElement(statusResponse.getLatestStatus().iterator()),
              MoreExecutors.directExecutor()
          )
      );
    }
  }

  @GET
  @Path("/config/datasources")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllCompactionConfigs(
      @Context HttpServletRequest request
  )
  {
    if (scheduler.isEnabled()) {
      final Response supervisorResponse =
          supervisorResource.specGetAll("includeSpec", true, "includeType", request);

      if (supervisorResponse.getStatus() >= 200 && supervisorResponse.getStatus() < 300) {
        final List<DataSourceCompactionConfig> configs = new ArrayList<>();

        @SuppressWarnings("unchecked")
        final List<SupervisorStatus> allSupervisors = (List<SupervisorStatus>) supervisorResponse.getEntity();
        for (SupervisorStatus status : allSupervisors) {
          final SupervisorSpec spec = status.getSpec();
          if (status.getType().equals(CompactionSupervisorSpec.TYPE)
              && spec instanceof CompactionSupervisorSpec) {
            configs.add(((CompactionSupervisorSpec) spec).getSpec());
          }
        }

        return Response.ok(new CompactionConfigsResponse(configs)).build();
      } else {
        return supervisorResponse;
      }
    } else {
      return buildResponse(
          Futures.transform(
              coordinatorClient.getCompactionConfig(),
              config -> new CompactionConfigsResponse(config.getCompactionConfigs()),
              MoreExecutors.directExecutor()
          )
      );
    }
  }

  @POST
  @Path("/config/datasources/{dataSource}")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response updateDatasourceCompactionConfig(
      @PathParam("dataSource") String dataSource,
      DataSourceCompactionConfig newConfig,
      @Context HttpServletRequest req
  )
  {
    if (isEmpty(dataSource)) {
      return invalidInputResponse("No DataSource specified");
    } else if (!dataSource.equals(newConfig.getDataSource())) {
      return invalidInputResponse(
          "DataSource in spec[%s] does not match DataSource in path[%s]",
          newConfig.getDataSource(), dataSource
      );
    }

    if (scheduler.isEnabled()) {
      return supervisorResource.updateSupervisorSpec(
          new CompactionSupervisorSpec(newConfig, false, scheduler),
          true,
          req
      );
    } else {
      return buildResponse(
          coordinatorClient.updateDatasourceCompactionConfig(newConfig)
      );
    }
  }

  @GET
  @Path("/config/datasources/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDatasourceCompactionConfig(
      @PathParam("dataSource") String dataSource
  )
  {
    if (isEmpty(dataSource)) {
      return invalidInputResponse("No DataSource specified");
    }

    if (scheduler.isEnabled()) {
      final String supervisorId = getCompactionSupervisorId(dataSource);
      final Response supervisorResponse = supervisorResource.specGet(supervisorId);

      if (supervisorResponse.getStatus() >= 200 && supervisorResponse.getStatus() < 300) {
        final Object spec = supervisorResponse.getEntity();
        if (spec instanceof CompactionSupervisorSpec) {
          return Response.ok(((CompactionSupervisorSpec) spec).getSpec()).build();
        } else {
          return ServletResourceUtils.buildErrorResponseFrom(
              InternalServerError.exception(
                  "Supervisor spec for ID[%s] is of unknown type[%s]",
                  supervisorId, spec == null ? null : spec.getClass().getSimpleName()
              )
          );
        }
      } else {
        return supervisorResponse;
      }
    } else {
      return buildResponse(coordinatorClient.getDatasourceCompactionConfig(dataSource));
    }
  }

  @DELETE
  @Path("/config/datasources/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response deleteDatasourceCompactionConfig(
      @PathParam("dataSource") String dataSource,
      @Context HttpServletRequest req
  )
  {
    if (isEmpty(dataSource)) {
      return invalidInputResponse("No DataSource specified");
    }

    if (scheduler.isEnabled()) {
      return supervisorResource.terminate(getCompactionSupervisorId(dataSource));
    } else {
      return buildResponse(
          coordinatorClient.deleteDatasourceCompactionConfig(dataSource)
      );
    }
  }

  @POST
  @Path("/simulate")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response simulateRunWithConfigUpdate(
      ClusterCompactionConfig updatePayload
  )
  {
    return Response.ok().entity(
        scheduler.simulateRunWithConfigUpdate(updatePayload)
    ).build();
  }

  private static boolean isEmpty(String dataSource)
  {
    return dataSource == null || dataSource.isEmpty();
  }

  private static String getCompactionSupervisorId(String dataSource)
  {
    return CompactionSupervisorSpec.ID_PREFIX + dataSource;
  }

  private static Response invalidInputResponse(String message, Object... args)
  {
    return ServletResourceUtils.buildErrorResponseFrom(InvalidInput.exception(message, args));
  }

  private static <T> Response buildResponse(ListenableFuture<T> future)
  {
    try {
      return Response.ok(FutureUtils.getUnchecked(future, true)).build();
    }
    catch (Exception e) {
      if (e.getCause() instanceof HttpResponseException) {
        final HttpResponseException cause = (HttpResponseException) e.getCause();
        return Response.status(cause.getResponse().getStatus().getCode())
                       .entity(cause.getResponse().getContent())
                       .build();
      } else {
        return ServletResourceUtils.buildErrorResponseFrom(
            InternalServerError.exception(e.getMessage())
        );
      }
    }
  }
}
