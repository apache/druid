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
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.NotFound;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.indexing.compact.CompactionSupervisorSpec;
import org.apache.druid.indexing.overlord.supervisor.CompactionSupervisorManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorResource;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CoordinatorConfigManager;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfigAuditEntry;
import org.apache.druid.server.http.ServletResourceUtils;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.stream.Collectors;

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
  private final AuthorizerMapper authorizerMapper;
  private final CoordinatorClient coordinatorClient;
  private final CoordinatorConfigManager configManager;
  private final CompactionSupervisorManager supervisorManager;

  @Inject
  public OverlordCompactionResource(
      CompactionScheduler scheduler,
      AuthorizerMapper authorizerMapper,
      CoordinatorClient coordinatorClient,
      CoordinatorConfigManager configManager,
      CompactionSupervisorManager supervisorManager
  )
  {
    this.scheduler = scheduler;
    this.configManager = configManager;
    this.authorizerMapper = authorizerMapper;
    this.coordinatorClient = coordinatorClient;
    this.supervisorManager = supervisorManager;
  }

  @GET
  @Path("/isSupervisorEnabled")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response isCompactionSupervisorEnabled()
  {
    return ServletResourceUtils.buildReadResponse(
        scheduler::isEnabled
    );
  }

  @POST
  @Path("/config/cluster")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response updateClusterCompactionConfig(
      ClusterCompactionConfig updatePayload,
      @Context HttpServletRequest req
  )
  {
    final AuditInfo auditInfo = AuthorizationUtils.buildAuditInfo(req);
    return ServletResourceUtils.buildUpdateResponse(
        () -> configManager.updateClusterCompactionConfig(updatePayload, auditInfo)
    );
  }

  @GET
  @Path("/config/cluster")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getClusterCompactionConfig()
  {
    return ServletResourceUtils.buildReadResponse(
        configManager::getClusterCompactionConfig
    );
  }

  @GET
  @Path("/status/datasources")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getAllCompactionSnapshots(
      @Context HttpServletRequest request
  )
  {
    if (scheduler.isEnabled()) {
      return ServletResourceUtils.buildReadResponse(() -> {
        final List<AutoCompactionSnapshot> allSnapshots =
            List.copyOf(scheduler.getAllCompactionSnapshots().values());
        return new CompactionStatusResponse(
            AuthorizationUtils.filterByAuthorizedDatasources(
                request,
                allSnapshots,
                AutoCompactionSnapshot::getDataSource,
                authorizerMapper
            )
        );
      });
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
      return ServletResourceUtils.buildReadResponse(() -> {
        final List<CompactionSupervisorSpec> configs = AuthorizationUtils.filterByAuthorizedDatasources(
            request,
            supervisorManager.getAllCompactionSupervisors(),
            supervisor -> supervisor.getSpec().getDataSource(),
            authorizerMapper
        );
        return new CompactionConfigsResponse(
            configs.stream()
                   .map(CompactionSupervisorSpec::getSpec)
                   .collect(Collectors.toList())
        );
      });
    } else {
      return ServletResourceUtils.buildReadResponse(() -> {
        final List<DataSourceCompactionConfig> configs = AuthorizationUtils.filterByAuthorizedDatasources(
            request,
            configManager.getCurrentCompactionConfig().getCompactionConfigs(),
            DataSourceCompactionConfig::getDataSource,
            authorizerMapper
        );
        return new CompactionConfigsResponse(configs);
      });
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
      @Context HttpServletRequest request
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
      final CompactionSupervisorSpec spec = new CompactionSupervisorSpec(newConfig, false, scheduler);
      return ServletResourceUtils.buildUpdateResponse(
          () -> supervisorManager.updateCompactionSupervisor(spec, request)
      );
    } else {
      final AuditInfo auditInfo = AuthorizationUtils.buildAuditInfo(request);
      return ServletResourceUtils.buildUpdateResponse(
          () -> configManager.updateDatasourceCompactionConfig(newConfig, auditInfo)
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
      return ServletResourceUtils.buildReadResponse(
          () -> supervisorManager.getCompactionSupervisor(dataSource).getSpec()
      );
    } else {
      return ServletResourceUtils.buildReadResponse(
          () -> configManager.getDatasourceCompactionConfig(dataSource)
      );
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
      return ServletResourceUtils.buildUpdateResponse(
          () -> supervisorManager.deleteCompactionSupervisor(dataSource)
      );
    } else {
      final AuditInfo auditInfo = AuthorizationUtils.buildAuditInfo(req);
      return ServletResourceUtils.buildUpdateResponse(
          () -> configManager.deleteDatasourceCompactionConfig(dataSource, auditInfo)
      );
    }
  }

  @GET
  @Path("/config/datasources/{dataSource}/history")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response getDatasourceCompactionConfigHistory(
      @PathParam("dataSource") String dataSource,
      @QueryParam("interval") String interval,
      @QueryParam("count") Integer count
  )
  {
    if (isEmpty(dataSource)) {
      return invalidInputResponse("No DataSource specified");
    }

    if (scheduler.isEnabled()) {
      return ServletResourceUtils.buildReadResponse(
          () -> new CompactionConfigHistoryResponse(
              filterByCountAndInterval(
                  supervisorManager.getCompactionSupervisorHistory(dataSource),
                  interval,
                  count
              )
          )
      );
    } else {
      return ServletResourceUtils.buildReadResponse(
          () -> new CompactionConfigHistoryResponse(
              configManager.getCompactionConfigHistory(dataSource, interval, count)
          )
      );
    }
  }

  @POST
  @Path("/simulate")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
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

  /**
   * Filters the given list of audit entries by both interval and count, if
   * specified.
   */
  private static List<DataSourceCompactionConfigAuditEntry> filterByCountAndInterval(
      List<DataSourceCompactionConfigAuditEntry> entries,
      @Nullable String serializedInterval,
      @Nullable Integer count
  )
  {
    final Interval interval = serializedInterval == null || serializedInterval.isEmpty()
                              ? null : Intervals.of(serializedInterval);
    return entries.stream()
                  .filter(
                      entry -> interval == null
                               || entry.getAuditTime() == null
                               || interval.contains(entry.getAuditTime())
                  )
                  .limit(count == null ? Integer.MAX_VALUE : count)
                  .collect(Collectors.toList());
  }
}
