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

package org.apache.druid.server.http;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.error.InternalServerError;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.NotFound;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.compaction.CompactionProgressResponse;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;

@Path("/druid/coordinator/v1/compaction")
public class CoordinatorCompactionResource
{
  private final DruidCoordinator coordinator;
  private final OverlordClient overlordClient;

  @Inject
  public CoordinatorCompactionResource(
      DruidCoordinator coordinator,
      OverlordClient overlordClient
  )
  {
    this.coordinator = coordinator;
    this.overlordClient = overlordClient;
  }

  /**
   * This API is meant to only be used by Druid's integration tests.
   */
  @POST
  @Path("/compact")
  @ResourceFilters(ConfigResourceFilter.class)
  @VisibleForTesting
  public Response forceTriggerCompaction()
  {
    coordinator.runCompactSegmentsDuty();
    return Response.ok().build();
  }

  @GET
  @Path("/progress")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getCompactionProgress(
      @QueryParam("dataSource") String dataSource
  )
  {
    if (dataSource == null || dataSource.isEmpty()) {
      return ServletResourceUtils.buildErrorResponseFrom(InvalidInput.exception("No DataSource specified"));
    }

    if (isCompactionSupervisorEnabled()) {
      return buildResponse(overlordClient.getBytesAwaitingCompaction(dataSource));
    }

    final AutoCompactionSnapshot snapshot = coordinator.getAutoCompactionSnapshotForDataSource(dataSource);
    if (snapshot == null) {
      return ServletResourceUtils.buildErrorResponseFrom(NotFound.exception("Unknown DataSource"));
    } else {
      return Response.ok(new CompactionProgressResponse(snapshot.getBytesAwaitingCompaction())).build();
    }
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getCompactionSnapshotForDataSource(
      @QueryParam("dataSource") String dataSource
  )
  {
    if (isCompactionSupervisorEnabled()) {
      return buildResponse(overlordClient.getCompactionSnapshots(dataSource));
    }

    final Collection<AutoCompactionSnapshot> snapshots;
    if (dataSource == null || dataSource.isEmpty()) {
      snapshots = coordinator.getAutoCompactionSnapshot().values();
    } else {
      AutoCompactionSnapshot autoCompactionSnapshot = coordinator.getAutoCompactionSnapshotForDataSource(dataSource);
      if (autoCompactionSnapshot == null) {
        return ServletResourceUtils.buildErrorResponseFrom(NotFound.exception("Unknown DataSource"));
      }
      snapshots = ImmutableList.of(autoCompactionSnapshot);
    }
    return Response.ok(new CompactionStatusResponse(snapshots)).build();
  }

  @POST
  @Path("/simulate")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response simulateWithClusterConfigUpdate(
      ClusterCompactionConfig updatePayload
  )
  {
    return Response.ok().entity(
        coordinator.simulateRunWithConfigUpdate(updatePayload)
    ).build();
  }

  private <T> Response buildResponse(ListenableFuture<T> future)
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
   * Check if compaction supervisors are enabled on the Overlord. 
   */
  private boolean isCompactionSupervisorEnabled()
  {
    try {
      return FutureUtils.getUnchecked(overlordClient.isCompactionSupervisorEnabled(), true);
    }
    catch (Exception e) {
      // Overlord is probably on an older version, assume that compaction supervisor is not enabled
      return false;
    }
  }
}
