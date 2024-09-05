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

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.error.NotFound;
import org.apache.druid.indexing.compact.CompactionScheduler;
import org.apache.druid.server.compaction.CompactionProgressResponse;
import org.apache.druid.server.compaction.CompactionStatusResponse;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.CompactionSupervisorConfig;
import org.apache.druid.server.http.ServletResourceUtils;
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
import java.util.Collections;

/**
 * Contains the same logic as {@code CoordinatorCompactionResource} but the APIs
 * are served by {@link CompactionScheduler} instead of {@code DruidCoordinator}.
 */
@Path("/druid/indexer/v1/compaction")
public class OverlordCompactionResource
{
  private final CompactionScheduler scheduler;
  private final CompactionSupervisorConfig supervisorConfig;

  @Inject
  public OverlordCompactionResource(
      CompactionSupervisorConfig supervisorConfig,
      CompactionScheduler scheduler
  )
  {
    this.scheduler = scheduler;
    this.supervisorConfig = supervisorConfig;
  }

  @GET
  @Path("/isSupervisorEnabled")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response isCompactionSupervisorEnabled()
  {
    return Response.ok(supervisorConfig.isEnabled()).build();
  }

  @GET
  @Path("/progress")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getCompactionProgress(
      @QueryParam("dataSource") String dataSource
  )
  {
    if (!supervisorConfig.isEnabled()) {
      return buildErrorResponseIfSchedulerDisabled();
    }

    if (dataSource == null || dataSource.isEmpty()) {
      return ServletResourceUtils.buildErrorResponseFrom(InvalidInput.exception("No DataSource specified"));
    }

    final AutoCompactionSnapshot snapshot = scheduler.getCompactionSnapshot(dataSource);
    if (snapshot == null) {
      return ServletResourceUtils.buildErrorResponseFrom(NotFound.exception("Unknown DataSource"));
    } else {
      return Response.ok(new CompactionProgressResponse(snapshot.getBytesAwaitingCompaction()))
                     .build();
    }
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getCompactionSnapshots(
      @QueryParam("dataSource") String dataSource
  )
  {
    if (!supervisorConfig.isEnabled()) {
      return buildErrorResponseIfSchedulerDisabled();
    }

    final Collection<AutoCompactionSnapshot> snapshots;
    if (dataSource == null || dataSource.isEmpty()) {
      snapshots = scheduler.getAllCompactionSnapshots().values();
    } else {
      AutoCompactionSnapshot autoCompactionSnapshot = scheduler.getCompactionSnapshot(dataSource);
      if (autoCompactionSnapshot == null) {
        return ServletResourceUtils.buildErrorResponseFrom(NotFound.exception("Unknown DataSource"));
      }
      snapshots = Collections.singleton(autoCompactionSnapshot);
    }
    return Response.ok(new CompactionStatusResponse(snapshots)).build();
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

  private Response buildErrorResponseIfSchedulerDisabled()
  {
    final String msg = "Compaction Supervisors are disabled on the Overlord."
                       + " Use Coordinator APIs to fetch compaction status.";
    return ServletResourceUtils.buildErrorResponseFrom(
        DruidException.forPersona(DruidException.Persona.USER)
                      .ofCategory(DruidException.Category.UNSUPPORTED)
                      .build(msg)
    );
  }
}
