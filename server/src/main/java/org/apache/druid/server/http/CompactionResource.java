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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.http.security.InternalInternalResourceFilter;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

@Path("/druid/coordinator/v1/compaction")
public class CompactionResource
{
  private final DruidCoordinator coordinator;
  private final AuthorizerMapper authorizerMapper;

  @Inject
  public CompactionResource(
      DruidCoordinator coordinator,
      AuthorizerMapper authorizerMapper
  )
  {
    this.coordinator = coordinator;
    this.authorizerMapper = authorizerMapper;
  }

  /**
   * This API is meant to only be used by Druid's integration tests.
   */
  @POST
  @Path("/compact")
  @ResourceFilters({ ConfigResourceFilter.class, InternalInternalResourceFilter.class })
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
      @QueryParam("dataSource") String dataSource,
      @Context HttpServletRequest request
  )
  {
    if (authorizerMapper.getAuthVersion().equals(AuthConfig.AUTH_VERSION_2)) {
      final Access authResult = AuthorizationUtils.authorizeResourceAction(
          request,
          new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ),
          authorizerMapper
      );

      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.getMessage());
      }
    }

    final Long notCompactedSegmentSizeBytes = coordinator.getTotalSizeOfSegmentsAwaitingCompaction(dataSource);
    if (notCompactedSegmentSizeBytes == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("error", "unknown dataSource")).build();
    } else {
      return Response.ok(ImmutableMap.of("remainingSegmentSize", notCompactedSegmentSizeBytes)).build();
    }
  }

  @GET
  @Path("/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getCompactionSnapshotForDataSource(
      @QueryParam("dataSource") String dataSource,
      @Context HttpServletRequest request
  )
  {
    final Collection<AutoCompactionSnapshot> snapshots;
    if (dataSource == null || dataSource.isEmpty()) {
      if (authorizerMapper.getAuthVersion().equals(AuthConfig.AUTH_VERSION_2)) {
        snapshots = new HashSet<>();
        AuthorizationUtils.filterAuthorizedResources(
            request,
            coordinator.getAutoCompactionSnapshot().values(),
            input -> Collections.singleton(
                new ResourceAction(new Resource(input.getDataSource(), ResourceType.DATASOURCE), Action.READ)),
            authorizerMapper
        ).forEach(snapshots::add);
      } else {
        snapshots = coordinator.getAutoCompactionSnapshot().values();
      }
    } else {
      if (authorizerMapper.getAuthVersion().equals(AuthConfig.AUTH_VERSION_2)) {
        final Access authResult = AuthorizationUtils.authorizeResourceAction(
            request,
            new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ),
            authorizerMapper
        );

        if (!authResult.isAllowed()) {
          throw new ForbiddenException(authResult.getMessage());
        }
      }

      AutoCompactionSnapshot autoCompactionSnapshot = coordinator.getAutoCompactionSnapshotForDataSource(dataSource);
      if (autoCompactionSnapshot == null) {
        return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("error", "unknown dataSource")).build();
      }
      snapshots = ImmutableList.of(autoCompactionSnapshot);
    }
    return Response.ok(ImmutableMap.of("latestStatus", snapshots)).build();
  }
}
