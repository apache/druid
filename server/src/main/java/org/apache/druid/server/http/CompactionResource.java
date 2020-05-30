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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid/coordinator/v1/compaction")
public class CompactionResource
{
  private final DruidCoordinator coordinator;

  @Inject
  public CompactionResource(
      DruidCoordinator coordinator
  )
  {
    this.coordinator = coordinator;
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
    final Long notCompactedSegmentSizeBytes = coordinator.getTotalSizeOfSegmentsAwaitingCompaction(dataSource);
    if (notCompactedSegmentSizeBytes == null) {
      return Response.status(Response.Status.BAD_REQUEST).entity(ImmutableMap.of("error", "unknown dataSource")).build();
    } else {
      return Response.ok(ImmutableMap.of("remainingSegmentSize", notCompactedSegmentSizeBytes)).build();
    }
  }
}
