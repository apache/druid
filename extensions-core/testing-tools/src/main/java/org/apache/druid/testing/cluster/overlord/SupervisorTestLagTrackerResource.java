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

package org.apache.druid.testing.cluster.overlord;

import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.indexing.overlord.http.security.SupervisorResourceFilter;
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

@Path("/druid-internal/v1/test")
public class SupervisorTestLagTrackerResource
{
  private final SupervisorLagTracker lagTracker;

  @Inject
  public SupervisorTestLagTrackerResource(SupervisorLagTracker lagTracker)
  {
    this.lagTracker = lagTracker;
  }

  @POST
  @Path("/supervisor/{id}/lag")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(SupervisorResourceFilter.class)
  public Response postLagForSupervisor(
      @PathParam("id") String supervisorId,
      Map<String, Object> payload
  )
  {
    final LagStats lagStats = new LagStats(
        ((Number) payload.getOrDefault("maxLag", 0)).longValue(),
        ((Number) payload.getOrDefault("totalLag", 0)).longValue(),
        ((Number) payload.getOrDefault("avgLag", 0)).longValue()
    );

    lagTracker.setLag(supervisorId, lagStats);
    return Response.ok().build();
  }
}
