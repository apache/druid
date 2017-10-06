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

package io.druid.server.http;

import com.google.common.collect.ImmutableMap;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.server.coordination.ZkCoordinator;
import io.druid.server.http.security.StateResourceFilter;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid/historical/v1")
@ResourceFilters(StateResourceFilter.class)
public class HistoricalResource
{
  private final ZkCoordinator coordinator;

  @Inject
  public HistoricalResource(
      ZkCoordinator coordinator
  )
  {
    this.coordinator = coordinator;
  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus()
  {
    return Response.ok(ImmutableMap.of("cacheInitialized", coordinator.isStarted())).build();
  }

  @GET
  @Path("/loadStatusCode")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatusCode()
  {
    if (coordinator.isStarted()) {
      return Response.ok(ImmutableMap.of()).build();
    } else {
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }
}
