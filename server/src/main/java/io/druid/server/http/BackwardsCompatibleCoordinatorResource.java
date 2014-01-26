/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.http;

import com.google.inject.Inject;
import io.druid.server.coordinator.DruidCoordinator;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 */
@Deprecated
@Path("/coordinator")
public class BackwardsCompatibleCoordinatorResource
{
  private final DruidCoordinator coordinator;

  @Inject
  public BackwardsCompatibleCoordinatorResource(
      DruidCoordinator coordinator
  )
  {
    this.coordinator = coordinator;
  }

  @GET
  @Path("/leader")
  @Produces("application/json")
  public Response getLeader()
  {
    return Response.ok(coordinator.getCurrentLeader()).build();
  }

  @GET
  @Path("/loadstatus")
  @Produces("application/json")
  public Response getLoadStatus(
  )
  {
    return Response.ok(coordinator.getLoadStatus()).build();
  }
}