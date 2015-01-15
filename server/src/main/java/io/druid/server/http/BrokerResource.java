/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.druid.client.BrokerServerView;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid/broker/v1")
public class BrokerResource
{
  private final BrokerServerView brokerServerView;

  @Inject
  public BrokerResource(BrokerServerView brokerServerView)
  {
    this.brokerServerView = brokerServerView;
  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus()
  {
    return Response.ok(ImmutableMap.of("inventoryInitialized", brokerServerView.isInitialized())).build();
  }
}
