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

import io.druid.common.config.JacksonConfigManager;
import io.druid.server.master.MasterDynamicConfig;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 */
@Path("/master/config")
public class MasterDynamicConfigsResource
{
  private final JacksonConfigManager manager;

  @Inject
  public MasterDynamicConfigsResource(
      JacksonConfigManager manager
  )
  {
    this.manager = manager;
  }

  @GET
  @Produces("application/json")
  public Response getDynamicConfigs()
  {
    return Response.ok(
        manager.watch(
            MasterDynamicConfig.CONFIG_KEY,
            MasterDynamicConfig.class
        ).get()
    ).build();
  }

  @POST
  @Consumes("application/json")
  public Response setDynamicConfigs(final MasterDynamicConfig dynamicConfig)
  {
    if (!manager.set(MasterDynamicConfig.CONFIG_KEY, dynamicConfig)) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }

}
