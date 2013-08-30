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

import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.master.MasterSegmentSettings;

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
public class MasterSegmentSettingsResource
{
  private final JacksonConfigManager manager;

  @Inject
  public MasterSegmentSettingsResource(
      JacksonConfigManager manager
  )
  {
    this.manager=manager;
  }
  @GET
  @Produces("application/json")
  public Response getDynamicConfigs()
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK)
                                               .entity(
                                                   manager.watch(MasterSegmentSettings.CONFIG_KEY,MasterSegmentSettings.class).get()
                                               );
    return builder.build();
  }

  @POST
  @Consumes("application/json")
  public Response setDynamicConfigs(
      final MasterSegmentSettings masterSegmentSettings
  )
  {
    if (!manager.set(MasterSegmentSettings.CONFIG_KEY, masterSegmentSettings)) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.status(Response.Status.OK).build();
  }

}
