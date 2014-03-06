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

import com.google.api.client.util.Maps;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.metamx.common.MapUtils;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Set;

/**
 */
@Path("/druid/coordinator/v1/tiers")
public class TiersResource
{
  private final InventoryView serverInventoryView;

  @Inject
  public TiersResource(
      InventoryView serverInventoryView
  )
  {
    this.serverInventoryView = serverInventoryView;
  }

  @GET
  @Produces("application/json")
  public Response getTiers(
      @QueryParam("simple") String simple
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);

    if (simple != null) {
      Map<String, Map<String, Long>> metadata = Maps.newHashMap();
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        Map<String, Long> tierMetadata = metadata.get(druidServer.getTier());

        if (tierMetadata == null) {
          tierMetadata = Maps.newHashMap();
          metadata.put(druidServer.getTier(), tierMetadata);
        }

        Long currSize = tierMetadata.get("currSize");
        tierMetadata.put("currSize", ((currSize == null) ? 0 : currSize) + druidServer.getCurrSize());

        Long maxSize = tierMetadata.get("maxSize");
        tierMetadata.put("maxSize", ((maxSize == null) ? 0 : maxSize) + druidServer.getMaxSize());
      }
      return builder.entity(metadata).build();
    }

    Set<String> tiers = Sets.newHashSet();
    for (DruidServer server : serverInventoryView.getInventory()) {
      tiers.add(server.getTier());
    }

    return builder.entity(tiers).build();
  }
}
