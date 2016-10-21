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

import com.google.common.base.Function;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.client.DruidDataSource;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.java.util.common.MapUtils;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Set;

/**
 */
@Path("/druid/coordinator/v1/tiers")
@ResourceFilters(StateResourceFilter.class)
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
  @Produces(MediaType.APPLICATION_JSON)
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

  @GET
  @Path("/{tierName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTierDatasources(
      @PathParam("tierName") String tierName,
      @QueryParam("simple") String simple
  )
  {
    if (simple != null) {
      Table<String, Interval, Map<String, Object>> retVal = HashBasedTable.create();
      for (DruidServer druidServer : serverInventoryView.getInventory()) {
        if (druidServer.getTier().equalsIgnoreCase(tierName)) {
          for (DataSegment dataSegment : druidServer.getSegments().values()) {
            Map<String, Object> properties = retVal.get(dataSegment.getDataSource(), dataSegment.getInterval());
            if (properties == null) {
              properties = Maps.newHashMap();
              retVal.put(dataSegment.getDataSource(), dataSegment.getInterval(), properties);
            }
            properties.put("size", MapUtils.getLong(properties, "size", 0L) + dataSegment.getSize());
            properties.put("count", MapUtils.getInt(properties, "count", 0) + 1);
          }
        }
      }

      return Response.ok(retVal.rowMap()).build();
    }

    Set<String> retVal = Sets.newHashSet();
    for (DruidServer druidServer : serverInventoryView.getInventory()) {
      if (druidServer.getTier().equalsIgnoreCase(tierName)) {
        retVal.addAll(
            Lists.newArrayList(
                Iterables.transform(
                    druidServer.getDataSources(),
                    new Function<DruidDataSource, String>()
                    {
                      @Override
                      public String apply(DruidDataSource input)
                      {
                        return input.getName();
                      }
                    }
                )
            )
        );
      }
    }

    return Response.ok(retVal).build();
  }
}
