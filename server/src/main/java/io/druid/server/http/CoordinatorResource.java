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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.timeline.DataSegment;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;

/**
 */
@Path("/druid/coordinator/v1")
public class CoordinatorResource
{
  private final DruidCoordinator coordinator;

  @Inject
  public CoordinatorResource(
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
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    if (simple != null) {
      return Response.ok(coordinator.getSegmentAvailability()).build();
    }

    if (full != null) {
      return Response.ok(coordinator.getReplicationStatus()).build();
    }
    return Response.ok(coordinator.getLoadStatus()).build();
  }

  @GET
  @Path("/loadqueue")
  @Produces("application/json")
  public Response getLoadQueue(
      @QueryParam("simple") String simple,
      @QueryParam("simple") String full
  )
  {
    if (simple != null) {
      return Response.ok(
          Maps.transformValues(
              coordinator.getLoadManagementPeons(),
              new Function<LoadQueuePeon, Object>()
              {
                @Override
                public Object apply(LoadQueuePeon input)
                {
                  long loadSize = 0;
                  for (DataSegment dataSegment : input.getSegmentsToLoad()) {
                    loadSize += dataSegment.getSize();
                  }

                  long dropSize = 0;
                  for (DataSegment dataSegment : input.getSegmentsToDrop()) {
                    dropSize += dataSegment.getSize();
                  }

                  return new ImmutableMap.Builder<>()
                      .put("segmentsToLoad", input.getSegmentsToLoad().size())
                      .put("segmentsToDrop", input.getSegmentsToDrop().size())
                      .put("segmentsToLoadSize", loadSize)
                      .put("segmentsToDropSize", dropSize)
                      .build();
                }
              }
          )
      ).build();
    }

    if (full != null) {
      return Response.ok(coordinator.getLoadManagementPeons()).build();
    }

    return Response.ok(
        Maps.transformValues(
            coordinator.getLoadManagementPeons(),
            new Function<LoadQueuePeon, Object>()
            {
              @Override
              public Object apply(LoadQueuePeon input)
              {
                return new ImmutableMap.Builder<>()
                    .put(
                        "segmentsToLoad",
                        Collections2.transform(
                            input.getSegmentsToLoad(),
                            new Function<DataSegment, Object>()
                            {
                              @Override
                              public String apply(DataSegment segment)
                              {
                                return segment.getIdentifier();
                              }
                            }
                        )
                    )
                    .put(
                        "segmentsToDrop", Collections2.transform(
                        input.getSegmentsToDrop(),
                        new Function<DataSegment, Object>()
                        {
                          @Override
                          public String apply(DataSegment segment)
                          {
                            return segment.getIdentifier();
                          }
                        }
                    )
                    )
                    .build();
              }
            }
        )
    ).build();
  }
}