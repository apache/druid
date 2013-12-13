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
import io.druid.server.coordinator.LoadPeonCallback;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.List;

/**
 */
@Path("/coordinator")
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

  @POST
  @Path("/move")
  @Consumes("application/json")
  public Response moveSegment(List<SegmentToMove> segmentsToMove)
  {
    Response resp = Response.status(Response.Status.OK).build();
    for (SegmentToMove segmentToMove : segmentsToMove) {
      try {
        coordinator.moveSegment(
            segmentToMove.getFromServer(),
            segmentToMove.getToServer(),
            segmentToMove.getSegmentName(),
            new LoadPeonCallback()
            {
              @Override
              public void execute()
              {
                return;
              }
            }
        );
      }
      catch (Exception e) {
        resp = Response
            .status(Response.Status.BAD_REQUEST)
            .entity(e.getMessage())
            .build();
        break;
      }
    }
    return resp;
  }

  @POST
  @Path("/drop")
  @Consumes("application/json")
  public Response dropSegment(List<SegmentToDrop> segmentsToDrop)
  {
    Response resp = Response.status(Response.Status.OK).build();
    for (SegmentToDrop segmentToDrop : segmentsToDrop) {
      try {
        coordinator.dropSegment(
            segmentToDrop.getFromServer(), segmentToDrop.getSegmentName(), new LoadPeonCallback()
        {
          @Override
          public void execute()
          {
            return;
          }
        }
        );
      }
      catch (Exception e) {
        resp = Response
            .status(Response.Status.BAD_REQUEST)
            .entity(e.getMessage())
            .build();
        break;
      }
    }
    return resp;
  }

  @GET
  @Path("/loadstatus")
  @Produces("application/json")
  public Response getLoadStatus()
  {
    return Response.ok(coordinator.getLoadStatus()).build();
  }
}