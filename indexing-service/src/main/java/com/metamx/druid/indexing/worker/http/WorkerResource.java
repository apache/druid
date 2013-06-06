/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.druid.indexing.coordinator.ForkingTaskRunner;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.InputStream;

/**
 */
@Path("/druid/worker/v1")
public class WorkerResource
{
  private static final Logger log = new Logger(WorkerResource.class);

  private final ObjectMapper jsonMapper;
  private final ForkingTaskRunner taskRunner;

  @Inject
  public WorkerResource(
      ObjectMapper jsonMapper,
      ForkingTaskRunner taskRunner

  ) throws Exception
  {
    this.jsonMapper = jsonMapper;
    this.taskRunner = taskRunner;
  }

  @POST
  @Path("/task/{taskid}/shutdown")
  @Produces("application/json")
  public Response doShutdown(@PathParam("taskid") String taskid)
  {
    try {
      taskRunner.shutdown(taskid);
    }
    catch (Exception e) {
      log.error(e, "Failed to issue shutdown for task: %s", taskid);
      return Response.serverError().build();
    }
    return Response.ok(ImmutableMap.of("task", taskid)).build();
  }

  @GET
  @Path("/task/{taskid}/log")
  @Produces("text/plain")
  public Response doGetLog(
      @PathParam("taskid") String taskid,
      @QueryParam("offset") @DefaultValue("0") long offset
  )
  {
    final Optional<InputSupplier<InputStream>> stream = taskRunner.streamTaskLog(taskid, offset);

    if (stream.isPresent()) {
      try {
        return Response.ok(stream.get().getInput()).build();
      } catch (Exception e) {
        log.warn(e, "Failed to read log for task: %s", taskid);
        return Response.serverError().build();
      }
    } else {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
  }
}
