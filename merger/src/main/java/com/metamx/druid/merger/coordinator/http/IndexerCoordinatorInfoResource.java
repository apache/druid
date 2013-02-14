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

package com.metamx.druid.merger.coordinator.http;

import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.coordinator.TaskMaster;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 */
@Path("/mmx/merger/v1/info")
public class IndexerCoordinatorInfoResource
{
  private static final Logger log = new Logger(IndexerCoordinatorInfoResource.class);

  private final TaskMaster taskMaster;

  @Inject
  public IndexerCoordinatorInfoResource(TaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @GET
  @Path("/pendingTasks}")
  @Produces("application/json")
  public Response getPendingTasks()
  {
    if (taskMaster.getTaskRunner() == null) {
      return Response.noContent().build();
    }
    return Response.ok(taskMaster.getTaskRunner().getPendingTasks()).build();
  }

  @GET
  @Path("/runningTasks}")
  @Produces("application/json")
  public Response getRunningTasks()
  {
    if (taskMaster.getTaskRunner() == null) {
      return Response.noContent().build();
    }
    return Response.ok(taskMaster.getTaskRunner().getRunningTasks()).build();
  }
}
