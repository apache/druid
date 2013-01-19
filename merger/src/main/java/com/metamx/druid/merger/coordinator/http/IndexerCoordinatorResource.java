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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.MergeTask;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupData;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import com.metamx.emitter.service.ServiceEmitter;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;

/**
 */
@Path("/mmx/merger/v1")
public class IndexerCoordinatorResource
{
  private static final Logger log = new Logger(IndexerCoordinatorResource.class);

  private final IndexerCoordinatorConfig config;
  private final ServiceEmitter emitter;
  private final TaskQueue tasks;
  private final WorkerSetupManager workerSetupManager;

  @Inject
  public IndexerCoordinatorResource(
      IndexerCoordinatorConfig config,
      ServiceEmitter emitter,
      TaskQueue tasks,
      WorkerSetupManager workerSetupManager

  ) throws Exception
  {
    this.config = config;
    this.emitter = emitter;
    this.tasks = tasks;
    this.workerSetupManager = workerSetupManager;
  }

  @POST
  @Path("/merge")
  @Consumes("application/json")
  @Produces("application/json")
  public Response doMerge(
      final MergeTask task
  )
  {
    // legacy endpoint
    return doIndex(task);
  }

  @POST
  @Path("/index")
  @Consumes("application/json")
  @Produces("application/json")
  public Response doIndex(
      final Task task
  )
  {
    // verify against whitelist
    if (config.isWhitelistEnabled() && !config.getWhitelistDatasources().contains(task.getDataSource())) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(
                         ImmutableMap.of(
                             "error",
                             String.format("dataSource[%s] is not whitelisted", task.getDataSource())
                         )
                     )
                     .build();
    }

    tasks.add(task);
    return okResponse(task.getId());
  }

  @GET
  @Path("/status/{taskid}")
  @Produces("application/json")
  public Response doStatus(@PathParam("taskid") String taskid)
  {
    final Optional<TaskStatus> status = tasks.getStatus(taskid);
    if (!status.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      return Response.ok().entity(status.get()).build();
    }
  }

  private Response okResponse(final String taskid)
  {
    return Response.ok(ImmutableMap.of("task", taskid)).build();
  }

  @GET
  @Path("/worker/setup")
  @Produces("application/json")
  public Response getWorkerSetupData()
  {
    return Response.ok(workerSetupManager.getWorkerSetupData()).build();
  }

  @POST
  @Path("/worker/setup")
  @Consumes("application/json")
  public Response setWorkerSetupData(
      final WorkerSetupData workerSetupData
  )
  {
    if (!workerSetupManager.setWorkerSetupData(workerSetupData)) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }
}
