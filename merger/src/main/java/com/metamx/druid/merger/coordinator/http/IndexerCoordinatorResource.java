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
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.actions.TaskAction;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.TaskMasterLifecycle;
import com.metamx.druid.merger.coordinator.TaskStorageQueryAdapter;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupData;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import com.metamx.emitter.service.ServiceEmitter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.util.Map;
import java.util.Set;

/**
 */
@Path("/mmx/merger/v1")
public class IndexerCoordinatorResource
{
  private static final Logger log = new Logger(IndexerCoordinatorResource.class);

  private final IndexerCoordinatorConfig config;
  private final ServiceEmitter emitter;
  private final TaskMasterLifecycle taskMasterLifecycle;
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final WorkerSetupManager workerSetupManager;
  private final ObjectMapper jsonMapper;

  @Inject
  public IndexerCoordinatorResource(
      IndexerCoordinatorConfig config,
      ServiceEmitter emitter,
      TaskMasterLifecycle taskMasterLifecycle,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      WorkerSetupManager workerSetupManager,
      ObjectMapper jsonMapper
  ) throws Exception
  {
    this.config = config;
    this.emitter = emitter;
    this.taskMasterLifecycle = taskMasterLifecycle;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.workerSetupManager = workerSetupManager;
    this.jsonMapper = jsonMapper;
  }

  @POST
  @Path("/merge")
  @Consumes("application/json")
  @Produces("application/json")
  public Response doMerge(final Task task)
  {
    // legacy endpoint
    return doIndex(task);
  }

  @POST
  @Path("/index")
  @Consumes("application/json")
  @Produces("application/json")
  public Response doIndex(final Task task)
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

    taskMasterLifecycle.getTaskQueue().add(task);
    return Response.ok(ImmutableMap.of("task", task.getId())).build();
  }

  @GET
  @Path("/task/{taskid}/status")
  @Produces("application/json")
  public Response getTaskStatus(@PathParam("taskid") String taskid)
  {
    final Optional<TaskStatus> status = taskStorageQueryAdapter.getSameGroupMergedStatus(taskid);
    if (!status.isPresent()) {
      return Response.status(Response.Status.NOT_FOUND).build();
    } else {
      return Response.ok().entity(status.get()).build();
    }
  }

  @GET
  @Path("/task/{taskid}/segments")
  @Produces("application/json")
  public Response getTaskSegments(@PathParam("taskid") String taskid)
  {
    final Set<DataSegment> segments = taskStorageQueryAdapter.getSameGroupNewSegments(taskid);
    return Response.ok().entity(segments).build();
  }

  // Legacy endpoint
  // TODO Remove
  @Deprecated
  @GET
  @Path("/status/{taskid}")
  @Produces("application/json")
  public Response getLegacyStatus(@PathParam("taskid") String taskid)
  {
    final Optional<TaskStatus> status = taskStorageQueryAdapter.getSameGroupMergedStatus(taskid);
    final Set<DataSegment> segments = taskStorageQueryAdapter.getSameGroupNewSegments(taskid);

    final Map<String, Object> ret = jsonMapper.convertValue(
        status, new TypeReference<Map<String, Object>>()
    {
    }
    );
    ret.put("segments", segments);

    return Response.ok().entity(ret).build();
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

  @POST
  @Path("/action")
  @Produces("application/json")
  public <T> Response doAction(final TaskAction<T> action)
  {
    final T ret = taskMasterLifecycle.getTaskToolbox().getTaskActionClient().submit(action);
    return Response.ok().entity(ret).build();
  }

  @GET
  @Path("/pendingTasks")
  @Produces("application/json")
  public Response getPendingTasks()
  {
    if (taskMasterLifecycle.getTaskRunner() == null) {
      return Response.noContent().build();
    }
    return Response.ok(taskMasterLifecycle.getTaskRunner().getPendingTasks()).build();
  }

  @GET
  @Path("/runningTasks")
  @Produces("application/json")
  public Response getRunningTasks()
  {
    if (taskMasterLifecycle.getTaskRunner() == null) {
      return Response.noContent().build();
    }
    return Response.ok(taskMasterLifecycle.getTaskRunner().getRunningTasks()).build();
  }

  @GET
  @Path("/workers")
  @Produces("application/json")
  public Response getWorkers()
  {
    if (taskMasterLifecycle.getTaskRunner() == null) {
      return Response.noContent().build();
    }
    return Response.ok(taskMasterLifecycle.getTaskRunner().getWorkers()).build();
  }

  @GET
  @Path("/scaling")
  @Produces("application/json")
  public Response getScalingState()
  {
    if (taskMasterLifecycle.getResourceManagementScheduler() == null) {
      return Response.noContent().build();
    }
    return Response.ok(taskMasterLifecycle.getResourceManagementScheduler().getStats()).build();
  }
}
