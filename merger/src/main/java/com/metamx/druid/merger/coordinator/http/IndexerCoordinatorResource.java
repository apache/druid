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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.actions.TaskActionHolder;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.TaskMasterLifecycle;
import com.metamx.druid.merger.coordinator.TaskStorageQueryAdapter;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupData;
import com.metamx.emitter.service.ServiceEmitter;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

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
  private final JacksonConfigManager configManager;
  private final ObjectMapper jsonMapper;

  private AtomicReference<WorkerSetupData> workerSetupDataRef = null;

  @Inject
  public IndexerCoordinatorResource(
      IndexerCoordinatorConfig config,
      ServiceEmitter emitter,
      TaskMasterLifecycle taskMasterLifecycle,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      JacksonConfigManager configManager,
      ObjectMapper jsonMapper
  ) throws Exception
  {
    this.config = config;
    this.emitter = emitter;
    this.taskMasterLifecycle = taskMasterLifecycle;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.configManager = configManager;
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
    return taskPost(task);
  }

  @POST
  @Path("/task")
  @Consumes("application/json")
  @Produces("application/json")
  public Response taskPost(final Task task)
  {
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
    if (workerSetupDataRef == null) {
      workerSetupDataRef = configManager.watch(WorkerSetupData.CONFIG_KEY, WorkerSetupData.class);
    }

    return Response.ok(workerSetupDataRef.get()).build();
  }

  @POST
  @Path("/worker/setup")
  @Consumes("application/json")
  public Response setWorkerSetupData(
      final WorkerSetupData workerSetupData
  )
  {
    if (!configManager.set(WorkerSetupData.CONFIG_KEY, workerSetupData)) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
    return Response.ok().build();
  }

  @POST
  @Path("/action")
  @Produces("application/json")
  public <T> Response doAction(final TaskActionHolder<T> holder)
  {
    final Map<String, Object> retMap;

    try {
      final T ret = taskMasterLifecycle.getTaskToolbox(holder.getTask())
                                       .getTaskActionClient()
                                       .submit(holder.getAction());
      retMap = Maps.newHashMap();
      retMap.put("result", ret);
    } catch(IOException e) {
      return Response.serverError().build();
    }

    return Response.ok().entity(retMap).build();
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
