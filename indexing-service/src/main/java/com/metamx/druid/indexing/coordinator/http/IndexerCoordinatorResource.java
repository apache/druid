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

package com.metamx.druid.indexing.coordinator.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.config.JacksonConfigManager;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.actions.TaskActionClient;
import com.metamx.druid.indexing.common.actions.TaskActionHolder;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.common.tasklogs.TaskLogProvider;
import com.metamx.druid.indexing.coordinator.TaskMasterLifecycle;
import com.metamx.druid.indexing.coordinator.TaskQueue;
import com.metamx.druid.indexing.coordinator.TaskRunner;
import com.metamx.druid.indexing.coordinator.TaskRunnerWorkItem;
import com.metamx.druid.indexing.coordinator.TaskStorageQueryAdapter;
import com.metamx.druid.indexing.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.indexing.coordinator.scaling.ResourceManagementScheduler;
import com.metamx.druid.indexing.coordinator.setup.WorkerSetupData;
import com.metamx.emitter.service.ServiceEmitter;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@Path("/druid/indexer/v1")
public class IndexerCoordinatorResource
{
  private static final Logger log = new Logger(IndexerCoordinatorResource.class);

  private static Function<TaskRunnerWorkItem, Map<String, Object>> simplifyTaskFn =
      new Function<TaskRunnerWorkItem, Map<String, Object>>()
      {
        @Override
        public Map<String, Object> apply(TaskRunnerWorkItem input)
        {
          return new ImmutableMap.Builder<String, Object>()
              .put("id", input.getTask().getId())
              .put("dataSource", input.getTask().getDataSource())
              .put("interval",
                   input.getTask().getImplicitLockInterval().get() == null
                   ? ""
                   : input.getTask().getImplicitLockInterval().get()
              )
              .put("nodeType", input.getTask().getNodeType() == null ? "" : input.getTask().getNodeType())
              .put("createdTime", input.getCreatedTime())
              .put("queueInsertionTime", input.getQueueInsertionTime())
              .build();
        }
      };

  private final IndexerCoordinatorConfig config;
  private final ServiceEmitter emitter;
  private final TaskMasterLifecycle taskMasterLifecycle;
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final TaskLogProvider taskLogProvider;
  private final JacksonConfigManager configManager;
  private final ObjectMapper jsonMapper;

  private AtomicReference<WorkerSetupData> workerSetupDataRef = null;

  @Inject
  public IndexerCoordinatorResource(
      IndexerCoordinatorConfig config,
      ServiceEmitter emitter,
      TaskMasterLifecycle taskMasterLifecycle,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogProvider taskLogProvider,
      JacksonConfigManager configManager,
      ObjectMapper jsonMapper
  ) throws Exception
  {
    this.config = config;
    this.emitter = emitter;
    this.taskMasterLifecycle = taskMasterLifecycle;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.taskLogProvider = taskLogProvider;
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
    return asLeaderWith(
        taskMasterLifecycle.getTaskQueue(),
        new Function<TaskQueue, Response>()
        {
          @Override
          public Response apply(TaskQueue taskQueue)
          {
            taskQueue.add(task);
            return Response.ok(ImmutableMap.of("task", task.getId())).build();
          }
        }
    );
  }

  @GET
  @Path("/task/{taskid}/status")
  @Produces("application/json")
  public Response getTaskStatus(@PathParam("taskid") String taskid)
  {
    return optionalTaskResponse(taskid, "status", taskStorageQueryAdapter.getSameGroupMergedStatus(taskid));
  }

  @GET
  @Path("/task/{taskid}/segments")
  @Produces("application/json")
  public Response getTaskSegments(@PathParam("taskid") String taskid)
  {
    final Set<DataSegment> segments = taskStorageQueryAdapter.getSameGroupNewSegments(taskid);
    return Response.ok().entity(segments).build();
  }

  @POST
  @Path("/task/{taskid}/shutdown")
  @Produces("application/json")
  public Response doShutdown(@PathParam("taskid") final String taskid)
  {
    return asLeaderWith(
        taskMasterLifecycle.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            taskRunner.shutdown(taskid);
            return Response.ok(ImmutableMap.of("task", taskid)).build();
          }
        }
    );
  }

  // Legacy endpoint
  // TODO remove
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

    log.info("Updating Worker Setup configs: %s", workerSetupData);

    return Response.ok().build();
  }

  @POST
  @Path("/action")
  @Produces("application/json")
  public <T> Response doAction(final TaskActionHolder<T> holder)
  {
    return asLeaderWith(
        taskMasterLifecycle.getTaskActionClient(holder.getTask()),
        new Function<TaskActionClient, Response>()
        {
          @Override
          public Response apply(TaskActionClient taskActionClient)
          {
            final Map<String, Object> retMap;

            // It would be great to verify that this worker is actually supposed to be running the task before
            // actually doing the task.  Some ideas for how that could be done would be using some sort of attempt_id
            // or token that gets passed around.

            try {
              final T ret = taskActionClient.submit(holder.getAction());
              retMap = Maps.newHashMap();
              retMap.put("result", ret);
            }
            catch (IOException e) {
              return Response.serverError().build();
            }

            return Response.ok().entity(retMap).build();
          }
        }
    );
  }

  @GET
  @Path("/pendingTasks")
  @Produces("application/json")
  public Response getPendingTasks(
      @QueryParam("full") String full
  )
  {
    if (full != null) {
      return asLeaderWith(
          taskMasterLifecycle.getTaskRunner(),
          new Function<TaskRunner, Response>()
          {
            @Override
            public Response apply(TaskRunner taskRunner)
            {
              return Response.ok(taskRunner.getPendingTasks()).build();
            }
          }
      );
    }

    return asLeaderWith(
        taskMasterLifecycle.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            return Response.ok(
                Collections2.transform(
                    taskRunner.getPendingTasks(),
                    simplifyTaskFn
                )
            ).build();
          }
        }
    );
  }

  @GET
  @Path("/runningTasks")
  @Produces("application/json")
  public Response getRunningTasks(
      @QueryParam("full") String full
  )
  {
    if (full != null) {
      return asLeaderWith(
          taskMasterLifecycle.getTaskRunner(),
          new Function<TaskRunner, Response>()
          {
            @Override
            public Response apply(TaskRunner taskRunner)
            {
              return Response.ok(taskRunner.getRunningTasks()).build();
            }
          }
      );
    }

    return asLeaderWith(
        taskMasterLifecycle.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            return Response.ok(
                Collections2.transform(
                    taskRunner.getRunningTasks(),
                    simplifyTaskFn
                )
            ).build();
          }
        }
    );
  }

  @GET
  @Path("/workers")
  @Produces("application/json")
  public Response getWorkers()
  {
    return asLeaderWith(
        taskMasterLifecycle.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            return Response.ok(taskRunner.getWorkers()).build();
          }
        }
    );
  }

  @GET
  @Path("/scaling")
  @Produces("application/json")
  public Response getScalingState()
  {
    return asLeaderWith(
        taskMasterLifecycle.getResourceManagementScheduler(),
        new Function<ResourceManagementScheduler, Response>()
        {
          @Override
          public Response apply(ResourceManagementScheduler resourceManagementScheduler)
          {
            return Response.ok(resourceManagementScheduler.getStats()).build();
          }
        }
    );
  }

  @GET
  @Path("/task/{taskid}/log")
  @Produces("text/plain")
  public Response doGetLog(
      @PathParam("taskid") final String taskid,
      @QueryParam("offset") @DefaultValue("0") final long offset
  )
  {
    try {
      final Optional<InputSupplier<InputStream>> stream = taskLogProvider.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return Response.ok(stream.get().getInput()).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND).build();
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to stream log for task %s", taskid);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  public <T> Response optionalTaskResponse(String taskid, String objectType, Optional<T> x)
  {
    final Map<String, Object> results = Maps.newHashMap();
    results.put("task", taskid);
    if (x.isPresent()) {
      results.put(objectType, x.get());
      return Response.status(Response.Status.OK).entity(results).build();
    } else {
      return Response.status(Response.Status.NOT_FOUND).entity(results).build();
    }
  }

  public <T> Response asLeaderWith(Optional<T> x, Function<T, Response> f)
  {
    if (x.isPresent()) {
      return f.apply(x.get());
    } else {
      // Encourage client to try again soon, when we'll likely have a redirect set up
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }
}
