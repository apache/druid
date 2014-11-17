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

package io.druid.indexing.overlord.http;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.common.config.JacksonConfigManager;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionHolder;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskExistsException;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.autoscaling.ResourceManagementScheduler;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerSetupData;
import io.druid.tasklogs.TaskLogStreamer;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 */
@Path("/druid/indexer/v1")
public class OverlordResource
{
  private static final Logger log = new Logger(OverlordResource.class);

  private final TaskMaster taskMaster;
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final TaskLogStreamer taskLogStreamer;
  private final JacksonConfigManager configManager;

  private AtomicReference<WorkerBehaviorConfig> workerConfigRef = null;

  @Deprecated
  private AtomicReference<WorkerSetupData> workerSetupDataRef = null;

  @Inject
  public OverlordResource(
      TaskMaster taskMaster,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogStreamer taskLogStreamer,
      JacksonConfigManager configManager
  ) throws Exception
  {
    this.taskMaster = taskMaster;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.taskLogStreamer = taskLogStreamer;
    this.configManager = configManager;
  }

  @POST
  @Path("/merge")
  @Consumes("application/json")
  @Produces("application/json")
  @Deprecated
  public Response doMerge(final Task task)
  {
    // legacy endpoint
    return doIndex(task);
  }

  @POST
  @Path("/index")
  @Consumes("application/json")
  @Produces("application/json")
  @Deprecated
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
        taskMaster.getTaskQueue(),
        new Function<TaskQueue, Response>()
        {
          @Override
          public Response apply(TaskQueue taskQueue)
          {
            try {
              taskQueue.add(task);
              return Response.ok(ImmutableMap.of("task", task.getId())).build();
            }
            catch (TaskExistsException e) {
              return Response.status(Response.Status.BAD_REQUEST)
                             .entity(ImmutableMap.of("error", String.format("Task[%s] already exists!", task.getId())))
                             .build();
            }
          }
        }
    );
  }

  @GET
  @Path("/task/{taskid}")
  @Produces("application/json")
  public Response getTaskPayload(@PathParam("taskid") String taskid)
  {
    return optionalTaskResponse(taskid, "payload", taskStorageQueryAdapter.getTask(taskid));
  }

  @GET
  @Path("/task/{taskid}/status")
  @Produces("application/json")
  public Response getTaskStatus(@PathParam("taskid") String taskid)
  {
    return optionalTaskResponse(taskid, "status", taskStorageQueryAdapter.getStatus(taskid));
  }

  @GET
  @Path("/task/{taskid}/segments")
  @Produces("application/json")
  public Response getTaskSegments(@PathParam("taskid") String taskid)
  {
    final Set<DataSegment> segments = taskStorageQueryAdapter.getInsertedSegments(taskid);
    return Response.ok().entity(segments).build();
  }

  @POST
  @Path("/task/{taskid}/shutdown")
  @Produces("application/json")
  public Response doShutdown(@PathParam("taskid") final String taskid)
  {
    return asLeaderWith(
        taskMaster.getTaskQueue(),
        new Function<TaskQueue, Response>()
        {
          @Override
          public Response apply(TaskQueue taskQueue)
          {
            taskQueue.shutdown(taskid);
            return Response.ok(ImmutableMap.of("task", taskid)).build();
          }
        }
    );
  }

  @GET
  @Path("/worker")
  @Produces("application/json")
  public Response getWorkerConfig()
  {
    if (workerConfigRef == null) {
      workerConfigRef = configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class);
    }

    return Response.ok(workerConfigRef.get()).build();
  }

  @POST
  @Path("/worker")
  @Consumes("application/json")
  public Response setWorkerConfig(
      final WorkerBehaviorConfig workerBehaviorConfig
  )
  {
    if (!configManager.set(WorkerBehaviorConfig.CONFIG_KEY, workerBehaviorConfig)) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    log.info("Updating Worker configs: %s", workerBehaviorConfig);

    return Response.ok().build();
  }


  @Deprecated
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

  @Deprecated
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
        taskMaster.getTaskActionClient(holder.getTask()),
        new Function<TaskActionClient, Response>()
        {
          @Override
          public Response apply(TaskActionClient taskActionClient)
          {
            final Map<String, Object> retMap;

            // It would be great to verify that this worker is actually supposed to be running the task before
            // actually doing the action.  Some ideas for how that could be done would be using some sort of attempt_id
            // or token that gets passed around.

            try {
              final T ret = taskActionClient.submit(holder.getAction());
              retMap = Maps.newHashMap();
              retMap.put("result", ret);
            }
            catch (IOException e) {
              log.warn(e, "Failed to perform task action");
              return Response.serverError().build();
            }

            return Response.ok().entity(retMap).build();
          }
        }
    );
  }

  @GET
  @Path("/waitingTasks")
  @Produces("application/json")
  public Response getWaitingTasks()
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            // A bit roundabout, but works as a way of figuring out what tasks haven't been handed
            // off to the runner yet:
            final List<Task> activeTasks = taskStorageQueryAdapter.getActiveTasks();
            final Set<String> runnersKnownTasks = Sets.newHashSet(
                Iterables.transform(
                    taskRunner.getKnownTasks(),
                    new Function<TaskRunnerWorkItem, String>()
                    {
                      @Override
                      public String apply(final TaskRunnerWorkItem workItem)
                      {
                        return workItem.getTaskId();
                      }
                    }
                )
            );
            final List<TaskRunnerWorkItem> waitingTasks = Lists.newArrayList();
            for (final Task task : activeTasks) {
              if (!runnersKnownTasks.contains(task.getId())) {
                waitingTasks.add(
                    // Would be nice to include the real created date, but the TaskStorage API doesn't yet allow it.
                    new TaskRunnerWorkItem(
                        task.getId(),
                        SettableFuture.<TaskStatus>create(),
                        new DateTime(0),
                        new DateTime(0)
                    )
                );
              }
            }
            return waitingTasks;
          }
        }
    );
  }

  @GET
  @Path("/pendingTasks")
  @Produces("application/json")
  public Response getPendingTasks()
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return taskRunner.getPendingTasks();
          }
        }
    );
  }

  @GET
  @Path("/runningTasks")
  @Produces("application/json")
  public Response getRunningTasks()
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return taskRunner.getRunningTasks();
          }
        }
    );
  }

  @GET
  @Path("/completeTasks")
  @Produces("application/json")
  public Response getCompleteTasks()
  {
    final List<TaskResponseObject> completeTasks = Lists.transform(
        taskStorageQueryAdapter.getRecentlyFinishedTaskStatuses(),
        new Function<TaskStatus, TaskResponseObject>()
        {
          @Override
          public TaskResponseObject apply(TaskStatus taskStatus)
          {
            // Would be nice to include the real created date, but the TaskStorage API doesn't yet allow it.
            return new TaskResponseObject(
                taskStatus.getId(),
                new DateTime(0),
                new DateTime(0),
                Optional.of(taskStatus)
            );
          }
        }
    );
    return Response.ok(completeTasks).build();
  }

  @GET
  @Path("/workers")
  @Produces("application/json")
  public Response getWorkers()
  {
    return asLeaderWith(
        taskMaster.getTaskRunner(),
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
    // Don't use asLeaderWith, since we want to return 200 instead of 503 when missing an autoscaler.
    final Optional<ResourceManagementScheduler> rms = taskMaster.getResourceManagementScheduler();
    if (rms.isPresent()) {
      return Response.ok(rms.get().getStats()).build();
    } else {
      return Response.ok().build();
    }
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
      final Optional<InputSupplier<InputStream>> stream = taskLogStreamer.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return Response.ok(stream.get().getInput()).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(
                           "No log was found for this task. "
                           + "The task may not exist, or it may not have begun running yet."
                       )
                       .build();
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to stream log for task %s", taskid);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
    }
  }

  private Response workItemsResponse(final Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>> fn)
  {
    return asLeaderWith(
        taskMaster.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            return Response.ok(
                Lists.transform(
                    Lists.newArrayList(fn.apply(taskRunner)),
                    new Function<TaskRunnerWorkItem, TaskResponseObject>()
                    {
                      @Override
                      public TaskResponseObject apply(TaskRunnerWorkItem workItem)
                      {
                        return new TaskResponseObject(
                            workItem.getTaskId(),
                            workItem.getCreatedTime(),
                            workItem.getQueueInsertionTime(),
                            Optional.<TaskStatus>absent()
                        );
                      }
                    }
                )
            ).build();
          }
        }
    );
  }

  private <T> Response optionalTaskResponse(String taskid, String objectType, Optional<T> x)
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

  private <T> Response asLeaderWith(Optional<T> x, Function<T, Response> f)
  {
    if (x.isPresent()) {
      return f.apply(x.get());
    } else {
      // Encourage client to try again soon, when we'll likely have a redirect set up
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
  }

  private static class TaskResponseObject
  {
    private final String id;
    private final DateTime createdTime;
    private final DateTime queueInsertionTime;
    private final Optional<TaskStatus> status;

    private TaskResponseObject(
        String id,
        DateTime createdTime,
        DateTime queueInsertionTime,
        Optional<TaskStatus> status
    )
    {
      this.id = id;
      this.createdTime = createdTime;
      this.queueInsertionTime = queueInsertionTime;
      this.status = status;
    }

    public String getId()
    {
      return id;
    }

    public DateTime getCreatedTime()
    {
      return createdTime;
    }

    public DateTime getQueueInsertionTime()
    {
      return queueInsertionTime;
    }

    public Optional<TaskStatus> getStatus()
    {
      return status;
    }

    @JsonValue
    public Map<String, Object> toJson()
    {
      final Map<String, Object> data = Maps.newLinkedHashMap();
      data.put("id", id);
      if (createdTime.getMillis() > 0) {
        data.put("createdTime", createdTime);
      }
      if (queueInsertionTime.getMillis() > 0) {
        data.put("queueInsertionTime", queueInsertionTime);
      }
      if (status.isPresent()) {
        data.put("statusCode", status.get().getStatusCode().toString());
      }
      return data;
    }
  }
}
