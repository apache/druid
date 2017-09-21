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

package io.druid.indexing.overlord.http;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.audit.AuditEntry;
import io.druid.audit.AuditInfo;
import io.druid.audit.AuditManager;
import io.druid.common.config.JacksonConfigManager;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionHolder;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.WorkerTaskRunner;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.http.security.TaskResourceFilter;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.metadata.EntryExistsException;
import io.druid.server.http.security.ConfigResourceFilter;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.AuthorizationUtils;
import io.druid.server.security.ForbiddenException;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceAction;
import io.druid.server.security.ResourceType;
import io.druid.tasklogs.TaskLogStreamer;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
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
  private final AuditManager auditManager;
  private final AuthorizerMapper authorizerMapper;

  private AtomicReference<WorkerBehaviorConfig> workerConfigRef = null;

  @Inject
  public OverlordResource(
      TaskMaster taskMaster,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      TaskLogStreamer taskLogStreamer,
      JacksonConfigManager configManager,
      AuditManager auditManager,
      AuthorizerMapper authorizerMapper
  ) throws Exception
  {
    this.taskMaster = taskMaster;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.taskLogStreamer = taskLogStreamer;
    this.configManager = configManager;
    this.auditManager = auditManager;
    this.authorizerMapper = authorizerMapper;
  }

  @POST
  @Path("/task")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response taskPost(
      final Task task,
      @Context final HttpServletRequest req
  )
  {
    final String dataSource = task.getDataSource();
    final ResourceAction resourceAction = new ResourceAction(
        new Resource(dataSource, ResourceType.DATASOURCE),
        Action.WRITE
    );

    Access authResult = AuthorizationUtils.authorizeResourceAction(
        req,
        resourceAction,
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

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
            catch (EntryExistsException e) {
              return Response.status(Response.Status.BAD_REQUEST)
                             .entity(ImmutableMap.of("error", StringUtils.format("Task[%s] already exists!", task.getId())))
                             .build();
            }
          }
        }
    );
  }

  @GET
  @Path("/leader")
  @ResourceFilters(StateResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLeader()
  {
    return Response.ok(taskMaster.getCurrentLeader()).build();
  }

  @GET
  @Path("/isLeader")
  @ResourceFilters(StateResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response isLeader()
  {
    final boolean leading = taskMaster.isLeader();
    final Map<String, Boolean> response = ImmutableMap.of("leader", leading);
    if (leading) {
      return Response.ok(response).build();
    } else {
      return Response.status(Response.Status.NOT_FOUND).entity(response).build();
    }
  }

  @GET
  @Path("/task/{taskid}")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response getTaskPayload(@PathParam("taskid") String taskid)
  {
    return optionalTaskResponse(taskid, "payload", taskStorageQueryAdapter.getTask(taskid));
  }

  @GET
  @Path("/task/{taskid}/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response getTaskStatus(@PathParam("taskid") String taskid)
  {
    return optionalTaskResponse(taskid, "status", taskStorageQueryAdapter.getStatus(taskid));
  }

  @GET
  @Path("/task/{taskid}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response getTaskSegments(@PathParam("taskid") String taskid)
  {
    final Set<DataSegment> segments = taskStorageQueryAdapter.getInsertedSegments(taskid);
    return Response.ok().entity(segments).build();
  }

  @POST
  @Path("/task/{taskid}/shutdown")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
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
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getWorkerConfig()
  {
    if (workerConfigRef == null) {
      workerConfigRef = configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class);
    }

    return Response.ok(workerConfigRef.get()).build();
  }

  // default value is used for backwards compatibility
  @POST
  @Path("/worker")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response setWorkerConfig(
      final WorkerBehaviorConfig workerBehaviorConfig,
      @HeaderParam(AuditManager.X_DRUID_AUTHOR) @DefaultValue("") final String author,
      @HeaderParam(AuditManager.X_DRUID_COMMENT) @DefaultValue("") final String comment,
      @Context final HttpServletRequest req
  )
  {
    if (!configManager.set(
        WorkerBehaviorConfig.CONFIG_KEY,
        workerBehaviorConfig,
        new AuditInfo(author, comment, req.getRemoteAddr())
    )) {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }

    log.info("Updating Worker configs: %s", workerBehaviorConfig);

    return Response.ok().build();
  }

  @GET
  @Path("/worker/history")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getWorkerConfigHistory(
      @QueryParam("interval") final String interval,
      @QueryParam("count") final Integer count
  )
  {
    Interval theInterval = interval == null ? null : Intervals.of(interval);
    if (theInterval == null && count != null) {
      try {
        List<AuditEntry> workerEntryList = auditManager.fetchAuditHistory(
            WorkerBehaviorConfig.CONFIG_KEY,
            WorkerBehaviorConfig.CONFIG_KEY,
            count
        );
        return Response.ok(workerEntryList).build();
      }
      catch (IllegalArgumentException e) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                       .build();
      }
    }
    List<AuditEntry> workerEntryList = auditManager.fetchAuditHistory(
        WorkerBehaviorConfig.CONFIG_KEY,
        WorkerBehaviorConfig.CONFIG_KEY,
        theInterval
    );
    return Response.ok(workerEntryList).build();
  }

  @POST
  @Path("/action")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response doAction(final TaskActionHolder holder)
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
              final Object ret = taskActionClient.submit(holder.getAction());
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
  @Produces(MediaType.APPLICATION_JSON)
  public Response getWaitingTasks(@Context final HttpServletRequest req)
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            // A bit roundabout, but works as a way of figuring out what tasks haven't been handed
            // off to the runner yet:
            final List<Task> allActiveTasks = taskStorageQueryAdapter.getActiveTasks();
            Function<Task, Iterable<ResourceAction>> raGenerator = task -> {
              return Lists.newArrayList(
                  new ResourceAction(
                      new Resource(task.getDataSource(), ResourceType.DATASOURCE),
                      Action.READ
                  )
              );
            };

            final List<Task> activeTasks = Lists.newArrayList(
                AuthorizationUtils.filterAuthorizedResources(
                    req,
                    allActiveTasks,
                    raGenerator,
                    authorizerMapper
                )
            );

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
                        DateTimes.EPOCH,
                        DateTimes.EPOCH
                    )
                    {
                      @Override
                      public TaskLocation getLocation()
                      {
                        return TaskLocation.unknown();
                      }
                    }
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
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPendingTasks(@Context final HttpServletRequest req)
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return securedTaskRunnerWorkItem(taskRunner.getPendingTasks(), req);
          }
        }
    );
  }

  @GET
  @Path("/runningTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks(@Context final HttpServletRequest req)
  {
    return workItemsResponse(
        new Function<TaskRunner, Collection<? extends TaskRunnerWorkItem>>()
        {
          @Override
          public Collection<? extends TaskRunnerWorkItem> apply(TaskRunner taskRunner)
          {
            return securedTaskRunnerWorkItem(taskRunner.getRunningTasks(), req);
          }
        }
    );
  }

  @GET
  @Path("/completeTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteTasks(@Context final HttpServletRequest req)
  {
    Function<TaskStatus, Iterable<ResourceAction>> raGenerator = taskStatus -> {
      final String taskId = taskStatus.getId();
      final Optional<Task> optionalTask = taskStorageQueryAdapter.getTask(taskId);
      if (!optionalTask.isPresent()) {
        throw new WebApplicationException(
            Response.serverError().entity(
                StringUtils.format("No task information found for task with id: [%s]", taskId)
            ).build()
        );
      }

      return Lists.newArrayList(
          new ResourceAction(
              new Resource(optionalTask.get().getDataSource(), ResourceType.DATASOURCE),
              Action.READ
          )
      );
    };

    final List<TaskStatus> recentlyFinishedTasks = Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            taskStorageQueryAdapter.getRecentlyFinishedTaskStatuses(),
            raGenerator,
            authorizerMapper
        )
    );

    final List<TaskResponseObject> completeTasks = Lists.transform(
        recentlyFinishedTasks,
        new Function<TaskStatus, TaskResponseObject>()
        {
          @Override
          public TaskResponseObject apply(TaskStatus taskStatus)
          {
            // Would be nice to include the real created date, but the TaskStorage API doesn't yet allow it.
            return new TaskResponseObject(
                taskStatus.getId(),
                DateTimes.EPOCH,
                DateTimes.EPOCH,
                Optional.of(taskStatus),
                TaskLocation.unknown()
            );
          }
        }
    );
    return Response.ok(completeTasks).build();
  }

  @GET
  @Path("/workers")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getWorkers()
  {
    return asLeaderWith(
        taskMaster.getTaskRunner(),
        new Function<TaskRunner, Response>()
        {
          @Override
          public Response apply(TaskRunner taskRunner)
          {
            if (taskRunner instanceof WorkerTaskRunner) {
              return Response.ok(((WorkerTaskRunner) taskRunner).getWorkers()).build();
            } else {
              log.debug(
                  "Task runner [%s] of type [%s] does not support listing workers",
                  taskRunner,
                  taskRunner.getClass().getCanonicalName()
              );
              return Response.serverError()
                             .entity(ImmutableMap.of("error", "Task Runner does not support worker listing"))
                             .build();
            }
          }
        }
    );
  }

  @GET
  @Path("/scaling")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getScalingState()
  {
    // Don't use asLeaderWith, since we want to return 200 instead of 503 when missing an autoscaler.
    final Optional<ScalingStats> rms = taskMaster.getScalingStats();
    if (rms.isPresent()) {
      return Response.ok(rms.get()).build();
    } else {
      return Response.ok().build();
    }
  }

  @GET
  @Path("/task/{taskid}/log")
  @Produces("text/plain")
  @ResourceFilters(TaskResourceFilter.class)
  public Response doGetLog(
      @PathParam("taskid") final String taskid,
      @QueryParam("offset") @DefaultValue("0") final long offset
  )
  {
    try {
      final Optional<ByteSource> stream = taskLogStreamer.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return Response.ok(stream.get().openStream()).build();
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
                            Optional.<TaskStatus>absent(),
                            workItem.getLocation()
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

  private Collection<? extends TaskRunnerWorkItem> securedTaskRunnerWorkItem(
      Collection<? extends TaskRunnerWorkItem> collectionToFilter,
      HttpServletRequest req
  )
  {
    Function<TaskRunnerWorkItem, Iterable<ResourceAction>> raGenerator = taskRunnerWorkItem -> {
      final String taskId = taskRunnerWorkItem.getTaskId();
      final Optional<Task> optionalTask = taskStorageQueryAdapter.getTask(taskId);
      if (!optionalTask.isPresent()) {
        throw new WebApplicationException(
            Response.serverError().entity(
                StringUtils.format("No task information found for task with id: [%s]", taskId)
            ).build()
        );
      }

      return Lists.newArrayList(
          new ResourceAction(
              new Resource(optionalTask.get().getDataSource(), ResourceType.DATASOURCE),
              Action.READ
          )
      );
    };

    return Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            collectionToFilter,
            raGenerator,
            authorizerMapper
        )
    );
  }

  static class TaskResponseObject
  {
    private final String id;
    private final DateTime createdTime;
    private final DateTime queueInsertionTime;
    private final Optional<TaskStatus> status;
    private final TaskLocation location;

    private TaskResponseObject(
        String id,
        DateTime createdTime,
        DateTime queueInsertionTime,
        Optional<TaskStatus> status,
        TaskLocation location
    )
    {
      this.id = id;
      this.createdTime = createdTime;
      this.queueInsertionTime = queueInsertionTime;
      this.status = status;
      this.location = location;
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
        if (status.get().isComplete()) {
          data.put("duration", status.get().getDuration());
        }
      }
      if (location != null) {
        data.put("location", location);
      }
      return data;
    }
  }
}
