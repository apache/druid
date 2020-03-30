/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.overlord.http;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditInfo;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionHolder;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorageQueryAdapter;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.WorkerTaskRunnerQueryAdapter;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.http.security.TaskResourceFilter;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.server.http.HttpMediaType;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
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
import javax.ws.rs.core.Response.Status;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 *
 */
@Path("/druid/indexer/v1")
public class OverlordResource
{
  private static final Logger log = new Logger(OverlordResource.class);

  private final TaskMaster taskMaster;
  private final TaskStorageQueryAdapter taskStorageQueryAdapter;
  private final IndexerMetadataStorageAdapter indexerMetadataStorageAdapter;
  private final TaskLogStreamer taskLogStreamer;
  private final JacksonConfigManager configManager;
  private final AuditManager auditManager;
  private final AuthorizerMapper authorizerMapper;
  private final WorkerTaskRunnerQueryAdapter workerTaskRunnerQueryAdapter;

  private AtomicReference<WorkerBehaviorConfig> workerConfigRef = null;
  private static final List API_TASK_STATES = ImmutableList.of("pending", "waiting", "running", "complete");

  @Inject
  public OverlordResource(
      TaskMaster taskMaster,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      IndexerMetadataStorageAdapter indexerMetadataStorageAdapter,
      TaskLogStreamer taskLogStreamer,
      JacksonConfigManager configManager,
      AuditManager auditManager,
      AuthorizerMapper authorizerMapper,
      WorkerTaskRunnerQueryAdapter workerTaskRunnerQueryAdapter
  )
  {
    this.taskMaster = taskMaster;
    this.taskStorageQueryAdapter = taskStorageQueryAdapter;
    this.indexerMetadataStorageAdapter = indexerMetadataStorageAdapter;
    this.taskLogStreamer = taskLogStreamer;
    this.configManager = configManager;
    this.auditManager = auditManager;
    this.authorizerMapper = authorizerMapper;
    this.workerTaskRunnerQueryAdapter = workerTaskRunnerQueryAdapter;
  }

  /**
   * Warning, magic: {@link org.apache.druid.client.indexing.HttpIndexingServiceClient#runTask} may call this method
   * remotely with {@link ClientTaskQuery} objects, but we deserialize {@link Task} objects. See the comment for {@link
   * ClientTaskQuery} for details.
   */
  @POST
  @Path("/task")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response taskPost(final Task task, @Context final HttpServletRequest req)
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
      throw new ForbiddenException(authResult.getMessage());
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
                             .entity(
                                 ImmutableMap.of(
                                     "error",
                                     StringUtils.format("Task[%s] already exists!", task.getId())
                                 )
                             )
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

  /**
   * This is an unsecured endpoint, defined as such in UNSECURED_PATHS in CliOverlord
   */
  @GET
  @Path("/isLeader")
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
    final TaskPayloadResponse response = new TaskPayloadResponse(
        taskid,
        taskStorageQueryAdapter.getTask(taskid).orNull()
    );

    final Response.Status status = response.getPayload() == null
                                   ? Response.Status.NOT_FOUND
                                   : Response.Status.OK;

    return Response.status(status).entity(response).build();
  }

  @GET
  @Path("/task/{taskid}/status")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response getTaskStatus(@PathParam("taskid") String taskid)
  {
    final TaskInfo<Task, TaskStatus> taskInfo = taskStorageQueryAdapter.getTaskInfo(taskid);
    TaskStatusResponse response = null;

    if (taskInfo != null) {
      if (taskMaster.getTaskRunner().isPresent()) {
        final TaskRunner taskRunner = taskMaster.getTaskRunner().get();
        final TaskRunnerWorkItem workItem = taskRunner
            .getKnownTasks()
            .stream()
            .filter(item -> item.getTaskId().equals(taskid))
            .findAny()
            .orElse(null);
        if (workItem != null) {
          response = new TaskStatusResponse(
              workItem.getTaskId(),
              new TaskStatusPlus(
                  taskInfo.getId(),
                  taskInfo.getTask() == null ? null : taskInfo.getTask().getGroupId(),
                  taskInfo.getTask() == null ? null : taskInfo.getTask().getType(),
                  taskInfo.getCreatedTime(),
                  // Would be nice to include the real queue insertion time, but the
                  // TaskStorage API doesn't yet allow it.
                  DateTimes.EPOCH,
                  taskInfo.getStatus().getStatusCode(),
                  taskRunner.getRunnerTaskState(workItem.getTaskId()),
                  taskInfo.getStatus().getDuration(),
                  workItem.getLocation(),
                  taskInfo.getDataSource(),
                  taskInfo.getStatus().getErrorMsg()
              )
          );
        }
      }

      if (response == null) {
        response = new TaskStatusResponse(
            taskid,
            new TaskStatusPlus(
                taskInfo.getId(),
                taskInfo.getTask() == null ? null : taskInfo.getTask().getGroupId(),
                taskInfo.getTask() == null ? null : taskInfo.getTask().getType(),
                taskInfo.getCreatedTime(),
                // Would be nice to include the real queue insertion time, but the
                // TaskStorage API doesn't yet allow it.
                DateTimes.EPOCH,
                taskInfo.getStatus().getStatusCode(),
                RunnerTaskState.WAITING,
                taskInfo.getStatus().getDuration(),
                taskInfo.getStatus().getLocation() == null ? TaskLocation.unknown() : taskInfo.getStatus().getLocation(),
                taskInfo.getDataSource(),
                taskInfo.getStatus().getErrorMsg()
            )
        );
      }
    } else {
      response = new TaskStatusResponse(taskid, null);
    }

    final Response.Status status = response.getStatus() == null
                                   ? Response.Status.NOT_FOUND
                                   : Response.Status.OK;

    return Response.status(status).entity(response).build();
  }

  @Deprecated
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
            taskQueue.shutdown(taskid, "Shutdown request from user");
            return Response.ok(ImmutableMap.of("task", taskid)).build();
          }
        }
    );
  }

  @POST
  @Path("/datasources/{dataSource}/shutdownAllTasks")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(DatasourceResourceFilter.class)
  public Response shutdownTasksForDataSource(@PathParam("dataSource") final String dataSource)
  {
    return asLeaderWith(
        taskMaster.getTaskQueue(),
        new Function<TaskQueue, Response>()
        {
          @Override
          public Response apply(TaskQueue taskQueue)
          {
            final List<TaskInfo<Task, TaskStatus>> tasks = taskStorageQueryAdapter.getActiveTaskInfo(dataSource);
            if (tasks.isEmpty()) {
              return Response.status(Status.NOT_FOUND).build();
            } else {
              for (final TaskInfo<Task, TaskStatus> task : tasks) {
                taskQueue.shutdown(task.getId(), "Shutdown request from user");
              }
              return Response.ok(ImmutableMap.of("dataSource", dataSource)).build();
            }
          }
        }
    );
  }

  @POST
  @Path("/taskStatus")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getMultipleTaskStatuses(Set<String> taskIds)
  {
    if (taskIds == null || taskIds.size() == 0) {
      return Response.status(Response.Status.BAD_REQUEST).entity("No TaskIds provided.").build();
    }

    Map<String, TaskStatus> result = Maps.newHashMapWithExpectedSize(taskIds.size());
    for (String taskId : taskIds) {
      Optional<TaskStatus> optional = taskStorageQueryAdapter.getStatus(taskId);
      if (optional.isPresent()) {
        result.put(taskId, optional.get());
      }
    }

    return Response.ok().entity(result).build();
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
    final SetResult setResult = configManager.set(
        WorkerBehaviorConfig.CONFIG_KEY,
        workerBehaviorConfig,
        new AuditInfo(author, comment, req.getRemoteAddr())
    );
    if (setResult.isOk()) {
      log.info("Updating Worker configs: %s", workerBehaviorConfig);

      return Response.ok().build();
    } else {
      return Response.status(Response.Status.BAD_REQUEST).build();
    }
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
              retMap = new HashMap<>();
              retMap.put("result", ret);
            }
            catch (Exception e) {
              log.warn(e, "Failed to perform task action");
              return Response.serverError().entity(ImmutableMap.of("error", e.getMessage())).build();
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
    return getTasks("waiting", null, null, null, null, req);
  }

  @GET
  @Path("/pendingTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getPendingTasks(@Context final HttpServletRequest req)
  {
    return getTasks("pending", null, null, null, null, req);
  }

  @GET
  @Path("/runningTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRunningTasks(
      @QueryParam("type") String taskType,
      @Context final HttpServletRequest req
  )
  {
    return getTasks("running", null, null, null, taskType, req);
  }

  @GET
  @Path("/completeTasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCompleteTasks(
      @QueryParam("n") final Integer maxTaskStatuses,
      @Context final HttpServletRequest req
  )
  {
    return getTasks("complete", null, null, maxTaskStatuses, null, req);
  }

  @GET
  @Path("/tasks")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getTasks(
      @QueryParam("state") final String state,
      @QueryParam("datasource") final String dataSource,
      @QueryParam("createdTimeInterval") final String createdTimeInterval,
      @QueryParam("max") final Integer maxCompletedTasks,
      @QueryParam("type") final String type,
      @Context final HttpServletRequest req
  )
  {
    //check for valid state
    if (state != null) {
      if (!API_TASK_STATES.contains(StringUtils.toLowerCase(state))) {
        return Response.status(Status.BAD_REQUEST)
                       .entity(StringUtils.format("Invalid state : %s, valid values are: %s", state, API_TASK_STATES))
                       .build();
      }
    }
    // early authorization check if datasource != null
    // fail fast if user not authorized to access datasource
    if (dataSource != null) {
      final ResourceAction resourceAction = new ResourceAction(
          new Resource(dataSource, ResourceType.DATASOURCE),
          Action.READ
      );
      final Access authResult = AuthorizationUtils.authorizeResourceAction(
          req,
          resourceAction,
          authorizerMapper
      );
      if (!authResult.isAllowed()) {
        throw new WebApplicationException(
            Response.status(Response.Status.FORBIDDEN)
                    .entity(StringUtils.format("Access-Check-Result: %s", authResult.toString()))
                    .build()
        );
      }
    }
    List<TaskStatusPlus> finalTaskList = new ArrayList<>();
    Function<AnyTask, TaskStatusPlus> activeTaskTransformFunc = workItem -> new TaskStatusPlus(
        workItem.getTaskId(),
        workItem.getTaskGroupId(),
        workItem.getTaskType(),
        workItem.getCreatedTime(),
        workItem.getQueueInsertionTime(),
        workItem.getTaskState(),
        workItem.getRunnerTaskState(),
        null,
        workItem.getLocation(),
        workItem.getDataSource(),
        null
    );

    Function<TaskInfo<Task, TaskStatus>, TaskStatusPlus> completeTaskTransformFunc = taskInfo -> new TaskStatusPlus(
        taskInfo.getId(),
        taskInfo.getTask() == null ? null : taskInfo.getTask().getGroupId(),
        taskInfo.getTask() == null ? null : taskInfo.getTask().getType(),
        taskInfo.getCreatedTime(),
        // Would be nice to include the real queue insertion time, but the
        // TaskStorage API doesn't yet allow it.
        DateTimes.EPOCH,
        taskInfo.getStatus().getStatusCode(),
        RunnerTaskState.NONE,
        taskInfo.getStatus().getDuration(),
        taskInfo.getStatus().getLocation() == null ? TaskLocation.unknown() : taskInfo.getStatus().getLocation(),
        taskInfo.getDataSource(),
        taskInfo.getStatus().getErrorMsg()
    );

    //checking for complete tasks first to avoid querying active tasks if user only wants complete tasks
    if (state == null || "complete".equals(StringUtils.toLowerCase(state))) {
      Duration createdTimeDuration = null;
      if (createdTimeInterval != null) {
        final Interval theInterval = Intervals.of(StringUtils.replace(createdTimeInterval, "_", "/"));
        createdTimeDuration = theInterval.toDuration();
      }
      final List<TaskInfo<Task, TaskStatus>> taskInfoList =
          taskStorageQueryAdapter.getCompletedTaskInfoByCreatedTimeDuration(maxCompletedTasks, createdTimeDuration, dataSource);
      final List<TaskStatusPlus> completedTasks = taskInfoList.stream()
                                                              .map(completeTaskTransformFunc::apply)
                                                              .collect(Collectors.toList());
      finalTaskList.addAll(completedTasks);
    }

    final List<TaskInfo<Task, TaskStatus>> allActiveTaskInfo;
    final List<AnyTask> allActiveTasks = new ArrayList<>();
    if (state == null || !"complete".equals(StringUtils.toLowerCase(state))) {
      allActiveTaskInfo = taskStorageQueryAdapter.getActiveTaskInfo(dataSource);
      for (final TaskInfo<Task, TaskStatus> task : allActiveTaskInfo) {
        allActiveTasks.add(
            new AnyTask(
                task.getId(),
                task.getTask() == null ? null : task.getTask().getGroupId(),
                task.getTask() == null ? null : task.getTask().getType(),
                SettableFuture.create(),
                task.getDataSource(),
                null,
                null,
                task.getCreatedTime(),
                DateTimes.EPOCH,
                TaskLocation.unknown()
            ));
      }
    }
    if (state == null || "waiting".equals(StringUtils.toLowerCase(state))) {
      final List<AnyTask> waitingWorkItems = filterActiveTasks(RunnerTaskState.WAITING, allActiveTasks);
      List<TaskStatusPlus> transformedWaitingList = waitingWorkItems.stream()
                                                                    .map(activeTaskTransformFunc::apply)
                                                                    .collect(Collectors.toList());
      finalTaskList.addAll(transformedWaitingList);
    }
    if (state == null || "pending".equals(StringUtils.toLowerCase(state))) {
      final List<AnyTask> pendingWorkItems = filterActiveTasks(RunnerTaskState.PENDING, allActiveTasks);
      List<TaskStatusPlus> transformedPendingList = pendingWorkItems.stream()
                                                                    .map(activeTaskTransformFunc::apply)
                                                                    .collect(Collectors.toList());
      finalTaskList.addAll(transformedPendingList);
    }
    if (state == null || "running".equals(StringUtils.toLowerCase(state))) {
      final List<AnyTask> runningWorkItems = filterActiveTasks(RunnerTaskState.RUNNING, allActiveTasks);
      List<TaskStatusPlus> transformedRunningList = runningWorkItems.stream()
                                                                    .map(activeTaskTransformFunc::apply)
                                                                    .collect(Collectors.toList());
      finalTaskList.addAll(transformedRunningList);
    }
    final List<TaskStatusPlus> authorizedList = securedTaskStatusPlus(
        finalTaskList,
        dataSource,
        type,
        req
    );
    return Response.ok(authorizedList).build();
  }

  @DELETE
  @Path("/pendingSegments/{dataSource}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response killPendingSegments(
      @PathParam("dataSource") String dataSource,
      @QueryParam("interval") String deleteIntervalString,
      @Context HttpServletRequest request
  )
  {
    final Interval deleteInterval = Intervals.of(deleteIntervalString);
    // check auth for dataSource
    final Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        request,
        ImmutableList.of(
            new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.READ),
            new ResourceAction(new Resource(dataSource, ResourceType.DATASOURCE), Action.WRITE)
        ),
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.getMessage());
    }

    if (taskMaster.isLeader()) {
      final int numDeleted = indexerMetadataStorageAdapter.deletePendingSegments(dataSource, deleteInterval);
      return Response.ok().entity(ImmutableMap.of("numDeleted", numDeleted)).build();
    } else {
      return Response.status(Status.SERVICE_UNAVAILABLE).build();
    }
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
                  taskRunner.getClass().getName()
              );
              return Response.serverError()
                             .entity(ImmutableMap.of("error", "Task Runner does not support worker listing"))
                             .build();
            }
          }
        }
    );
  }

  @POST
  @Path("/worker/{host}/enable")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response enableWorker(@PathParam("host") final String host)
  {
    return changeWorkerStatus(host, WorkerTaskRunner.ActionType.ENABLE);
  }

  @POST
  @Path("/worker/{host}/disable")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response disableWorker(@PathParam("host") final String host)
  {
    return changeWorkerStatus(host, WorkerTaskRunner.ActionType.DISABLE);
  }

  private Response changeWorkerStatus(String host, WorkerTaskRunner.ActionType action)
  {
    try {
      if (WorkerTaskRunner.ActionType.DISABLE.equals(action)) {
        workerTaskRunnerQueryAdapter.disableWorker(host);
        return Response.ok(ImmutableMap.of(host, "disabled")).build();
      } else if (WorkerTaskRunner.ActionType.ENABLE.equals(action)) {
        workerTaskRunnerQueryAdapter.enableWorker(host);
        return Response.ok(ImmutableMap.of(host, "enabled")).build();
      } else {
        return Response.serverError()
                       .entity(ImmutableMap.of("error", "Worker does not support " + action + " action!"))
                       .build();
      }
    }
    catch (Exception e) {
      log.error(e, "Error in posting [%s] action to [%s]", action, host);
      return Response.serverError()
                     .entity(ImmutableMap.of("error", e.getMessage()))
                     .build();
    }
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
  @Produces(HttpMediaType.TEXT_PLAIN_UTF8)
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

  @GET
  @Path("/task/{taskid}/reports")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(TaskResourceFilter.class)
  public Response doGetReports(
      @PathParam("taskid") final String taskid
  )
  {
    try {
      final Optional<ByteSource> stream = taskLogStreamer.streamTaskReports(taskid);
      if (stream.isPresent()) {
        return Response.ok(stream.get().openStream()).build();
      } else {
        return Response.status(Response.Status.NOT_FOUND)
                       .entity(
                           "No task reports were found for this task. "
                           + "The task may not exist, or it may not have completed yet."
                       )
                       .build();
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to stream task reports for task %s", taskid);
      return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
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

  private List<AnyTask> filterActiveTasks(
      RunnerTaskState state,
      List<AnyTask> allTasks
  )
  {
    //divide active tasks into 3 lists : running, pending, waiting
    Optional<TaskRunner> taskRunnerOpt = taskMaster.getTaskRunner();
    if (!taskRunnerOpt.isPresent()) {
      throw new WebApplicationException(
          Response.serverError().entity("No task runner found").build()
      );
    }
    TaskRunner runner = taskRunnerOpt.get();
    // the order of tasks below is waiting, pending, running to prevent
    // skipping a task, it's the order in which tasks will change state
    // if they do while this is code is executing, so a task might be
    // counted twice but never skipped
    if (RunnerTaskState.WAITING.equals(state)) {
      Collection<? extends TaskRunnerWorkItem> runnersKnownTasks = runner.getKnownTasks();
      Set<String> runnerKnownTaskIds = runnersKnownTasks
          .stream()
          .map(TaskRunnerWorkItem::getTaskId)
          .collect(Collectors.toSet());
      final List<AnyTask> waitingTasks = new ArrayList<>();
      for (TaskRunnerWorkItem task : allTasks) {
        if (!runnerKnownTaskIds.contains(task.getTaskId())) {
          waitingTasks.add(((AnyTask) task).withTaskState(
              TaskState.RUNNING,
              RunnerTaskState.WAITING,
              task.getCreatedTime(),
              task.getQueueInsertionTime(),
              task.getLocation()
          ));
        }
      }
      return waitingTasks;
    }

    if (RunnerTaskState.PENDING.equals(state)) {
      Collection<? extends TaskRunnerWorkItem> knownPendingTasks = runner.getPendingTasks();
      Set<String> pendingTaskIds = knownPendingTasks
          .stream()
          .map(TaskRunnerWorkItem::getTaskId)
          .collect(Collectors.toSet());
      Map<String, TaskRunnerWorkItem> workItemIdMap = knownPendingTasks
          .stream()
          .collect(Collectors.toMap(
              TaskRunnerWorkItem::getTaskId,
              java.util.function.Function.identity(),
              (previousWorkItem, newWorkItem) -> newWorkItem
          ));
      final List<AnyTask> pendingTasks = new ArrayList<>();
      for (TaskRunnerWorkItem task : allTasks) {
        if (pendingTaskIds.contains(task.getTaskId())) {
          pendingTasks.add(((AnyTask) task).withTaskState(
              TaskState.RUNNING,
              RunnerTaskState.PENDING,
              workItemIdMap.get(task.getTaskId()).getCreatedTime(),
              workItemIdMap.get(task.getTaskId()).getQueueInsertionTime(),
              workItemIdMap.get(task.getTaskId()).getLocation()
          ));
        }
      }
      return pendingTasks;
    }

    if (RunnerTaskState.RUNNING.equals(state)) {
      Collection<? extends TaskRunnerWorkItem> knownRunningTasks = runner.getRunningTasks();
      Set<String> runningTaskIds = knownRunningTasks
          .stream()
          .map(TaskRunnerWorkItem::getTaskId)
          .collect(Collectors.toSet());
      Map<String, TaskRunnerWorkItem> workItemIdMap = knownRunningTasks
          .stream()
          .collect(Collectors.toMap(
              TaskRunnerWorkItem::getTaskId,
              java.util.function.Function.identity(),
              (previousWorkItem, newWorkItem) -> newWorkItem
          ));
      final List<AnyTask> runningTasks = new ArrayList<>();
      for (TaskRunnerWorkItem task : allTasks) {
        if (runningTaskIds.contains(task.getTaskId())) {
          runningTasks.add(((AnyTask) task).withTaskState(
              TaskState.RUNNING,
              RunnerTaskState.RUNNING,
              workItemIdMap.get(task.getTaskId()).getCreatedTime(),
              workItemIdMap.get(task.getTaskId()).getQueueInsertionTime(),
              workItemIdMap.get(task.getTaskId()).getLocation()
          ));
        }
      }
      return runningTasks;
    }
    return allTasks;
  }

  private List<TaskStatusPlus> securedTaskStatusPlus(
      List<TaskStatusPlus> collectionToFilter,
      @Nullable String dataSource,
      @Nullable String type,
      HttpServletRequest req
  )
  {
    Function<TaskStatusPlus, Iterable<ResourceAction>> raGenerator = taskStatusPlus -> {
      final String taskId = taskStatusPlus.getId();
      final String taskDatasource = taskStatusPlus.getDataSource();
      if (taskDatasource == null) {
        throw new WebApplicationException(
            Response.serverError().entity(
                StringUtils.format("No task information found for task with id: [%s]", taskId)
            ).build()
        );
      }
      return Collections.singletonList(
          new ResourceAction(new Resource(taskDatasource, ResourceType.DATASOURCE), Action.READ)
      );
    };
    List<TaskStatusPlus> optionalTypeFilteredList = collectionToFilter;
    if (type != null) {
      optionalTypeFilteredList = collectionToFilter
          .stream()
          .filter(task -> type.equals(task.getType()))
          .collect(Collectors.toList());
    }
    if (dataSource != null) {
      //skip auth check here, as it's already done in getTasks
      return optionalTypeFilteredList;
    }
    return Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            optionalTypeFilteredList,
            raGenerator,
            authorizerMapper
        )
    );
  }

  private static class AnyTask extends TaskRunnerWorkItem
  {
    private final String taskGroupId;
    private final String taskType;
    private final String dataSource;
    private final TaskState taskState;
    private final RunnerTaskState runnerTaskState;
    private final DateTime createdTime;
    private final DateTime queueInsertionTime;
    private final TaskLocation taskLocation;

    AnyTask(
        String taskId,
        String taskGroupId,
        String taskType,
        ListenableFuture<TaskStatus> result,
        String dataSource,
        TaskState state,
        RunnerTaskState runnerState,
        DateTime createdTime,
        DateTime queueInsertionTime,
        TaskLocation taskLocation
    )
    {
      super(taskId, result, DateTimes.EPOCH, DateTimes.EPOCH);
      this.taskGroupId = taskGroupId;
      this.taskType = taskType;
      this.dataSource = dataSource;
      this.taskState = state;
      this.runnerTaskState = runnerState;
      this.createdTime = createdTime;
      this.queueInsertionTime = queueInsertionTime;
      this.taskLocation = taskLocation;
    }

    @Override
    public TaskLocation getLocation()
    {
      return taskLocation;
    }

    @Override
    public String getTaskType()
    {
      return taskType;
    }

    @Override
    public String getDataSource()
    {
      return dataSource;
    }

    public String getTaskGroupId()
    {
      return taskGroupId;
    }

    public TaskState getTaskState()
    {
      return taskState;
    }

    public RunnerTaskState getRunnerTaskState()
    {
      return runnerTaskState;
    }

    @Override
    public DateTime getCreatedTime()
    {
      return createdTime;
    }

    @Override
    public DateTime getQueueInsertionTime()
    {
      return queueInsertionTime;
    }

    public AnyTask withTaskState(
        TaskState newTaskState,
        RunnerTaskState runnerState,
        DateTime createdTime,
        DateTime queueInsertionTime,
        TaskLocation taskLocation
    )
    {
      return new AnyTask(
          getTaskId(),
          getTaskGroupId(),
          getTaskType(),
          getResult(),
          getDataSource(),
          newTaskState,
          runnerState,
          createdTime,
          queueInsertionTime,
          taskLocation
      );
    }
  }
}
