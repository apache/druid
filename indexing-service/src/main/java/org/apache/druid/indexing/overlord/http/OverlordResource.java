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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.audit.AuditEntry;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.client.indexing.ClientTaskQuery;
import org.apache.druid.common.config.ConfigManager.SetResult;
import org.apache.druid.common.config.JacksonConfigManager;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskIdentifier;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionHolder;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.ImmutableWorkerInfo;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskQueue;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorageQueryAdapter;
import org.apache.druid.indexing.overlord.WorkerTaskRunner;
import org.apache.druid.indexing.overlord.WorkerTaskRunnerQueryAdapter;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.http.security.TaskResourceFilter;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.metadata.TaskLookup;
import org.apache.druid.metadata.TaskLookup.ActiveTaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.metadata.TaskLookup.TaskLookupType;
import org.apache.druid.server.http.HttpMediaType;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.apache.druid.server.http.security.DatasourceResourceFilter;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.tasklogs.TaskLogStreamer;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
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
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
  private final ProvisioningStrategy provisioningStrategy;

  private final AuthConfig authConfig;

  private AtomicReference<WorkerBehaviorConfig> workerConfigRef = null;
  private static final List<String> API_TASK_STATES = ImmutableList.of("pending", "waiting", "running", "complete");
  private static final Set<String> AUDITED_TASK_TYPES
      = ImmutableSet.of("index", "index_parallel", "compact", "index_hadoop", "kill");

  private enum TaskStateLookup
  {
    ALL,
    WAITING,
    PENDING,
    RUNNING,
    COMPLETE;

    private static TaskStateLookup fromString(@Nullable String state)
    {
      if (state == null) {
        return ALL;
      } else {
        return TaskStateLookup.valueOf(StringUtils.toUpperCase(state));
      }
    }
  }

  @Inject
  public OverlordResource(
      TaskMaster taskMaster,
      TaskStorageQueryAdapter taskStorageQueryAdapter,
      IndexerMetadataStorageAdapter indexerMetadataStorageAdapter,
      TaskLogStreamer taskLogStreamer,
      JacksonConfigManager configManager,
      AuditManager auditManager,
      AuthorizerMapper authorizerMapper,
      WorkerTaskRunnerQueryAdapter workerTaskRunnerQueryAdapter,
      ProvisioningStrategy provisioningStrategy,
      AuthConfig authConfig
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
    this.provisioningStrategy = provisioningStrategy;
    this.authConfig = authConfig;
  }

  /**
   * Warning, magic: {@link org.apache.druid.rpc.indexing.OverlordClient#runTask} may call this method
   * remotely with {@link ClientTaskQuery} objects, but we deserialize {@link Task} objects. See the comment for {@link
   * ClientTaskQuery} for details.
   */
  @POST
  @Path("/task")
  @Consumes(MediaType.APPLICATION_JSON)
  @Produces(MediaType.APPLICATION_JSON)
  public Response taskPost(
      final Task task,
      @Context final HttpServletRequest req
  )
  {
    final Set<ResourceAction> resourceActions;
    try {
      resourceActions = getNeededResourceActionsForTask(task);
    }
    catch (UOE e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.of("error", e.getMessage()))
                     .build();
    }

    Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        req,
        resourceActions,
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.getMessage());
    }

    return asLeaderWith(
        taskMaster.getTaskQueue(),
        taskQueue -> {
          try {
            taskQueue.add(task);

            if (AUDITED_TASK_TYPES.contains(task.getType())) {
              auditManager.doAudit(
                  AuditEntry.builder()
                            .key(task.getDataSource())
                            .type("task")
                            .request(AuthorizationUtils.buildRequestInfo("overlord", req))
                            .payload(new TaskIdentifier(task.getId(), task.getGroupId(), task.getType()))
                            .auditInfo(AuthorizationUtils.buildAuditInfo(req))
                            .build()
              );
            }

            return Response.ok(ImmutableMap.of("task", task.getId())).build();
          }
          catch (DruidException e) {
            return Response
                .status(e.getStatusCode())
                .entity(new ErrorResponse(e))
                .build();
          }
          catch (org.apache.druid.common.exception.DruidException e) {
            return Response.status(e.getResponseCode())
                           .entity(ImmutableMap.of("error", e.getMessage()))
                           .build();
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

  @Deprecated
  @POST
  @Path("/lockedIntervals")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getDatasourceLockedIntervals(Map<String, Integer> minTaskPriority)
  {
    if (minTaskPriority == null || minTaskPriority.isEmpty()) {
      return Response.status(Status.BAD_REQUEST).entity("No Datasource provided").build();
    }

    // Build the response
    return Response.ok(taskStorageQueryAdapter.getLockedIntervals(minTaskPriority)).build();
  }

  @POST
  @Path("/lockedIntervals/v2")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(StateResourceFilter.class)
  public Response getDatasourceLockedIntervalsV2(List<LockFilterPolicy> lockFilterPolicies)
  {
    if (lockFilterPolicies == null || lockFilterPolicies.isEmpty()) {
      return Response.status(Status.BAD_REQUEST).entity("No filter provided").build();
    }

    // Build the response
    return Response.ok(taskStorageQueryAdapter.getLockedIntervals(lockFilterPolicies)).build();
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
                taskInfo.getStatus().getLocation() == null
                ? TaskLocation.unknown()
                : taskInfo.getStatus().getLocation(),
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

    final Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    Map<String, TaskStatus> result = Maps.newHashMapWithExpectedSize(taskIds.size());
    for (String taskId : taskIds) {
      final Optional<TaskStatus> optional;
      if (taskQueue.isPresent()) {
        optional = taskQueue.get().getTaskStatus(taskId);
      } else {
        optional = taskStorageQueryAdapter.getStatus(taskId);
      }
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

  /**
   * Gets the total worker capacity of varies states of the cluster.
   */
  @GET
  @Path("/totalWorkerCapacity")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response getTotalWorkerCapacity()
  {
    // Calculate current cluster capacity
    Optional<TaskRunner> taskRunnerOptional = taskMaster.getTaskRunner();
    if (!taskRunnerOptional.isPresent()) {
      // Cannot serve call as not leader
      return Response.status(Response.Status.SERVICE_UNAVAILABLE).build();
    }
    TaskRunner taskRunner = taskRunnerOptional.get();
    Collection<ImmutableWorkerInfo> workers = taskRunner instanceof WorkerTaskRunner ?
        ((WorkerTaskRunner) taskRunner).getWorkers() : ImmutableList.of();

    int currentCapacity = taskRunner.getTotalCapacity();
    int usedCapacity = taskRunner.getUsedCapacity();
    // Calculate maximum capacity with auto scale
    int maximumCapacity;
    if (workerConfigRef == null) {
      workerConfigRef = configManager.watch(WorkerBehaviorConfig.CONFIG_KEY, WorkerBehaviorConfig.class);
    }
    WorkerBehaviorConfig workerBehaviorConfig = workerConfigRef.get();
    if (workerBehaviorConfig == null) {
      // Auto scale not setup
      log.debug("Cannot calculate maximum worker capacity as worker behavior config is not configured");
      maximumCapacity = -1;
    } else if (workerBehaviorConfig instanceof DefaultWorkerBehaviorConfig) {
      DefaultWorkerBehaviorConfig defaultWorkerBehaviorConfig = (DefaultWorkerBehaviorConfig) workerBehaviorConfig;
      if (defaultWorkerBehaviorConfig.getAutoScaler() == null) {
        // Auto scale not setup
        log.debug("Cannot calculate maximum worker capacity as auto scaler not configured");
        maximumCapacity = -1;
      } else {
        int maxWorker = defaultWorkerBehaviorConfig.getAutoScaler().getMaxNumWorkers();
        int expectedWorkerCapacity = provisioningStrategy.getExpectedWorkerCapacity(workers);
        maximumCapacity = expectedWorkerCapacity == -1 ? -1 : maxWorker * expectedWorkerCapacity;
      }
    } else {
      // Auto scale is not using DefaultWorkerBehaviorConfig
      log.debug(
          "Cannot calculate maximum worker capacity as WorkerBehaviorConfig [%s] of type [%s] does not support getting max capacity",
          workerBehaviorConfig,
          workerBehaviorConfig.getClass().getSimpleName()
      );
      maximumCapacity = -1;
    }
    return Response.ok(new TotalWorkerCapacityResponse(currentCapacity, maximumCapacity, usedCapacity)).build();
  }

  // default value is used for backwards compatibility
  @POST
  @Path("/worker")
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response setWorkerConfig(
      final WorkerBehaviorConfig workerBehaviorConfig,
      @Context final HttpServletRequest req
  )
  {
    final SetResult setResult = configManager.set(
        WorkerBehaviorConfig.CONFIG_KEY,
        workerBehaviorConfig,
        AuthorizationUtils.buildAuditInfo(req)
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
                       .type(MediaType.TEXT_PLAIN)
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
                    .type(MediaType.TEXT_PLAIN)
                    .entity(StringUtils.format("Access-Check-Result: %s", authResult.toString()))
                    .build()
        );
      }
    }

    return asLeaderWith(
        taskMaster.getTaskRunner(),
        taskRunner -> {
          final List<TaskStatusPlus> authorizedList = securedTaskStatusPlus(
              getTaskStatusPlusList(
                  taskRunner,
                  TaskStateLookup.fromString(state),
                  dataSource,
                  createdTimeInterval,
                  maxCompletedTasks,
                  type
              ),
              dataSource,
              req
          );
          return Response.ok(authorizedList).build();
        }
    );
  }

  private List<TaskStatusPlus> getTaskStatusPlusList(
      TaskRunner taskRunner,
      TaskStateLookup state,
      @Nullable String dataSource,
      @Nullable String createdTimeInterval,
      @Nullable Integer maxCompletedTasks,
      @Nullable String type
  )
  {
    final Duration createdTimeDuration;
    if (createdTimeInterval != null) {
      final Interval theInterval = Intervals.of(StringUtils.replace(createdTimeInterval, "_", "/"));
      createdTimeDuration = theInterval.toDuration();
    } else {
      createdTimeDuration = null;
    }

    // Ideally, snapshotting in taskStorage and taskRunner should be done atomically,
    // but there is no way to do it today.
    // Instead, we first gets a snapshot from taskStorage and then one from taskRunner.
    // This way, we can use the snapshot from taskStorage as the source of truth for the set of tasks to process
    // and use the snapshot from taskRunner as a reference for potential task state updates happened
    // after the first snapshotting.
    Stream<TaskStatusPlus> taskStatusPlusStream = getTaskStatusPlusList(
        state,
        dataSource,
        createdTimeDuration,
        maxCompletedTasks,
        type
    );
    final Map<String, ? extends TaskRunnerWorkItem> runnerWorkItems = getTaskRunnerWorkItems(
        taskRunner,
        state,
        dataSource,
        type
    );

    if (state == TaskStateLookup.PENDING || state == TaskStateLookup.RUNNING) {
      // We are interested in only those tasks which are in taskRunner.
      taskStatusPlusStream = taskStatusPlusStream
          .filter(statusPlus -> runnerWorkItems.containsKey(statusPlus.getId()));
    }
    final List<TaskStatusPlus> taskStatusPlusList = taskStatusPlusStream.collect(Collectors.toList());

    // Separate complete and active tasks from taskStorage.
    // Note that taskStorage can return only either complete tasks or active tasks per TaskLookupType.
    final List<TaskStatusPlus> completeTaskStatusPlusList = new ArrayList<>();
    final List<TaskStatusPlus> activeTaskStatusPlusList = new ArrayList<>();
    for (TaskStatusPlus statusPlus : taskStatusPlusList) {
      if (statusPlus.getStatusCode().isComplete()) {
        completeTaskStatusPlusList.add(statusPlus);
      } else {
        activeTaskStatusPlusList.add(statusPlus);
      }
    }

    final List<TaskStatusPlus> taskStatuses = new ArrayList<>(completeTaskStatusPlusList);

    activeTaskStatusPlusList.forEach(statusPlus -> {
      final TaskRunnerWorkItem runnerWorkItem = runnerWorkItems.get(statusPlus.getId());
      if (runnerWorkItem == null) {
        // a task is assumed to be a waiting task if it exists in taskStorage but not in taskRunner.
        if (state == TaskStateLookup.WAITING || state == TaskStateLookup.ALL) {
          taskStatuses.add(statusPlus);
        }
      } else {
        if (state == TaskStateLookup.PENDING || state == TaskStateLookup.RUNNING || state == TaskStateLookup.ALL) {
          taskStatuses.add(
              new TaskStatusPlus(
                  statusPlus.getId(),
                  statusPlus.getGroupId(),
                  statusPlus.getType(),
                  runnerWorkItem.getCreatedTime(),
                  runnerWorkItem.getQueueInsertionTime(),
                  statusPlus.getStatusCode(),
                  taskRunner.getRunnerTaskState(statusPlus.getId()), // this is racy for remoteTaskRunner
                  statusPlus.getDuration(),
                  runnerWorkItem.getLocation(), // location in taskInfo is only updated after the task is done.
                  statusPlus.getDataSource(),
                  statusPlus.getErrorMsg()
              )
          );
        }
      }
    });

    return taskStatuses;
  }

  private Stream<TaskStatusPlus> getTaskStatusPlusList(
      TaskStateLookup state,
      @Nullable String dataSource,
      Duration createdTimeDuration,
      @Nullable Integer maxCompletedTasks,
      @Nullable String type
  )
  {
    final Map<TaskLookupType, TaskLookup> taskLookups;
    switch (state) {
      case ALL:
        taskLookups = ImmutableMap.of(
            TaskLookupType.ACTIVE,
            ActiveTaskLookup.getInstance(),
            TaskLookupType.COMPLETE,
            CompleteTaskLookup.of(maxCompletedTasks, createdTimeDuration)
        );
        break;
      case COMPLETE:
        taskLookups = ImmutableMap.of(
            TaskLookupType.COMPLETE,
            CompleteTaskLookup.of(maxCompletedTasks, createdTimeDuration)
        );
        break;
      case WAITING:
      case PENDING:
      case RUNNING:
        taskLookups = ImmutableMap.of(
            TaskLookupType.ACTIVE,
            ActiveTaskLookup.getInstance()
        );
        break;
      default:
        throw new IAE("Unknown state: [%s]", state);
    }

    final Stream<TaskStatusPlus> taskStatusPlusStream = taskStorageQueryAdapter.getTaskStatusPlusList(
        taskLookups,
        dataSource
    ).stream();
    if (type != null) {
      return taskStatusPlusStream.filter(
          statusPlus -> type.equals(statusPlus == null ? null : statusPlus.getType())
      );
    } else {
      return taskStatusPlusStream;
    }
  }

  private Map<String, ? extends TaskRunnerWorkItem> getTaskRunnerWorkItems(
      TaskRunner taskRunner,
      TaskStateLookup state,
      @Nullable String dataSource,
      @Nullable String type
  )
  {
    Stream<? extends TaskRunnerWorkItem> runnerWorkItemsStream;
    switch (state) {
      case ALL:
      case WAITING:
        // waiting tasks can be found by (all tasks in taskStorage - all tasks in taskRunner)
        runnerWorkItemsStream = taskRunner.getKnownTasks().stream();
        break;
      case PENDING:
        runnerWorkItemsStream = taskRunner.getPendingTasks().stream();
        break;
      case RUNNING:
        runnerWorkItemsStream = taskRunner.getRunningTasks().stream();
        break;
      case COMPLETE:
        runnerWorkItemsStream = Stream.empty();
        break;
      default:
        throw new IAE("Unknown state: [%s]", state);
    }
    if (dataSource != null) {
      runnerWorkItemsStream = runnerWorkItemsStream.filter(item -> dataSource.equals(item.getDataSource()));
    }
    if (type != null) {
      runnerWorkItemsStream = runnerWorkItemsStream.filter(item -> type.equals(item.getTaskType()));
    }
    return runnerWorkItemsStream
        .collect(Collectors.toMap(TaskRunnerWorkItem::getTaskId, item -> item));
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
      try {
        final int numDeleted = indexerMetadataStorageAdapter.deletePendingSegments(dataSource, deleteInterval);
        return Response.ok().entity(ImmutableMap.of("numDeleted", numDeleted)).build();
      }
      catch (DruidException e) {
        return Response.status(e.getStatusCode())
                       .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                       .build();
      }
      catch (Exception e) {
        log.warn(e, "Failed to delete pending segments for datasource[%s] and interval[%s].", dataSource, deleteInterval);
        return Response.status(Status.INTERNAL_SERVER_ERROR)
                       .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                       .build();
      }
    } else {
      return Response.status(Status.SERVICE_UNAVAILABLE)
                     .entity(ImmutableMap.of("error", "overlord is not the leader or not initialized yet"))
                     .build();
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
      final Optional<InputStream> stream = taskLogStreamer.streamTaskLog(taskid, offset);
      if (stream.isPresent()) {
        return Response.ok(stream.get()).build();
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
      final Optional<InputStream> stream = taskLogStreamer.streamTaskReports(taskid);
      if (stream.isPresent()) {
        return Response.ok(stream.get()).build();
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

  @VisibleForTesting
  Set<ResourceAction> getNeededResourceActionsForTask(Task task) throws UOE
  {
    final Set<ResourceAction> resourceActions = new HashSet<>();
    java.util.Optional<Resource> destinationResource = task.getDestinationResource();
    destinationResource.ifPresent(resource -> resourceActions.add(new ResourceAction(resource, Action.WRITE)));
    if (authConfig.isEnableInputSourceSecurity()) {
      resourceActions.addAll(task.getInputSourceResources());
    }
    return resourceActions;
  }

  private List<TaskStatusPlus> securedTaskStatusPlus(
      List<TaskStatusPlus> collectionToFilter,
      @Nullable String dataSource,
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
    if (dataSource != null) {
      //skip auth check here, as it's already done in getTasks
      return collectionToFilter;
    }
    return Lists.newArrayList(
        AuthorizationUtils.filterAuthorizedResources(
            req,
            collectionToFilter,
            raGenerator,
            authorizerMapper
        )
    );
  }
}
