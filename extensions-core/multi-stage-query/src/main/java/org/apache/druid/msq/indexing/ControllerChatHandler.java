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

package org.apache.druid.msq.indexing;

import org.apache.druid.indexing.common.TaskReport;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.msq.counters.CounterSnapshots;
import org.apache.druid.msq.counters.CounterSnapshotsTree;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.exec.ControllerClient;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.statistics.ClusterByStatisticsSnapshot;
import org.apache.druid.segment.realtime.firehose.ChatHandler;
import org.apache.druid.segment.realtime.firehose.ChatHandlers;
import org.apache.druid.server.security.Action;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

public class ControllerChatHandler implements ChatHandler
{
  private final Controller controller;
  private final MSQControllerTask task;
  private final TaskToolbox toolbox;

  public ControllerChatHandler(TaskToolbox toolbox, Controller controller)
  {
    this.controller = controller;
    this.task = controller.task();
    this.toolbox = toolbox;
  }

  /**
   * Used by subtasks to post {@link ClusterByStatisticsSnapshot} for shuffling stages.
   *
   * See {@link ControllerClient#postKeyStatistics} for the client-side code that calls this API.
   */
  @POST
  @Path("/keyStatistics/{queryId}/{stageNumber}/{workerNumber}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostKeyStatistics(
      final Object keyStatisticsObject,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("workerNumber") final int workerNumber,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    controller.updateStatus(stageNumber, workerNumber, keyStatisticsObject);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * Used by subtasks to post system errors. Note that the errors are organized by taskId, not by query/stage/worker,
   * because system errors are associated with a task rather than a specific query/stage/worker execution context.
   *
   * See {@link ControllerClient#postWorkerError} for the client-side code that calls this API.
   */
  @POST
  @Path("/workerError/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostWorkerError(
      final MSQErrorReport errorReport,
      @PathParam("taskId") final String taskId,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    controller.workerError(errorReport);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * Used by subtasks to post system warnings.
   *
   * See {@link ControllerClient#postWorkerWarning} for the client-side code that calls this API.
   */
  @POST
  @Path("/workerWarning/{taskId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostWorkerWarning(
      final List<MSQErrorReport> errorReport,
      @PathParam("taskId") final String taskId,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    controller.workerWarning(errorReport);
    return Response.status(Response.Status.ACCEPTED).build();
  }


  /**
   * Used by subtasks to post {@link CounterSnapshots} periodically.
   *
   * See {@link ControllerClient#postCounters} for the client-side code that calls this API.
   */
  @POST
  @Path("/counters")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostCounters(
      final CounterSnapshotsTree snapshotsTree,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    controller.updateCounters(snapshotsTree);
    return Response.status(Response.Status.OK).build();
  }

  /**
   * Used by subtasks to post notifications that their results are ready.
   *
   * See {@link ControllerClient#postResultsComplete} for the client-side code that calls this API.
   */
  @POST
  @Path("/resultsComplete/{queryId}/{stageNumber}/{workerNumber}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  public Response httpPostResultsComplete(
      final Object resultObject,
      @PathParam("queryId") final String queryId,
      @PathParam("stageNumber") final int stageNumber,
      @PathParam("workerNumber") final int workerNumber,
      @Context final HttpServletRequest req
  )
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    controller.resultsComplete(queryId, stageNumber, workerNumber, resultObject);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * See {@link ControllerClient#getTaskList} for the client-side code that calls this API.
   */
  @GET
  @Path("/taskList")
  @Produces(MediaType.APPLICATION_JSON)
  public Response httpGetTaskList(@Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());

    return Response.ok(new MSQTaskList(controller.getTaskIds())).build();
  }

  /**
   * See {@link org.apache.druid.indexing.overlord.RemoteTaskRunner#streamTaskReports} for the client-side code that
   * calls this API.
   */
  @GET
  @Path("/liveReports")
  @Produces(MediaType.APPLICATION_JSON)
  public Response httpGetLiveReports(@Context final HttpServletRequest req)
  {
    ChatHandlers.authorizationCheck(req, Action.WRITE, task.getDataSource(), toolbox.getAuthorizerMapper());
    final Map<String, TaskReport> reports = controller.liveReports();
    if (reports == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }
    return Response.ok(reports).build();
  }
}
