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

package org.apache.druid.msq.dart.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.inject.Inject;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.messages.server.MessageRelayResource;
import org.apache.druid.messages.server.Outbox;
import org.apache.druid.msq.dart.Dart;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.dart.worker.DartWorkerRunner;
import org.apache.druid.msq.kernel.WorkOrder;
import org.apache.druid.msq.rpc.MSQResourceUtils;
import org.apache.druid.msq.rpc.ResourcePermissionMapper;
import org.apache.druid.msq.rpc.WorkerResource;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.jetty.ServiceUnavailableException;
import org.apache.druid.server.security.AuthorizerMapper;

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

/**
 * Subclass of {@link WorkerResource} suitable for usage on a Historical.
 *
 * Note that this is not the same resource as used by {@link org.apache.druid.msq.indexing.MSQWorkerTask}.
 * For that, see {@link org.apache.druid.msq.indexing.client.WorkerChatHandler}.
 */
@LazySingleton
@Path(DartWorkerResource.PATH + '/')
public class DartWorkerResource
{
  /**
   * Root of worker APIs.
   */
  public static final String PATH = "/druid/dart-worker";

  /**
   * Header containing the controller host:port, from {@link DruidNode#getHostAndPortToUse()}.
   */
  public static final String HEADER_CONTROLLER_HOST = "X-Dart-Controller-Host";

  private final DartWorkerRunner workerRunner;
  private final ResourcePermissionMapper permissionMapper;
  private final AuthorizerMapper authorizerMapper;
  private final MessageRelayResource<ControllerMessage> messageRelayResource;

  @Inject
  public DartWorkerResource(
      final DartWorkerRunner workerRunner,
      @Dart final ResourcePermissionMapper permissionMapper,
      @Smile final ObjectMapper smileMapper,
      final Outbox<ControllerMessage> outbox,
      final AuthorizerMapper authorizerMapper
  )
  {
    this.workerRunner = workerRunner;
    this.permissionMapper = permissionMapper;
    this.authorizerMapper = authorizerMapper;
    this.messageRelayResource = new MessageRelayResource<>(
        outbox,
        smileMapper,
        ControllerMessage.class
    );
  }

  /**
   * API for retrieving all currently-running queries.
   */
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/workers")
  public GetWorkersResponse httpGetWorkers(@Context final HttpServletRequest req)
  {
    MSQResourceUtils.authorizeAdminRequest(permissionMapper, authorizerMapper, req);
    return workerRunner.getWorkersResponse();
  }

  /**
   * Like {@link WorkerResource#httpPostWorkOrder(WorkOrder, HttpServletRequest)}, but implicitly starts a worker
   * when the work order is posted. Shadows {@link WorkerResource#httpPostWorkOrder(WorkOrder, HttpServletRequest)}.
   */
  @POST
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Path("/workers/{queryId}/workOrder")
  public Response httpPostWorkOrder(
      final WorkOrder workOrder,
      @PathParam("queryId") final String queryId,
      @Context final HttpServletRequest req
  )
  {
    MSQResourceUtils.authorizeAdminRequest(permissionMapper, authorizerMapper, req);
    final String controllerHost = req.getHeader(HEADER_CONTROLLER_HOST);
    if (controllerHost == null) {
      throw DruidException.forPersona(DruidException.Persona.DEVELOPER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("Missing controllerId[%s]", HEADER_CONTROLLER_HOST);
    }

    workerRunner.startWorker(queryId, controllerHost, workOrder.getWorkerContext())
                .postWorkOrder(workOrder);

    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * Stops a worker. Returns immediately; does not wait for the worker to actually finish.
   */
  @POST
  @Path("/workers/{queryId}/stop")
  public Response httpPostStopWorker(
      @PathParam("queryId") final String queryId,
      @Context final HttpServletRequest req
  )
  {
    MSQResourceUtils.authorizeAdminRequest(permissionMapper, authorizerMapper, req);
    workerRunner.stopWorker(queryId);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  /**
   * Handles all {@link WorkerResource} calls, except {@link WorkerResource#httpPostWorkOrder}, which is handled
   * by {@link #httpPostWorkOrder(WorkOrder, String, HttpServletRequest)}.
   */
  @Path("/workers/{queryId}")
  public Object httpCallWorkerResource(
      @PathParam("queryId") final String queryId,
      @Context final HttpServletRequest req
  )
  {
    final WorkerResource resource = workerRunner.getWorkerResource(queryId);

    if (resource != null) {
      return resource;
    } else {
      // Return HTTP 503 (Service Unavailable) so worker -> worker clients can retry. When workers are first starting
      // up and contacting each other, worker A may contact worker B before worker B has started up. In the future, it
      // would be better to do an async wait, with some timeout, for the worker to show up before returning 503.
      // That way a retry wouldn't be necessary.
      MSQResourceUtils.authorizeAdminRequest(permissionMapper, authorizerMapper, req);
      throw new ServiceUnavailableException(StringUtils.format("No worker running for query[%s]", queryId));
    }
  }

  @Path("/relay")
  public Object httpCallMessageRelayServer(@Context final HttpServletRequest req)
  {
    MSQResourceUtils.authorizeAdminRequest(permissionMapper, authorizerMapper, req);
    return messageRelayResource;
  }
}
