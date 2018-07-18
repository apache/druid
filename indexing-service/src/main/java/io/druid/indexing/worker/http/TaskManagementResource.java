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

package io.druid.indexing.worker.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.hrtr.WorkerHolder;
import io.druid.indexing.worker.WorkerHistoryItem;
import io.druid.indexing.worker.WorkerTaskMonitor;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.server.coordination.ChangeRequestHistory;
import io.druid.server.coordination.ChangeRequestsSnapshot;
import io.druid.server.http.SegmentListerResource;
import io.druid.server.http.security.StateResourceFilter;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Endpoints used by Overlord to Manage tasks on this Middle Manager.
 */
@Path("/druid-internal/v1/worker/")
@ResourceFilters(StateResourceFilter.class)
public class TaskManagementResource
{
  protected static final EmittingLogger log = new EmittingLogger(SegmentListerResource.class);

  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  private final WorkerTaskMonitor workerTaskMonitor;

  @Inject
  public TaskManagementResource(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      WorkerTaskMonitor workerTaskMonitor
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.workerTaskMonitor = workerTaskMonitor;
  }

  /**
   * This endpoint is used by HttpRemoteTaskRunner to keep an up-to-date state of the worker wrt to the tasks it is
   * running, completed etc and other metadata such as its enabled/disabled status.
   *
   * Here is how, this is used.
   *
   * (1) Client sends first request /druid/internal/v1/worker?counter=-1&timeout=<timeout>
   * Server responds with current list of running/completed tasks and metadata. And, a <counter,hash> pair.
   *
   * (2) Client sends subsequent requests /druid/internal/v1/worker?counter=<counter>&hash=<hash>&timeout=<timeout>
   * Where <counter,hash> values are used from the last response. Server responds with changes since then.
   *
   * This endpoint makes the client wait till either there is some update or given timeout elapses.
   *
   * So, clients keep on sending next request immediately after receiving the response in order to keep the state of
   * this server up-to-date.
   *
   * @param counter counter received in last response.
   * @param hash hash received in last response.
   * @param timeout after which response is sent even if there are no new segment updates.
   * @param req
   * @throws IOException
   */
  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public void getWorkerState(
      @QueryParam("counter") long counter,
      @QueryParam("hash") long hash,
      @QueryParam("timeout") long timeout,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    if (timeout <= 0) {
      sendErrorResponse(req, HttpServletResponse.SC_BAD_REQUEST, "timeout must be positive.");
      return;
    }

    final ResponseContext context = createContext(req.getHeader("Accept"));

    final ListenableFuture<ChangeRequestsSnapshot<WorkerHistoryItem>> future = workerTaskMonitor.getChangesSince(
        new ChangeRequestHistory.Counter(
            counter,
            hash
        )
    );

    final AsyncContext asyncContext = req.startAsync();

    asyncContext.addListener(
        new AsyncListener()
        {
          @Override
          public void onComplete(AsyncEvent event)
          {
          }

          @Override
          public void onTimeout(AsyncEvent event)
          {

            // HTTP 204 NO_CONTENT is sent to the client.
            future.cancel(true);
            event.getAsyncContext().complete();
          }

          @Override
          public void onError(AsyncEvent event)
          {
          }

          @Override
          public void onStartAsync(AsyncEvent event)
          {
          }
        }
    );

    Futures.addCallback(
        future,
        new FutureCallback<ChangeRequestsSnapshot>()
        {
          @Override
          public void onSuccess(ChangeRequestsSnapshot result)
          {
            try {
              HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              response.setStatus(HttpServletResponse.SC_OK);
              context.inputMapper.writerWithType(WorkerHolder.WORKER_SYNC_RESP_TYPE_REF)
                                 .writeValue(asyncContext.getResponse().getOutputStream(), result);
              asyncContext.complete();
            }
            catch (Exception ex) {
              log.debug(ex, "Request timed out or closed already.");
            }
          }

          @Override
          public void onFailure(Throwable th)
          {
            try {
              HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              if (th instanceof IllegalArgumentException) {
                response.sendError(HttpServletResponse.SC_BAD_REQUEST, th.getMessage());
              } else {
                response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, th.getMessage());
              }
              asyncContext.complete();
            }
            catch (Exception ex) {
              log.debug(ex, "Request timed out or closed already.");
            }
          }
        }
    );

    asyncContext.setTimeout(timeout);
  }

  @POST
  @Path("/assignTask")
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Response assignTask(Task task)
  {
    try {
      workerTaskMonitor.assignTask(task);
      return Response.ok().build();
    }
    catch (RuntimeException ex) {
      return Response.serverError().entity(ex.getMessage()).build();
    }
  }

  private void sendErrorResponse(HttpServletRequest req, int code, String error) throws IOException
  {
    AsyncContext asyncContext = req.startAsync();
    HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
    response.sendError(code, error);
    asyncContext.complete();
  }

  private ResponseContext createContext(String requestType)
  {
    boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType);
    return new ResponseContext(isSmile ? smileMapper : jsonMapper);
  }

  private static class ResponseContext
  {
    private final ObjectMapper inputMapper;

    ResponseContext(ObjectMapper inputMapper)
    {
      this.inputMapper = inputMapper;
    }
  }
}
