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

package org.apache.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.HttpServerInventoryView;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordination.BatchDataSegmentAnnouncer;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.coordination.DataSegmentChangeRequest;
import org.apache.druid.server.coordination.SegmentLoadDropHandler;
import org.apache.druid.server.coordinator.HttpLoadQueuePeon;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.annotation.Nullable;
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
import java.io.IOException;
import java.util.List;

/**
 * Endpoints exposed here are to be used only for druid internal management of segments by Coordinators, Brokers etc.
 */
@Path("/druid-internal/v1/segments/")
@ResourceFilters(StateResourceFilter.class)
public class SegmentListerResource
{
  protected static final EmittingLogger log = new EmittingLogger(SegmentListerResource.class);

  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  private final BatchDataSegmentAnnouncer announcer;
  private final SegmentLoadDropHandler loadDropRequestHandler;

  @Inject
  public SegmentListerResource(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      @Nullable BatchDataSegmentAnnouncer announcer,
      @Nullable SegmentLoadDropHandler loadDropRequestHandler
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.announcer = announcer;
    this.loadDropRequestHandler = loadDropRequestHandler;
  }

  /**
   * This endpoint is used by HttpServerInventoryView to keep an up-to-date list of segments served by
   * historical/realtime nodes.
   *
   * This endpoint lists segments served by this server and can also incrementally provide the segments added/dropped
   * since last response.
   *
   * Here is how, this is used.
   *
   * (1) Client sends first request /druid/internal/v1/segments?counter=-1&timeout=<timeout>
   * Server responds with list of segments currently served and a <counter,hash> pair.
   *
   * (2) Client sends subsequent requests /druid/internal/v1/segments?counter=<counter>&hash=<hash>&timeout=<timeout>
   * Where <counter,hash> values are used from the last response. Server responds with list of segment updates
   * since given counter.
   *
   * This endpoint makes the client wait till either there is some segment update or given timeout elapses.
   *
   * So, clients keep on sending next request immediately after receiving the response in order to keep the list
   * of segments served by this server up-to-date.
   *
   * @param counter counter received in last response.
   * @param hash hash received in last response.
   * @param timeout after which response is sent even if there are no new segment updates.
   * @param req
   * @return null to avoid "MUST return a non-void type" warning.
   * @throws IOException
   */
  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public Void getSegments(
      @QueryParam("counter") long counter,
      @QueryParam("hash") long hash,
      @QueryParam("timeout") long timeout,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    if (announcer == null) {
      sendErrorResponse(req, HttpServletResponse.SC_NOT_FOUND, "announcer is not available.");
      return null;
    }

    if (timeout <= 0) {
      sendErrorResponse(req, HttpServletResponse.SC_BAD_REQUEST, "timeout must be positive.");
      return null;
    }

    final ResponseContext context = createContext(req.getHeader("Accept"));
    final ListenableFuture<ChangeRequestsSnapshot<DataSegmentChangeRequest>> future = announcer.getSegmentChangesSince(
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
        new FutureCallback<ChangeRequestsSnapshot<DataSegmentChangeRequest>>()
        {
          @Override
          public void onSuccess(ChangeRequestsSnapshot<DataSegmentChangeRequest> result)
          {
            try {
              HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              response.setStatus(HttpServletResponse.SC_OK);
              context.inputMapper.writerWithType(HttpServerInventoryView.SEGMENT_LIST_RESP_TYPE_REF)
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
    return null;
  }

  /**
   * This endpoint is used by HttpLoadQueuePeon to assign segment load/drop requests batch. This endpoint makes the
   * client wait till one of the following events occur. Note that this is implemented using async IO so no jetty
   * threads are held while in wait.
   *
   * (1) Given timeout elapses.
   * (2) Some load/drop request completed.
   *
   * It returns a map of "load/drop request -> SUCCESS/FAILED/PENDING status" for each request in the batch.
   */
  @POST
  @Path("/changeRequests")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public void applyDataSegmentChangeRequests(
      @QueryParam("timeout") long timeout,
      List<DataSegmentChangeRequest> changeRequestList,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    if (loadDropRequestHandler == null) {
      sendErrorResponse(req, HttpServletResponse.SC_NOT_FOUND, "load/drop handler is not available.");
      return;
    }

    if (timeout <= 0) {
      sendErrorResponse(req, HttpServletResponse.SC_BAD_REQUEST, "timeout must be positive.");
      return;
    }

    if (changeRequestList == null || changeRequestList.isEmpty()) {
      sendErrorResponse(req, HttpServletResponse.SC_BAD_REQUEST, "No change requests provided.");
      return;
    }

    final ResponseContext context = createContext(req.getHeader("Accept"));
    final ListenableFuture<List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus>> future = loadDropRequestHandler
        .processBatch(changeRequestList);

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
        new FutureCallback<List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus>>()
        {
          @Override
          public void onSuccess(List<SegmentLoadDropHandler.DataSegmentChangeRequestAndStatus> result)
          {
            try {
              HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              response.setStatus(HttpServletResponse.SC_OK);
              context.inputMapper.writerWithType(HttpLoadQueuePeon.RESPONSE_ENTITY_TYPE_REF)
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
