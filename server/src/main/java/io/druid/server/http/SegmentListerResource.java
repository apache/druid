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

package io.druid.server.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.server.coordination.SegmentChangeRequestHistory;
import io.druid.server.coordination.SegmentChangeRequestsSnapshot;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.server.security.AuthConfig;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import java.io.IOException;

/**
 */
@Path("/druid/internal/v1/segments/")
@ResourceFilters(StateResourceFilter.class)
public class SegmentListerResource
{
  protected static final EmittingLogger log = new EmittingLogger(SegmentListerResource.class);

  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  protected final AuthConfig authConfig;
  private final DataSegmentAnnouncer announcer;

  @Inject
  public SegmentListerResource(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      AuthConfig authConfig,
      DataSegmentAnnouncer announcer
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.authConfig = authConfig;
    this.announcer = announcer;
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  public void getSegments(
      @QueryParam("counter") long counter,
      @QueryParam("hash") long hash,
      @QueryParam("timeout") long timeout,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    if (timeout <= 0) {
      AsyncContext asyncContext = req.startAsync();
      HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
      response.sendError(HttpServletResponse.SC_BAD_REQUEST, "timeout must be > 0");
      asyncContext.complete();
      return;
    }

    final ResponseContext context = createContext(req.getHeader("Accept"));
    final ListenableFuture<SegmentChangeRequestsSnapshot> future = announcer.getSegmentChangesSince(
        new SegmentChangeRequestHistory.Counter(
            counter,
            hash
        )
    );

    final AsyncContext asyncContext = req.startAsync();

    asyncContext.addListener(
        new AsyncListener()
        {
          @Override
          public void onComplete(AsyncEvent event) throws IOException
          {
          }

          @Override
          public void onTimeout(AsyncEvent event) throws IOException
          {

            // HTTP 204 NO_CONTENT is sent to the client.
            future.cancel(true);
            event.getAsyncContext().complete();
          }

          @Override
          public void onError(AsyncEvent event) throws IOException
          {
          }

          @Override
          public void onStartAsync(AsyncEvent event) throws IOException
          {
          }
        }
    );

    Futures.addCallback(
        future,
        new FutureCallback<SegmentChangeRequestsSnapshot>()
        {
          @Override
          public void onSuccess(SegmentChangeRequestsSnapshot result)
          {
            try {
              HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
              response.setStatus(HttpServletResponse.SC_OK);
              context.inputMapper.writeValue(asyncContext.getResponse().getOutputStream(), result);
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
