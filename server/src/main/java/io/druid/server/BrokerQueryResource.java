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

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.client.ServerViewUtil;
import io.druid.client.TimelineServerView;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.Query;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.server.security.AuthConfig;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;

/**
 */
@Path("/druid/v2/")
public class BrokerQueryResource extends QueryResource
{
  private final TimelineServerView brokerServerView;

  @Inject
  public BrokerQueryResource(
      QueryLifecycleFactory queryLifecycleFactory,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryManager queryManager,
      AuthConfig authConfig,
      TimelineServerView brokerServerView
  )
  {
    super(
        queryLifecycleFactory,
        jsonMapper,
        smileMapper,
        queryManager,
        authConfig
    );
    this.brokerServerView = brokerServerView;
  }

  @POST
  @Path("/candidates")
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  @ResourceFilters(StateResourceFilter.class)
  public Response getQueryTargets(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @QueryParam("numCandidates") @DefaultValue("-1") int numCandidates,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final ResponseContext context = createContext(req.getContentType(), pretty != null);
    try {
      Query<?> query = context.getObjectMapper().readValue(in, Query.class);
      return context.ok(
          ServerViewUtil.getTargetLocations(
              brokerServerView,
              query.getDataSource(),
              query.getIntervals(),
              numCandidates
          )
      );
    }
    catch (Exception e) {
      return context.gotError(e);
    }
  }
}
