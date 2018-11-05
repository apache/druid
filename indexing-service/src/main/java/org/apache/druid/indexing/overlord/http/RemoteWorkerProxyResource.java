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

import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.server.http.security.ConfigResourceFilter;
import org.jboss.netty.handler.codec.http.HttpMethod;

import javax.inject.Inject;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;

@Path("/druid/indexer/v1/remote-worker")
public class RemoteWorkerProxyResource
{
  private static final Logger log = new Logger(RemoteWorkerProxyResource.class);

  private final HttpClient httpClient;
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(StandardCharsets.UTF_8);

  @Inject
  public RemoteWorkerProxyResource(@EscalatedGlobal final HttpClient httpClient)
  {
    this.httpClient = httpClient;
  }

  @POST
  @Path("/{scheme}/{host}/disable")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response doDisable(
      @PathParam("scheme") final String scheme,
      @PathParam("host") final String host
  )
  {
    return sendRequestToWorker(scheme, host, "/druid/worker/v1/disable", "Failed to disable worker!");
  }

  @POST
  @Path("/{scheme}/{host}/enable")
  @Produces(MediaType.APPLICATION_JSON)
  @ResourceFilters(ConfigResourceFilter.class)
  public Response doEnable(
      @PathParam("scheme") final String scheme,
      @PathParam("host") final String host
  )
  {
    return sendRequestToWorker(scheme, host, "/druid/worker/v1/enable", "Failed to enable worker!");
  }

  private Response sendRequestToWorker(String scheme, String host, String path, String errorMessage)
  {
    try {
      final URL url = new URI(
          StringUtils.format("%s://%s%s", scheme, host, path)
      ).toURL();

      final StatusResponseHolder response = httpClient.go(
          new Request(HttpMethod.POST, url),
          RESPONSE_HANDLER
      ).get();

      if (response.getStatus().getCode() == 200) {
        return Response.ok(response.getContent()).build();
      } else {
        return Response.status(response.getStatus().getCode())
                       .entity(response.getContent())
                       .build();
      }
    }
    catch (Exception e) {
      log.warn(e, errorMessage);
      return Response.serverError().build();
    }
  }
}
