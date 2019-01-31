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

package org.apache.druid.security.basic.escalator.endpoint;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.security.basic.BasicSecurityResourceFilter;
import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid-ext/basic-security/escalator")
@LazySingleton
public class BasicEscalatorResource
{
  private final BasicEscalatorResourceHandler handler;

  @Inject
  public BasicEscalatorResource(
      BasicEscalatorResourceHandler handler
  )
  {
    this.handler = handler;
  }

  /**
   * @param req      HTTP request
   *
   * @return escalator credential
   */
  @GET
  @Path("/db/credential")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getEscalatorCredential(
      @Context HttpServletRequest req
  )
  {
    return handler.getEscalatorCredential();
  }

  /**
   * Update escalator credential
   *
   * @param req                 HTTP request
   * @param escalatorCredential Escalator credential
   *
   * @return OK response
   */
  @POST
  @Path("/db/credential")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response updateEscalatorCredential(
      @Context HttpServletRequest req,
      BasicEscalatorCredential escalatorCredential
  )
  {
    return handler.updateEscalatorCredential(escalatorCredential);
  }

  /**
   * @param req HTTP request
   *
   * @return serialized escalator credential
   */
  @GET
  @Path("/db/cachedSerializedCredential")
  @Produces(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getCachedSerializedEscalatorCredential(
      @Context HttpServletRequest req
  )
  {
    return handler.getCachedSerializedEscalatorCredential();
  }

  /**
   * Listen for escalator credential update notifications for the escalator storage
   */
  @POST
  @Path("/listen/credential")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response escalatorCredentialUpdateListener(
      @Context HttpServletRequest req,
      byte[] serializedEscalatorCredential
  )
  {
    return handler.escalatorCredentialUpdateListener(serializedEscalatorCredential);
  }

  /**
   * @param req HTTP request
   *
   * @return Load status of escalator DB caches
   */
  @GET
  @Path("/loadStatus")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getLoadStatus(
      @Context HttpServletRequest req
  )
  {
    return handler.getLoadStatus();
  }

  /**
   * @param req HTTP request
   *
   * Sends an "update" notification to all services with the current escalator database state,
   * causing them to refresh their DB cache state.
   */
  @GET
  @Path("/refreshAll")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response refreshAll(
      @Context HttpServletRequest req
  )
  {
    return handler.refreshAll();
  }
}
