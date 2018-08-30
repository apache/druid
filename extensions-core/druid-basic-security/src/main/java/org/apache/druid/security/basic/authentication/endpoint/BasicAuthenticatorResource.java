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

package org.apache.druid.security.basic.authentication.endpoint;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.security.basic.BasicSecurityResourceFilter;
import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorCredentialUpdate;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/druid-ext/basic-security/authentication")
@LazySingleton
public class BasicAuthenticatorResource
{
  private final BasicAuthenticatorResourceHandler handler;

  @Inject
  public BasicAuthenticatorResource(
      BasicAuthenticatorResourceHandler handler
  )
  {
    this.handler = handler;
  }

  /**
   * @param req HTTP request
   *
   * @return Load status of authenticator DB caches
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
   * Sends an "update" notification to all services with the current user database state,
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

  /**
   * @param req HTTP request
   *
   * @return List of all users
   */
  @GET
  @Path("/db/{authenticatorName}/users")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllUsers(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName
  )
  {
    return handler.getAllUsers(authenticatorName);
  }

  /**
   * @param req      HTTP request
   * @param userName Name of user to retrieve information about
   *
   * @return Name and credentials of the user with userName, 400 error response if user doesn't exist
   */
  @GET
  @Path("/db/{authenticatorName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getUser(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      @PathParam("userName") final String userName
  )
  {
    return handler.getUser(authenticatorName, userName);
  }

  /**
   * Create a new user with name userName
   *
   * @param req      HTTP request
   * @param userName Name to assign the new user
   *
   * @return OK response, or 400 error response if user already exists
   */
  @POST
  @Path("/db/{authenticatorName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createUser(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      @PathParam("userName") String userName
  )
  {
    return handler.createUser(authenticatorName, userName);
  }

  /**
   * Delete a user
   *
   * @param req      HTTP request
   * @param userName Name of user to delete
   *
   * @return OK response, or 400 error response if user doesn't exist
   */
  @DELETE
  @Path("/db/{authenticatorName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteUser(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      @PathParam("userName") String userName
  )
  {
    return handler.deleteUser(authenticatorName, userName);
  }

  /**
   * Assign credentials for a user
   *
   * @param req      HTTP request
   * @param userName Name of user
   *
   * @return OK response, 400 error if user doesn't exist
   */
  @POST
  @Path("/db/{authenticatorName}/users/{userName}/credentials")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response updateUserCredentials(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      @PathParam("userName") String userName,
      BasicAuthenticatorCredentialUpdate update
  )
  {
    return handler.updateUserCredentials(authenticatorName, userName, update);
  }

  /**
   * @param req HTTP request
   *
   * @return serialized user map
   */
  @GET
  @Path("/db/{authenticatorName}/cachedSerializedUserMap")
  @Produces(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getCachedSerializedUserMap(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName
  )
  {
    return handler.getCachedSerializedUserMap(authenticatorName);
  }

  /**
   * Listen for update notifications for the auth storage
   */
  @POST
  @Path("/listen/{authenticatorName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response authenticatorUpdateListener(
      @Context HttpServletRequest req,
      @PathParam("authenticatorName") final String authenticatorName,
      byte[] serializedUserMap
  )
  {
    return handler.authenticatorUpdateListener(authenticatorName, serializedUserMap);
  }
}
