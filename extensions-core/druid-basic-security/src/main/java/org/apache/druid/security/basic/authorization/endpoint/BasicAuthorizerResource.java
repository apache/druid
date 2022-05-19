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

package org.apache.druid.security.basic.authorization.endpoint;

import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.security.basic.BasicSecurityResourceFilter;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.AuthValidator;
import org.apache.druid.server.security.ResourceAction;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Path("/druid-ext/basic-security/authorization")
@LazySingleton
public class BasicAuthorizerResource
{
  private final BasicAuthorizerResourceHandler resourceHandler;
  private final AuthValidator authValidator;

  @Inject
  public BasicAuthorizerResource(
      BasicAuthorizerResourceHandler resourceHandler,
      AuthValidator authValidator
  )
  {
    this.resourceHandler = resourceHandler;
    this.authValidator = authValidator;
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
    return resourceHandler.getLoadStatus();
  }

  /**
   * @param req HTTP request
   *
   * Sends an "update" notification to all services with the current user/role database state,
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
    return resourceHandler.refreshAll();
  }


  /**
   * @param req HTTP request
   *
   * @return List of all users
   */
  @GET
  @Path("/db/{authorizerName}/users")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllUsers(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getAllUsers(authorizerName);
  }

  /**
   * @param req HTTP request
   *
   * @return List of all groupMappings
   */
  @GET
  @Path("/db/{authorizerName}/groupMappings")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllGroupMappings(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getAllGroupMappings(authorizerName);
  }

  /**
   * @param req      HTTP request
   * @param userName Name of user to retrieve information about
   *
   * @return Name, roles, and permissions of the user with userName, 400 error response if user doesn't exist
   */
  @GET
  @Path("/db/{authorizerName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") final String userName,
      @QueryParam("full") String full,
      @QueryParam("simplifyPermissions") String simplifyPermissions
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getUser(authorizerName, userName, full != null, simplifyPermissions != null);
  }

  /**
   * @param req               HTTP request
   * @param groupMappingName  Name of groupMapping to retrieve information about
   *
   * @return Name, groupPattern, roles, and permissions of the groupMapping with groupMappingName, 400 error response if groupMapping doesn't exist
   */
  @GET
  @Path("/db/{authorizerName}/groupMappings/{groupMappingName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getGroupMapping(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("groupMappingName") final String groupMappingName,
      @QueryParam("full") String full
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getGroupMapping(authorizerName, groupMappingName, full != null);
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
  @Path("/db/{authorizerName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") String userName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.createUser(authorizerName, userName);
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
  @Path("/db/{authorizerName}/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") String userName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.deleteUser(authorizerName, userName);
  }

  /**
   * Create a new groupMapping with name groupMappingName
   *
   * @param req               HTTP request
   * @param groupMappingName  Name to assign the new groupMapping
   *
   * @return OK response, or 400 error response if groupMapping already exists
   */
  @POST
  @Path("/db/{authorizerName}/groupMappings/{groupMappingName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createGroupMapping(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("groupMappingName") String groupMappingName,
      BasicAuthorizerGroupMapping groupMapping
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.createGroupMapping(
        authorizerName,
        new BasicAuthorizerGroupMapping(groupMappingName, groupMapping.getGroupPattern(), groupMapping.getRoles())
    );
  }

  /**
   * Delete a groupMapping with name groupMappingName
   *
   * @param req               HTTP request
   * @param groupMappingName  Name of groupMapping to delete
   *
   * @return OK response, or 400 error response if groupMapping doesn't exist
   */
  @DELETE
  @Path("/db/{authorizerName}/groupMappings/{groupMappingName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteGroupMapping(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("groupMappingName") String groupMappingName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.deleteGroupMapping(authorizerName, groupMappingName);
  }

  /**
   * @param req HTTP request
   *
   * @return List of all roles
   */
  @GET
  @Path("/db/{authorizerName}/roles")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllRoles(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getAllRoles(authorizerName);
  }

  /**
   * Get info about a role
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return Role name, users with role, groupMappings with role, and permissions of role. 400 error if role doesn't exist.
   */
  @GET
  @Path("/db/{authorizerName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getRole(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") final String roleName,
      @QueryParam("full") String full,
      @QueryParam("simplifyPermissions") String simplifyPermissions
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getRole(authorizerName, roleName, full != null, simplifyPermissions != null);
  }

  /**
   * Create a new role.
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return OK response, 400 error if role already exists
   */
  @POST
  @Path("/db/{authorizerName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createRole(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") final String roleName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.createRole(authorizerName, roleName);
  }

  /**
   * Delete a role.
   *
   * @param req      HTTP request
   * @param roleName Name of role
   *
   * @return OK response, 400 error if role doesn't exist.
   */
  @DELETE
  @Path("/db/{authorizerName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteRole(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") String roleName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.deleteRole(authorizerName, roleName);
  }

  /**
   * Assign a role to a user.
   *
   * @param req      HTTP request
   * @param userName Name of user
   * @param roleName Name of role
   *
   * @return OK response. 400 error if user/role don't exist, or if user already has the role
   */
  @POST
  @Path("/db/{authorizerName}/users/{userName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response assignRoleToUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") String userName,
      @PathParam("roleName") String roleName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.assignRoleToUser(authorizerName, userName, roleName);
  }

  /**
   * Remove a role from a user.
   *
   * @param req      HTTP request
   * @param userName Name of user
   * @param roleName Name of role
   *
   * @return OK response. 400 error if user/role don't exist, or if user does not have the role.
   */
  @DELETE
  @Path("/db/{authorizerName}/users/{userName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response unassignRoleFromUser(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("userName") String userName,
      @PathParam("roleName") String roleName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.unassignRoleFromUser(authorizerName, userName, roleName);
  }

  /**
   * Assign a role to a groupMapping.
   *
   * @param req       HTTP request
   * @param groupMappingName Name of groupMapping
   * @param roleName  Name of role
   *
   * @return OK response. 400 error if groupMapping/role don't exist, or if groupMapping already has the role
   */
  @POST
  @Path("/db/{authorizerName}/groupMappings/{groupMappingName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response assignRoleToGroupMapping(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("groupMappingName") String groupMappingName,
      @PathParam("roleName") String roleName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.assignRoleToGroupMapping(authorizerName, groupMappingName, roleName);
  }

  /**
   * Remove a role from a groupMapping.
   *
   * @param req       HTTP request
   * @param groupMappingName Name of groupMapping
   * @param roleName  Name of role
   *
   * @return OK response. 400 error if groupMapping/role don't exist, or if groupMapping does not have the role.
   */
  @DELETE
  @Path("/db/{authorizerName}/groupMappings/{groupMappingName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response unassignRoleFromGroupMapping(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("groupMappingName") String groupMappingName,
      @PathParam("roleName") String roleName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.unassignRoleFromGroupMapping(authorizerName, groupMappingName, roleName);
  }

  /**
   * Set the permissions of a role. This replaces the previous permissions of the role.
   *
   * @param req         HTTP request
   * @param roleName    Name of role
   * @param permissions Permissions to set
   *
   * @return OK response. 400 error if role doesn't exist.
   */
  @POST
  @Path("/db/{authorizerName}/roles/{roleName}/permissions")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response setRolePermissions(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") String roleName,
      List<ResourceAction> permissions
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.setRolePermissions(authorizerName, roleName, permissions);
  }

  /**
   * Get the permissions of a role.
   *
   * @param req         HTTP request
   * @param roleName    Name of role
   *
   * @return OK response. 400 error if role doesn't exist.
   */
  @GET
  @Path("/db/{authorizerName}/roles/{roleName}/permissions")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getRolePermissions(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      @PathParam("roleName") String roleName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getRolePermissions(authorizerName, roleName);
  }

  /**
   * @param req HTTP request
   *
   * @return serialized user map
   */
  @GET
  @Path("/db/{authorizerName}/cachedSerializedUserMap")
  @Produces(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getCachedSerializedUserMap(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getCachedUserMaps(authorizerName);
  }

  /**
   * @param req HTTP request
   *
   * @return serialized groupMapping map
   */
  @GET
  @Path("/db/{authorizerName}/cachedSerializedGroupMappingMap")
  @Produces(SmileMediaTypes.APPLICATION_JACKSON_SMILE)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getCachedSerializedGroupMap(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.getCachedGroupMappingMaps(authorizerName);
  }


  /**
   * Listen for update notifications for the user auth storage
   * @deprecated  path /listen/{authorizerName} is to replaced by /listen/users/{authorizerName}
   *              use {@link #authorizerUserUpdateListener(HttpServletRequest, String, byte[])} instead
   */
  @POST
  @Path("/listen/{authorizerName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  @Deprecated
  public Response authorizerUpdateListener(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      byte[] serializedUserAndRoleMap
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.authorizerUserUpdateListener(authorizerName, serializedUserAndRoleMap);
  }

  /**
   * Listen for update notifications for the user auth storage
   */
  @POST
  @Path("/listen/users/{authorizerName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response authorizerUserUpdateListener(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      byte[] serializedUserAndRoleMap
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.authorizerUserUpdateListener(authorizerName, serializedUserAndRoleMap);
  }

  /**
   * Listen for update notifications for the groupMapping auth storage
   */
  @POST
  @Path("/listen/groupMappings/{authorizerName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response authorizerGroupMappingUpdateListener(
      @Context HttpServletRequest req,
      @PathParam("authorizerName") final String authorizerName,
      byte[] serializedGroupMappingAndRoleMap
  )
  {
    authValidator.validateAuthorizerName(authorizerName);
    return resourceHandler.authorizerGroupMappingUpdateListener(authorizerName, serializedGroupMappingAndRoleMap);
  }
}
