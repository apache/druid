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

package io.druid.security.basic;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.guice.annotations.Json;
import io.druid.java.util.common.StringUtils;
import io.druid.security.basic.db.BasicSecurityStorageConnector;
import io.druid.server.security.ResourceAction;

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
import java.util.List;
import java.util.Map;

@Path("/druid/coordinator/v1/security")
public class BasicSecurityResource
{
  private final BasicSecurityStorageConnector dbConnector;
  private final ObjectMapper jsonMapper;

  @Inject
  public BasicSecurityResource(
      @Json ObjectMapper jsonMapper,
      BasicSecurityStorageConnector dbConnector
  )
  {
    this.jsonMapper = jsonMapper;
    this.dbConnector = dbConnector;
  }

  @GET
  @Path("/users")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllUsers(
      @Context HttpServletRequest req
  )
  {
    List<Map<String, Object>> users = dbConnector.getAllUsers();
    return Response.ok(users).build();
  }

  @GET
  @Path("/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getUser(
      @Context HttpServletRequest req,
      @PathParam("userName") final String userName
  )
  {
    Map<String, Object> user = dbConnector.getUser(userName);
    List<Map<String, Object>> roles = dbConnector.getRolesForUser(userName);
    List<Map<String, Object>> permissions = dbConnector.getPermissionsForUser(userName);

    Map<String, Object> userInfo = ImmutableMap.of(
        "user", user,
        "roles", roles,
        "permissions", permissions
    );

    return Response.ok(userInfo).build();
  }

  @POST
  @Path("/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createUser(
      @Context HttpServletRequest req,
      @PathParam("userName") String userName
  )
  {
    dbConnector.createUser(userName);
    return Response.ok().build();
  }

  @DELETE
  @Path("/users/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteUser(
      @Context HttpServletRequest req,
      @PathParam("userName") String userName
  )
  {
    Map<String, Object> dbUser = dbConnector.getUser(userName);
    if (dbUser == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", StringUtils.format("user [%s] not found", userName)))
                     .build();
    }

    dbConnector.deleteUser(userName);

    return Response.ok().build();
  }

  @GET
  @Path("/credentials/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getUserCredentials(
      @Context HttpServletRequest req,
      @PathParam("userName") final String userName
  )
  {
    Map<String, Object> credentials = dbConnector.getUserCredentials(userName);
    return Response.ok(credentials).build();
  }

  @POST
  @Path("/credentials/{userName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response updateUserCredentials(
      @Context HttpServletRequest req,
      @PathParam("userName") String userName,
      String password
  )
  {
    Map<String, Object> dbUser = dbConnector.getUser(userName);
    if (dbUser == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", StringUtils.format("user [%s] not found", userName)))
                     .build();
    }

    dbConnector.setUserCredentials(userName, password.toCharArray());
    return Response.ok().build();
  }

  @GET
  @Path("/roles")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAllRoles(
      @Context HttpServletRequest req
  )
  {
    List<Map<String, Object>> roles = dbConnector.getAllRoles();
    return Response.ok(roles).build();
  }

  @GET
  @Path("/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getRole(
      @Context HttpServletRequest req,
      @PathParam("roleName") final String roleName
  )
  {
    Map<String, Object> role = dbConnector.getRole(roleName);
    List<Map<String, Object>> users = dbConnector.getUsersWithRole(roleName);
    List<Map<String, Object>> permissions = dbConnector.getPermissionsForRole(roleName);

    Map<String, Object> roleInfo = ImmutableMap.of(
        "role", role,
        "users", users,
        "permissions", permissions
    );

    return Response.ok(roleInfo).build();
  }

  @POST
  @Path("/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response createRole(
      @Context HttpServletRequest req,
      @PathParam("roleName") final String roleName
  )
  {
    dbConnector.createRole(roleName);
    return Response.ok().build();
  }

  @DELETE
  @Path("/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteRole(
      @Context HttpServletRequest req,
      @PathParam("roleName") String roleName
  )
  {
    Map<String, Object> dbRole = dbConnector.getRole(roleName);
    if (dbRole == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", StringUtils.format("role [%s] not found", roleName)))
                     .build();
    }

    dbConnector.deleteRole(roleName);

    return Response.ok().build();
  }


  @POST
  @Path("/users/{userName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response assignRoleToUser(
      @Context HttpServletRequest req,
      @PathParam("userName") String userName,
      @PathParam("roleName") String roleName
  )
  {
    dbConnector.assignRole(userName, roleName);
    return Response.ok().build();
  }

  @DELETE
  @Path("/users/{userName}/roles/{roleName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response unassignRoleFromUser(
      @Context HttpServletRequest req,
      @PathParam("userName") String userName,
      @PathParam("roleName") String roleName
  )
  {
    dbConnector.unassignRole(userName, roleName);
    return Response.ok().build();
  }

  @POST
  @Path("/roles/{roleName}/permissions")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response addPermissionsToRole(
      @Context HttpServletRequest req,
      @PathParam("roleName") String roleName,
      List<ResourceAction> resourceActions
  )
  {
    Map<String, Object> dbRole = dbConnector.getRole(roleName);
    if (dbRole == null) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of(
                         "error",
                         StringUtils.format("role does not exist: %s", roleName)
                     ))
                     .build();
    }

    for (ResourceAction resourceAction : resourceActions) {
      try {
        final byte[] serializedPermission = jsonMapper.writeValueAsBytes(resourceAction);
        dbConnector.addPermission(roleName, serializedPermission, "a");
      }
      catch (JsonProcessingException jpe) {
        return Response.status(Response.Status.BAD_REQUEST)
                       .entity(ImmutableMap.<String, Object>of(
                           "error",
                           StringUtils.format(
                               "cannot serialize permission: %s",
                               resourceAction
                           )
                       ))
                       .build();
      }
    }

    return Response.ok().build();
  }

  @DELETE
  @Path("/permissions/{permId}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response removePermissionFromRole(
      @Context HttpServletRequest req,
      @PathParam("roleName") String roleName,
      @PathParam("permId") Integer permId
  )
  {
    dbConnector.deletePermission(permId);
    return Response.ok().build();
  }

  @GET
  @Path("/authenticationMappings/{authenticationName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response getAuthenticationMapping(
      @Context HttpServletRequest req,
      @PathParam("authenticationName") String authenticationName
  )
  {
    String authorizationName = dbConnector.getAuthorizationNameFromAuthenticationName(authenticationName);
    return Response.ok(authorizationName).build();
  }

  @POST
  @Path("/authenticationMappings/{authenticationName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response setAuthenticationMapping(
      @Context HttpServletRequest req,
      @PathParam("authenticationName") String authenticationName,
      String authorizationName
  )
  {
    dbConnector.createAuthenticationToAuthorizationNameMapping(authenticationName, authorizationName);
    return Response.ok().build();
  }

  @DELETE
  @Path("/authenticationMappings/{authenticationName}")
  @Produces(MediaType.APPLICATION_JSON)
  @Consumes(MediaType.APPLICATION_JSON)
  @ResourceFilters(BasicSecurityResourceFilter.class)
  public Response deleteAuthenticationMapping(
      @Context HttpServletRequest req,
      @PathParam("authenticationName") String authenticationName
  )
  {
    dbConnector.deleteAuthenticationToAuthorizationNameMapping(authenticationName);
    return Response.ok().build();
  }
}
