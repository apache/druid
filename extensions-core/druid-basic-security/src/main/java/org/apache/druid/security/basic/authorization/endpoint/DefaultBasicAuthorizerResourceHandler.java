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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultBasicAuthorizerResourceHandler implements BasicAuthorizerResourceHandler
{
  private static final Logger log = new Logger(DefaultBasicAuthorizerResourceHandler.class);
  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();
  private static final String UNKNOWN_AUTHORIZER_MSG_FORMAT = "Received update for unknown authorizer[%s]";

  private final BasicAuthorizerCacheManager cacheManager;
  private final Map<String, BasicRoleBasedAuthorizer> authorizerMap;

  @Inject
  public DefaultBasicAuthorizerResourceHandler(
      BasicAuthorizerCacheManager cacheManager,
      AuthorizerMapper authorizerMapper
  )
  {
    this.cacheManager = cacheManager;

    this.authorizerMap = new HashMap<>();
    for (Map.Entry<String, Authorizer> authorizerEntry : authorizerMapper.getAuthorizerMap().entrySet()) {
      final String authorizerName = authorizerEntry.getKey();
      final Authorizer authorizer = authorizerEntry.getValue();
      if (authorizer instanceof BasicRoleBasedAuthorizer) {
        authorizerMap.put(
            authorizerName,
            (BasicRoleBasedAuthorizer) authorizer
        );
      }
    }
  }

  @Override
  public Response getAllUsers(String authorizerName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getAllGroupMappings(String authorizerName)
  {
    return NOT_FOUND_RESPONSE;
  }


  @Override
  public Response getUser(String authorizerName, String userName, boolean isFull, boolean simplifyPermissions)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getGroupMapping(String authorizerName, String groupMappingName, boolean isFull)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response createUser(String authorizerName, String userName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response createGroupMapping(String authorizerName, BasicAuthorizerGroupMapping groupMapping)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response deleteUser(String authorizerName, String userName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response deleteGroupMapping(String authorizerName, String groupMappingName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getAllRoles(String authorizerName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getRole(String authorizerName, String roleName, boolean isFull, boolean simplifyPermissions)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response createRole(String authorizerName, String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response deleteRole(String authorizerName, String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response assignRoleToUser(String authorizerName, String userName, String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response assignRoleToGroupMapping(String authorizerName, String groupMappingName, String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response unassignRoleFromUser(String authorizerName, String userName, String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response unassignRoleFromGroupMapping(String authorizerName, String groupMappingName, String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response setRolePermissions(String authorizerName, String roleName, List<ResourceAction> permissions)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getRolePermissions(String authorizerName, String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getCachedUserMaps(String authorizerName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getCachedGroupMappingMaps(String authorizerName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response refreshAll()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response authorizerUserUpdateListener(String authorizerName, byte[] serializedUserAndRoleMap)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      log.error(UNKNOWN_AUTHORIZER_MSG_FORMAT, authorizerName);
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of(
                         "error",
                         StringUtils.format(UNKNOWN_AUTHORIZER_MSG_FORMAT, authorizerName)
                     ))
                     .build();
    }

    cacheManager.handleAuthorizerUserUpdate(authorizerName, serializedUserAndRoleMap);
    return Response.ok().build();
  }

  @Override
  public Response authorizerGroupMappingUpdateListener(String authorizerName, byte[] serializedGroupMappingAndRoleMap)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      log.error(UNKNOWN_AUTHORIZER_MSG_FORMAT, authorizerName);
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of(
                         "error",
                         StringUtils.format(UNKNOWN_AUTHORIZER_MSG_FORMAT, authorizerName)
                     ))
                     .build();
    }

    cacheManager.handleAuthorizerGroupMappingUpdate(authorizerName, serializedGroupMappingAndRoleMap);
    return Response.ok().build();
  }

  @Override
  public Response getLoadStatus()
  {
    Map<String, Boolean> loadStatus = new HashMap<>();
    authorizerMap.forEach(
        (authorizerName, authorizer) -> {
          loadStatus.put(authorizerName, cacheManager.getUserMap(authorizerName) != null &&
                                         cacheManager.getRoleMap(authorizerName) != null &&
                                         cacheManager.getGroupMappingMap(authorizerName) != null &&
                                         cacheManager.getGroupMappingRoleMap(authorizerName) != null);
        }
    );
    return Response.ok(loadStatus).build();
  }
}
