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

package io.druid.security.basic.authorization.endpoint;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.authorization.db.cache.BasicAuthorizerCacheManager;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ResourceAction;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultBasicAuthorizerResourceHandler implements BasicAuthorizerResourceHandler
{
  private static final Logger log = new Logger(DefaultBasicAuthorizerResourceHandler.class);
  private static final Response NOT_FOUND_RESPONSE = Response.status(Response.Status.NOT_FOUND).build();

  private final BasicAuthorizerCacheManager cacheManager;
  private final Map<String, BasicRoleBasedAuthorizer> authorizerMap;

  @Inject
  public DefaultBasicAuthorizerResourceHandler(
      BasicAuthorizerCacheManager cacheManager,
      AuthorizerMapper authorizerMapper
  )
  {
    this.cacheManager = cacheManager;

    this.authorizerMap = Maps.newHashMap();
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
  public Response getUser(String authorizerName, String userName, boolean isFull)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response createUser(String authorizerName, String userName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response deleteUser(String authorizerName, String userName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getAllRoles(String authorizerName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getRole(String authorizerName, String roleName, boolean isFull)
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
  public Response unassignRoleFromUser(String authorizerName, String userName, String roleName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response setRolePermissions(
      String authorizerName, String roleName, List<ResourceAction> permissions
  )
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response getCachedMaps(String authorizerName)
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response refreshAll()
  {
    return NOT_FOUND_RESPONSE;
  }

  @Override
  public Response authorizerUpdateListener(String authorizerName, byte[] serializedUserAndRoleMap)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      String errMsg = StringUtils.format("Received update for unknown authorizer[%s]", authorizerName);
      log.error(errMsg);
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of(
                         "error",
                         StringUtils.format(errMsg)
                     ))
                     .build();
    }

    cacheManager.handleAuthorizerUpdate(authorizerName, serializedUserAndRoleMap);
    return Response.ok().build();
  }

  @Override
  public Response getLoadStatus()
  {
    Map<String, Boolean> loadStatus = new HashMap<>();
    authorizerMap.forEach(
        (authorizerName, authorizer) -> {
          loadStatus.put(authorizerName, cacheManager.getUserMap(authorizerName) != null);
        }
    );
    return Response.ok(loadStatus).build();
  }
}
