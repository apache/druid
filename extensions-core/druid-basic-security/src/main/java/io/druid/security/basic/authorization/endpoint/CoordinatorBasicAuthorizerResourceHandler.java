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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.security.basic.BasicAuthUtils;
import io.druid.security.basic.BasicSecurityDBResourceException;
import io.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import io.druid.security.basic.authorization.db.updater.BasicAuthorizerMetadataStorageUpdater;
import io.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import io.druid.security.basic.authorization.entity.BasicAuthorizerRoleFull;
import io.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import io.druid.security.basic.authorization.entity.BasicAuthorizerUserFull;
import io.druid.security.basic.authorization.entity.UserAndRoleMap;
import io.druid.server.security.Authorizer;
import io.druid.server.security.AuthorizerMapper;
import io.druid.server.security.ResourceAction;

import javax.ws.rs.core.Response;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CoordinatorBasicAuthorizerResourceHandler implements BasicAuthorizerResourceHandler
{
  private static final Logger log = new Logger(CoordinatorBasicAuthorizerResourceHandler.class);

  private final BasicAuthorizerMetadataStorageUpdater storageUpdater;
  private final Map<String, BasicRoleBasedAuthorizer> authorizerMap;
  private final ObjectMapper objectMapper;

  @Inject
  public CoordinatorBasicAuthorizerResourceHandler(
      BasicAuthorizerMetadataStorageUpdater storageUpdater,
      AuthorizerMapper authorizerMapper,
      @Smile ObjectMapper objectMapper
  )
  {
    this.storageUpdater = storageUpdater;
    this.objectMapper = objectMapper;

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
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        storageUpdater.getCurrentUserMapBytes(authorizerName)
    );
    return Response.ok(userMap.keySet()).build();
  }

  @Override
  public Response getUser(String authorizerName, String userName, boolean isFull)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    if (isFull) {
      return getUserFull(authorizerName, userName);
    } else {
      return getUserSimple(authorizerName, userName);
    }
  }

  @Override
  public Response createUser(String authorizerName, String userName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.createUser(authorizerName, userName);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response deleteUser(String authorizerName, String userName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.deleteUser(authorizerName, userName);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response getAllRoles(String authorizerName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        storageUpdater.getCurrentRoleMapBytes(authorizerName)
    );

    return Response.ok(roleMap.keySet()).build();
  }

  @Override
  public Response getRole(String authorizerName, String roleName, boolean isFull)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    if (isFull) {
      return getRoleFull(authorizerName, roleName);
    } else {
      return getRoleSimple(authorizerName, roleName);
    }
  }

  @Override
  public Response createRole(String authorizerName, String roleName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.createRole(authorizerName, roleName);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response deleteRole(String authorizerName, String roleName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.deleteRole(authorizerName, roleName);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response assignRoleToUser(String authorizerName, String userName, String roleName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.assignRole(authorizerName, userName, roleName);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response unassignRoleFromUser(String authorizerName, String userName, String roleName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.unassignRole(authorizerName, userName, roleName);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response setRolePermissions(
      String authorizerName, String roleName, List<ResourceAction> permissions
  )
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.setPermissions(authorizerName, roleName, permissions);
      return Response.ok().build();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response getCachedMaps(String authorizerName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    UserAndRoleMap userAndRoleMap = new UserAndRoleMap(
        storageUpdater.getCachedUserMap(authorizerName),
        storageUpdater.getCachedRoleMap(authorizerName)
    );

    return Response.ok(userAndRoleMap).build();
  }

  @Override
  public Response refreshAll()
  {
    storageUpdater.refreshAllNotification();
    return Response.ok().build();
  }

  @Override
  public Response authorizerUpdateListener(String authorizerName, byte[] serializedUserAndRoleMap)
  {
    return Response.status(Response.Status.NOT_FOUND).build();
  }

  @Override
  public Response getLoadStatus()
  {
    Map<String, Boolean> loadStatus = new HashMap<>();
    authorizerMap.forEach(
        (authorizerName, authorizer) -> {
          loadStatus.put(authorizerName, storageUpdater.getCachedUserMap(authorizerName) != null);
        }
    );
    return Response.ok(loadStatus).build();
  }

  private static Response makeResponseForAuthorizerNotFound(String authorizerName)
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.<String, Object>of(
                       "error",
                       StringUtils.format("Basic authorizer with name [%s] does not exist.", authorizerName)
                   ))
                   .build();
  }

  private static Response makeResponseForBasicSecurityDBResourceException(BasicSecurityDBResourceException bsre)
  {
    return Response.status(Response.Status.BAD_REQUEST)
                   .entity(ImmutableMap.<String, Object>of(
                       "error", bsre.getMessage()
                   ))
                   .build();
  }

  private Response getUserSimple(String authorizerName, String userName)
  {
    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        storageUpdater.getCurrentUserMapBytes(authorizerName)
    );

    try {
      BasicAuthorizerUser user = userMap.get(userName);
      if (user == null) {
        throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
      }
      return Response.ok(user).build();
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Response getUserFull(String authorizerName, String userName)
  {
    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        storageUpdater.getCurrentUserMapBytes(authorizerName)
    );

    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        storageUpdater.getCurrentRoleMapBytes(authorizerName)
    );

    try {
      BasicAuthorizerUser user = userMap.get(userName);
      if (user == null) {
        throw new BasicSecurityDBResourceException("User [%s] does not exist.", userName);
      }

      Set<BasicAuthorizerRole> roles = new HashSet<>();
      for (String roleName : user.getRoles()) {
        BasicAuthorizerRole role = roleMap.get(roleName);
        if (role == null) {
          log.error("User [%s] had role [%s], but role was not found.", userName, roleName);
        } else {
          roles.add(role);
        }
      }

      BasicAuthorizerUserFull fullUser = new BasicAuthorizerUserFull(userName, roles);
      return Response.ok(fullUser).build();
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Response getRoleSimple(String authorizerName, String roleName)
  {
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        storageUpdater.getCurrentRoleMapBytes(authorizerName)
    );

    try {
      BasicAuthorizerRole role = roleMap.get(roleName);
      if (role == null) {
        throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
      }
      return Response.ok(role).build();
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Response getRoleFull(String authorizerName, String roleName)
  {
    Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
        objectMapper,
        storageUpdater.getCurrentRoleMapBytes(authorizerName)
    );

    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        storageUpdater.getCurrentUserMapBytes(authorizerName)
    );

    Set<String> users = new HashSet<>();
    for (BasicAuthorizerUser user : userMap.values()) {
      if (user.getRoles().contains(roleName)) {
        users.add(user.getName());
      }
    }

    try {
      BasicAuthorizerRole role = roleMap.get(roleName);
      if (role == null) {
        throw new BasicSecurityDBResourceException("Role [%s] does not exist.", roleName);
      }
      BasicAuthorizerRoleFull roleFull = new BasicAuthorizerRoleFull(
          roleName,
          users,
          role.getPermissions()
      );
      return Response.ok(roleFull).build();
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }
}
