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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.BasicAuthUtils;
import org.apache.druid.security.basic.BasicSecurityDBResourceException;
import org.apache.druid.security.basic.authorization.BasicRoleBasedAuthorizer;
import org.apache.druid.security.basic.authorization.db.updater.BasicAuthorizerMetadataStorageUpdater;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMappingFull;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRoleFull;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRoleSimplifiedPermissions;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUserFull;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUserFullSimplifiedPermissions;
import org.apache.druid.security.basic.authorization.entity.GroupMappingAndRoleMap;
import org.apache.druid.security.basic.authorization.entity.UserAndRoleMap;
import org.apache.druid.server.initialization.jetty.HttpResponses;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceAction;

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
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
        objectMapper,
        storageUpdater.getCurrentUserMapBytes(authorizerName)
    );
    return HttpResponses.OK.entity(userMap.keySet());
  }

  @Override
  public Response getAllGroupMappings(String authorizerName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    Map<String, BasicAuthorizerGroupMapping> groupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        storageUpdater.getCurrentGroupMappingMapBytes(authorizerName)
    );
    return HttpResponses.OK.entity(groupMappingMap.keySet());
  }

  @Override
  public Response getUser(String authorizerName, String userName, boolean isFull, boolean simplifyPermissions)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    if (isFull) {
      return getUserFull(authorizerName, userName, simplifyPermissions);
    } else {
      return getUserSimple(authorizerName, userName);
    }
  }

  @Override
  public Response getGroupMapping(String authorizerName, String groupMappingName, boolean isFull)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    if (isFull) {
      return getGroupMappingFull(authorizerName, groupMappingName);
    } else {
      return getGroupMappingSimple(authorizerName, groupMappingName);
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
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response createGroupMapping(String authorizerName, BasicAuthorizerGroupMapping groupMapping)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.createGroupMapping(authorizerName, groupMapping);
      return HttpResponses.OK.empty();
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
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response deleteGroupMapping(String authorizerName, String groupMappingName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.deleteGroupMapping(authorizerName, groupMappingName);
      return HttpResponses.OK.empty();
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

    return HttpResponses.OK.entity(roleMap.keySet());
  }

  @Override
  public Response getRole(String authorizerName, String roleName, boolean isFull, boolean simplifyPermissions)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    if (isFull) {
      return getRoleFull(authorizerName, roleName, simplifyPermissions);
    } else {
      return getRoleSimple(authorizerName, roleName, simplifyPermissions);
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
      return HttpResponses.OK.empty();
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
      return HttpResponses.OK.empty();
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
      storageUpdater.assignUserRole(authorizerName, userName, roleName);
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response assignRoleToGroupMapping(String authorizerName, String groupMappingName, String roleName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.assignGroupMappingRole(authorizerName, groupMappingName, roleName);
      return HttpResponses.OK.empty();
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
      storageUpdater.unassignUserRole(authorizerName, userName, roleName);
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response unassignRoleFromGroupMapping(String authorizerName, String groupMappingName, String roleName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.unassignGroupMappingRole(authorizerName, groupMappingName, roleName);
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response setRolePermissions(String authorizerName, String roleName, List<ResourceAction> permissions)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    try {
      storageUpdater.setPermissions(authorizerName, roleName, permissions);
      return HttpResponses.OK.empty();
    }
    catch (BasicSecurityDBResourceException cfe) {
      return makeResponseForBasicSecurityDBResourceException(cfe);
    }
  }

  @Override
  public Response getRolePermissions(String authorizerName, String roleName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    return getPermissions(authorizerName, roleName);
  }

  @Override
  public Response getCachedUserMaps(String authorizerName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }

    UserAndRoleMap userAndRoleMap = new UserAndRoleMap(
        storageUpdater.getCachedUserMap(authorizerName),
        storageUpdater.getCachedRoleMap(authorizerName)
    );

    return HttpResponses.OK.entity(userAndRoleMap);
  }

  @Override
  public Response getCachedGroupMappingMaps(String authorizerName)
  {
    final BasicRoleBasedAuthorizer authorizer = authorizerMap.get(authorizerName);
    if (authorizer == null) {
      return makeResponseForAuthorizerNotFound(authorizerName);
    }
    GroupMappingAndRoleMap groupMappingAndRoleMap = new GroupMappingAndRoleMap(
        storageUpdater.getCachedGroupMappingMap(authorizerName),
        storageUpdater.getCachedRoleMap(authorizerName)
    );

    return HttpResponses.OK.entity(groupMappingAndRoleMap);
  }

  @Override
  public Response refreshAll()
  {
    storageUpdater.refreshAllNotification();
    return HttpResponses.OK.empty();
  }

  @Override
  public Response authorizerUserUpdateListener(String authorizerName, byte[] serializedUserAndRoleMap)
  {
    return HttpResponses.NOT_FOUND.empty();
  }

  @Override
  public Response authorizerGroupMappingUpdateListener(String authorizerName, byte[] serializedGroupMappingAndRoleMap)
  {
    return HttpResponses.NOT_FOUND.empty();
  }

  @Override
  public Response getLoadStatus()
  {
    Map<String, Boolean> loadStatus = new HashMap<>();
    authorizerMap.forEach(
        (authorizerName, authorizer) -> {
          loadStatus.put(authorizerName, storageUpdater.getCachedUserMap(authorizerName) != null &&
                                         storageUpdater.getCachedGroupMappingMap(authorizerName) != null &&
                                         storageUpdater.getCachedRoleMap(authorizerName) != null);
        }
    );
    return HttpResponses.OK.entity(loadStatus);
  }

  private static Response makeResponseForAuthorizerNotFound(String authorizerName)
  {
    return HttpResponses.BAD_REQUEST.error("Basic authorizer with name [%s] does not exist.", authorizerName);
  }

  private static Response makeResponseForBasicSecurityDBResourceException(BasicSecurityDBResourceException bsre)
  {
    return HttpResponses.BAD_REQUEST.error(bsre.getMessage());
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
      return HttpResponses.OK.entity(user);
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Response getUserFull(String authorizerName, String userName, boolean simplifyPermissions)
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

      Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
          objectMapper,
          storageUpdater.getCurrentRoleMapBytes(authorizerName)
      );

      if (simplifyPermissions) {
        Set<BasicAuthorizerRoleSimplifiedPermissions> roles = getRolesForUserWithSimplifiedPermissions(user, roleMap);
        BasicAuthorizerUserFullSimplifiedPermissions fullUser = new BasicAuthorizerUserFullSimplifiedPermissions(
            userName,
            roles
        );
        return HttpResponses.OK.entity(fullUser);
      } else {
        Set<BasicAuthorizerRole> roles = getRolesForUser(user, roleMap);
        BasicAuthorizerUserFull fullUser = new BasicAuthorizerUserFull(userName, roles);
        return HttpResponses.OK.entity(fullUser);
      }
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Set<BasicAuthorizerRoleSimplifiedPermissions> getRolesForUserWithSimplifiedPermissions(
      BasicAuthorizerUser user,
      Map<String, BasicAuthorizerRole> roleMap
  )
  {
    Set<BasicAuthorizerRoleSimplifiedPermissions> roles = new HashSet<>();
    for (String roleName : user.getRoles()) {
      BasicAuthorizerRole role = roleMap.get(roleName);
      if (role == null) {
        log.error("User [%s] had role [%s], but role object was not found.", user.getName(), roleName);
      } else {
        BasicAuthorizerRoleSimplifiedPermissions roleWithSimplifiedPermissions = new BasicAuthorizerRoleSimplifiedPermissions(
            role.getName(),
            null,
            BasicAuthorizerRoleSimplifiedPermissions.convertPermissions(role.getPermissions())
        );
        roles.add(roleWithSimplifiedPermissions);
      }
    }
    return roles;
  }

  private Set<BasicAuthorizerRole> getRolesForUser(
      BasicAuthorizerUser user,
      Map<String, BasicAuthorizerRole> roleMap
  )
  {
    Set<BasicAuthorizerRole> roles = new HashSet<>();
    for (String roleName : user.getRoles()) {
      BasicAuthorizerRole role = roleMap.get(roleName);
      if (role == null) {
        log.error("User [%s] had role [%s], but role object was not found.", user.getName(), roleName);
      } else {
        roles.add(role);
      }
    }
    return roles;
  }

  private Response getGroupMappingSimple(String authorizerName, String groupMappingName)
  {
    Map<String, BasicAuthorizerGroupMapping> groupMappings = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        storageUpdater.getCurrentGroupMappingMapBytes(authorizerName)
    );

    try {
      BasicAuthorizerGroupMapping groupMapping = groupMappings.get(groupMappingName);
      if (groupMapping == null) {
        throw new BasicSecurityDBResourceException("Group mapping [%s] does not exist.", groupMappingName);
      }
      return HttpResponses.OK.entity(groupMapping);
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Response getGroupMappingFull(String authorizerName, String groupMappingName)
  {
    Map<String, BasicAuthorizerGroupMapping> groupMappings = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
        objectMapper,
        storageUpdater.getCurrentGroupMappingMapBytes(authorizerName)
    );

    try {
      BasicAuthorizerGroupMapping groupMapping = groupMappings.get(groupMappingName);
      if (groupMapping == null) {
        throw new BasicSecurityDBResourceException("Group mapping [%s] does not exist.", groupMappingName);
      }

      Map<String, BasicAuthorizerRole> roleMap = BasicAuthUtils.deserializeAuthorizerRoleMap(
          objectMapper,
          storageUpdater.getCurrentRoleMapBytes(authorizerName)
      );

      Set<BasicAuthorizerRole> roles = new HashSet<>();
      for (String roleName : groupMapping.getRoles()) {
        BasicAuthorizerRole role = roleMap.get(roleName);
        if (role == null) {
          log.error("Group mapping [%s] had role [%s], but role was not found.", groupMappingName, roleName);
        } else {
          roles.add(role);
        }
      }

      BasicAuthorizerGroupMappingFull fullGroup = new BasicAuthorizerGroupMappingFull(groupMapping.getName(), groupMapping.getGroupPattern(), roles);
      return HttpResponses.OK.entity(fullGroup);
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Response getRoleSimple(String authorizerName, String roleName, boolean simplifyPermissions)
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

      if (simplifyPermissions) {
        return HttpResponses.OK.entity(new BasicAuthorizerRoleSimplifiedPermissions(role, null));
      } else {
        return HttpResponses.OK.entity(role);
      }
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Response getRoleFull(String authorizerName, String roleName, boolean simplifyPermissions)
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

      Map<String, BasicAuthorizerUser> userMap = BasicAuthUtils.deserializeAuthorizerUserMap(
          objectMapper,
          storageUpdater.getCurrentUserMapBytes(authorizerName)
      );

      Map<String, BasicAuthorizerGroupMapping> groupMappingMap = BasicAuthUtils.deserializeAuthorizerGroupMappingMap(
          objectMapper,
          storageUpdater.getCurrentGroupMappingMapBytes(authorizerName)
      );

      Set<String> users = new HashSet<>();
      for (BasicAuthorizerUser user : userMap.values()) {
        if (user.getRoles().contains(roleName)) {
          users.add(user.getName());
        }
      }

      Set<String> groupMappings = new HashSet<>();
      for (BasicAuthorizerGroupMapping group : groupMappingMap.values()) {
        if (group.getRoles().contains(roleName)) {
          groupMappings.add(group.getName());
        }
      }
      if (simplifyPermissions) {
        return HttpResponses.OK.entity(new BasicAuthorizerRoleSimplifiedPermissions(role, users));
      } else {
        BasicAuthorizerRoleFull roleFull = new BasicAuthorizerRoleFull(
            roleName,
            users,
            groupMappings,
            role.getPermissions()
        );
        return HttpResponses.OK.entity(roleFull);
      }
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }

  private Response getPermissions(String authorizerName, String roleName)
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
      return HttpResponses.OK.entity(role.getPermissions());
    }
    catch (BasicSecurityDBResourceException e) {
      return makeResponseForBasicSecurityDBResourceException(e);
    }
  }
}
