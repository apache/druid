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

import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.server.security.ResourceAction;

import javax.ws.rs.core.Response;
import java.util.List;

/**
 * Handles authorizer-related API calls. Coordinator and non-coordinator methods are combined here because of an
 * inability to selectively inject jetty resources in configure(Binder binder) of the extension module based
 * on node type.
 */
public interface BasicAuthorizerResourceHandler
{
  // coordinator methods
  Response getAllUsers(String authorizerName);

  Response getAllGroupMappings(String authorizerName);

  Response getUser(String authorizerName, String userName, boolean isFull, boolean simplifyPermissions);

  Response getGroupMapping(String authorizerName, String groupMappingName, boolean isFull);

  Response createUser(String authorizerName, String userName);

  Response createGroupMapping(String authorizerName, BasicAuthorizerGroupMapping groupMapping);

  Response deleteUser(String authorizerName, String userName);

  Response deleteGroupMapping(String authorizerName, String groupMappingName);

  Response getAllRoles(String authorizerName);

  Response getRole(String authorizerName, String roleName, boolean isFull, boolean simplifyPermissions);

  Response createRole(String authorizerName, String roleName);

  Response deleteRole(String authorizerName, String roleName);

  Response assignRoleToUser(String authorizerName, String userName, String roleName);

  Response assignRoleToGroupMapping(String authorizerName, String groupMappingName, String roleName);

  Response unassignRoleFromUser(String authorizerName, String userName, String roleName);

  Response unassignRoleFromGroupMapping(String authorizerName, String groupMappingName, String roleName);

  Response setRolePermissions(String authorizerName, String roleName, List<ResourceAction> permissions);

  Response getRolePermissions(String authorizerName, String roleName);

  Response getCachedUserMaps(String authorizerName);

  Response getCachedGroupMappingMaps(String authorizerName);

  Response refreshAll();

  // non-coordinator methods
  Response authorizerUserUpdateListener(String authorizerName, byte[] serializedUserAndRoleMap);

  Response authorizerGroupMappingUpdateListener(String authorizerName, byte[] serializedGroupMappingAndRoleMap);

  // common
  Response getLoadStatus();
}
