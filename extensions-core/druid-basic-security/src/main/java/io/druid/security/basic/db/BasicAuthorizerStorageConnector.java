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

package io.druid.security.basic.db;

import io.druid.server.security.ResourceAction;

import java.util.List;
import java.util.Map;

public interface BasicAuthorizerStorageConnector
{
  void createUser(String dbPrefix, String userName);

  void deleteUser(String dbPrefix, String userName);

  void createRole(String dbPrefix, String roleName);

  void deleteRole(String dbPrefix, String roleName);

  void addPermission(String dbPrefix, String roleName, ResourceAction resourceAction);

  void deletePermission(String dbPrefix, int permissionId);

  void assignRole(String dbPrefix, String userName, String roleName);

  void unassignRole(String dbPrefix, String userName, String roleName);

  List<Map<String, Object>> getAllUsers(String dbPrefix);

  List<Map<String, Object>> getAllRoles(String dbPrefix);

  Map<String, Object> getUser(String dbPrefix, String userName);

  Map<String, Object> getRole(String dbPrefix, String roleName);

  List<Map<String, Object>> getRolesForUser(String dbPrefix, String userName);

  List<Map<String, Object>> getUsersWithRole(String dbPrefix, String roleName);

  List<Map<String, Object>> getPermissionsForRole(String dbPrefix, String roleName);

  List<Map<String, Object>> getPermissionsForUser(String dbPrefix, String userName);

  void createRoleTable(String dbPrefix);

  void createUserTable(String dbPrefix);

  void createPermissionTable(String dbPrefix);

  void createUserRoleTable(String dbPrefix);
}
