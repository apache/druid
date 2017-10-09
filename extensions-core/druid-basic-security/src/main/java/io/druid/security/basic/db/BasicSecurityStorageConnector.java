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

import java.util.List;
import java.util.Map;

public interface BasicSecurityStorageConnector
{
  void createUser(String userName);

  void deleteUser(String userName);

  void createRole(String roleName);

  void deleteRole(String roleName);

  void addPermission(String roleName, byte[] serializedResourceIdentifier, String action);

  void deleteAllPermissionsFromRole(String roleName);

  void deletePermission(int permissionId);

  void assignRole(String userName, String roleName);

  void unassignRole(String userName, String roleName);

  List<Map<String, Object>> getAllUsers();

  List<Map<String, Object>> getAllRoles();

  Map<String, Object> getUser(String userName);

  Map<String, Object> getRole(String roleName);

  List<Map<String, Object>> getRolesForUser(String userName);

  List<Map<String, Object>> getUsersWithRole(String roleName);

  List<Map<String, Object>> getPermissionsForRole(String roleName);

  List<Map<String, Object>> getPermissionsForUser(String userName);

  void createRoleTable();

  void createUserTable();

  void createPermissionTable();

  void createUserRoleTable();

  void createUserCredentialsTable();

  void createAuthenticationToAuthorizationNameMappingTable();

  void deleteAllRecords(String tableName);


  void createAuthenticationToAuthorizationNameMapping(String authenticationName, String authorizationName);

  String getAuthorizationNameFromAuthenticationName(String authenticationName);

  void deleteAuthenticationToAuthorizationNameMapping(String authenticationName);


  void setUserCredentials(String userName, char[] password);

  boolean checkCredentials(String userName, char[] password);

  Map<String, Object> getUserCredentials(String userName);
}
