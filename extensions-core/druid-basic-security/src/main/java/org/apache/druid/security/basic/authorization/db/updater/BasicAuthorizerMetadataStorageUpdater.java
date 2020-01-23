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

package org.apache.druid.security.basic.authorization.db.updater;

import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import org.apache.druid.server.security.ResourceAction;

import java.util.List;
import java.util.Map;

/**
 * Implementations of this interface are responsible for connecting directly to the metadata storage,
 * modifying the authorizer database state or reading it. This interface is used by the
 * MetadataStoragePollingBasicAuthorizerCacheManager (for reads) and the CoordinatorBasicAuthorizerResourceHandler
 * (for handling configuration read/writes).
 */
public interface BasicAuthorizerMetadataStorageUpdater
{
  void createUser(String prefix, String userName);

  void deleteUser(String prefix, String userName);

  void createGroupMapping(String prefix, BasicAuthorizerGroupMapping groupMapping);

  void deleteGroupMapping(String prefix, String groupMappingName);

  void createRole(String prefix, String roleName);

  void deleteRole(String prefix, String roleName);

  void assignUserRole(String prefix, String userName, String roleName);

  void unassignUserRole(String prefix, String userName, String roleName);

  void assignGroupMappingRole(String prefix, String groupMappingName, String roleName);

  void unassignGroupMappingRole(String prefix, String groupMappingName, String roleName);

  void setPermissions(String prefix, String roleName, List<ResourceAction> permissions);

  Map<String, BasicAuthorizerUser> getCachedUserMap(String prefix);

  Map<String, BasicAuthorizerGroupMapping> getCachedGroupMappingMap(String prefix);

  Map<String, BasicAuthorizerRole> getCachedRoleMap(String prefix);

  byte[] getCurrentUserMapBytes(String prefix);

  byte[] getCurrentGroupMappingMapBytes(String prefix);

  byte[] getCurrentRoleMapBytes(String prefix);

  void refreshAllNotification();
}
