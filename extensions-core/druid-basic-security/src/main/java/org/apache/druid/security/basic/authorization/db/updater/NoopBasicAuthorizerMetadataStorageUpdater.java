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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Empty implementation of {@link BasicAuthorizerMetadataStorageUpdater}.
 * Void methods do nothing, other return empty maps or empty arrays depending on the return type.
 */
public class NoopBasicAuthorizerMetadataStorageUpdater implements BasicAuthorizerMetadataStorageUpdater
{
  @Override
  public void createUser(String prefix, String userName)
  {
  }

  @Override
  public void deleteUser(String prefix, String userName)
  {
  }

  @Override
  public void createGroupMapping(String prefix, BasicAuthorizerGroupMapping groupMapping)
  {
  }

  @Override
  public void deleteGroupMapping(String prefix, String groupMappingName)
  {
  }

  @Override
  public void createRole(String prefix, String roleName)
  {
  }

  @Override
  public void deleteRole(String prefix, String roleName)
  {
  }

  @Override
  public void assignUserRole(String prefix, String userName, String roleName)
  {
  }

  @Override
  public void unassignUserRole(String prefix, String userName, String roleName)
  {
  }

  @Override
  public void assignGroupMappingRole(String prefix, String groupMappingName, String roleName)
  {
  }

  @Override
  public void unassignGroupMappingRole(String prefix, String groupMappingName, String roleName)
  {
  }

  @Override
  public void setPermissions(String prefix, String roleName, List<ResourceAction> permissions)
  {
  }

  @Override
  public Map<String, BasicAuthorizerUser> getCachedUserMap(String prefix)
  {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, BasicAuthorizerGroupMapping> getCachedGroupMappingMap(String prefix)
  {
    return Collections.emptyMap();
  }

  @Override
  public Map<String, BasicAuthorizerRole> getCachedRoleMap(String prefix)
  {
    return Collections.emptyMap();
  }

  @Override
  public byte[] getCurrentUserMapBytes(String prefix)
  {
    return new byte[0];
  }

  @Override
  public byte[] getCurrentRoleMapBytes(String prefix)
  {
    return new byte[0];
  }

  @Override
  public byte[] getCurrentGroupMappingMapBytes(String prefix)
  {
    return new byte[0];
  }

  @Override
  public void refreshAllNotification()
  {
  }
}
