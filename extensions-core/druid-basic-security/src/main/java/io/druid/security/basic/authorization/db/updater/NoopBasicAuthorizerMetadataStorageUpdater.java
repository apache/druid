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

package io.druid.security.basic.authorization.db.updater;

import io.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import io.druid.security.basic.authorization.entity.BasicAuthorizerUser;
import io.druid.server.security.ResourceAction;

import java.util.List;
import java.util.Map;

public class NoopBasicAuthorizerMetadataStorageUpdater implements BasicAuthorizerMetadataStorageUpdater
{
  @Override
  public void createUser(String prefix, String userName)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void deleteUser(String prefix, String userName)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void createRole(String prefix, String roleName)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void deleteRole(String prefix, String roleName)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void assignRole(String prefix, String userName, String roleName)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void unassignRole(String prefix, String userName, String roleName)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public void setPermissions(
      String prefix, String roleName, List<ResourceAction> permissions
  )
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public Map<String, BasicAuthorizerUser> getCachedUserMap(String prefix)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public Map<String, BasicAuthorizerRole> getCachedRoleMap(String prefix)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public Map<String, BasicAuthorizerUser> deserializeUserMap(byte[] userMapBytes)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public Map<String, BasicAuthorizerRole> deserializeRoleMap(byte[] roleMapBytes)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public byte[] getCurrentUserMapBytes(String prefix)
  {
    throw new UnsupportedOperationException("not supported");
  }

  @Override
  public byte[] getCurrentRoleMapBytes(String prefix)
  {
    throw new UnsupportedOperationException("not supported");
  }
}
