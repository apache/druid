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

package org.apache.druid.security.basic.authorization.db.cache;

import com.google.inject.Inject;
import org.apache.druid.security.basic.authorization.db.updater.BasicAuthorizerMetadataStorageUpdater;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;

import java.util.Map;

public class MetadataStoragePollingBasicAuthorizerCacheManager implements BasicAuthorizerCacheManager
{
  private final BasicAuthorizerMetadataStorageUpdater storageUpdater;

  @Inject
  public MetadataStoragePollingBasicAuthorizerCacheManager(
      BasicAuthorizerMetadataStorageUpdater storageUpdater
  )
  {
    this.storageUpdater = storageUpdater;
  }

  @Override
  public void handleAuthorizerUserUpdate(String authorizerPrefix, byte[] serializedUserAndRoleMap)
  {

  }

  @Override
  public void handleAuthorizerGroupMappingUpdate(String authorizerPrefix, byte[] serializedGroupMappingAndRoleMap)
  {

  }

  @Override
  public Map<String, BasicAuthorizerUser> getUserMap(String authorizerPrefix)
  {
    return storageUpdater.getCachedUserMap(authorizerPrefix);
  }

  @Override
  public Map<String, BasicAuthorizerRole> getRoleMap(String authorizerPrefix)
  {
    return storageUpdater.getCachedRoleMap(authorizerPrefix);
  }

  @Override
  public Map<String, BasicAuthorizerGroupMapping> getGroupMappingMap(String authorizerPrefix)
  {
    return storageUpdater.getCachedGroupMappingMap(authorizerPrefix);
  }

  @Override
  public Map<String, BasicAuthorizerRole> getGroupMappingRoleMap(String authorizerPrefix)
  {
    return getRoleMap(authorizerPrefix);
  }
}
