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

import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerGroupMapping;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerRole;
import org.apache.druid.security.basic.authorization.entity.BasicAuthorizerUser;

import java.util.Map;

/**
 * This class is reponsible for maintaining a cache of the authorization database state. The BasicRBACAuthorizer
 * uses an injected BasicAuthorizerCacheManager to make its authorization decisions.
 */
public interface BasicAuthorizerCacheManager
{
  /**
   * Update this cache manager's local state with fresh information pushed by the coordinator.
   * @param authorizerPrefix The name of the authorizer this update applies to.
   * @param serializedUserAndRoleMap The updated, serialized user and role maps
   */
  void handleAuthorizerUserUpdate(String authorizerPrefix, byte[] serializedUserAndRoleMap);

  /**
   * Update this cache manager's local state with fresh information pushed by the coordinator.
   * @param authorizerPrefix The name of the authorizer this update applies to.
   * @param serializedGroupMappingAndRoleMap The updated, serialized group and role maps
   * */
  void handleAuthorizerGroupMappingUpdate(String authorizerPrefix, byte[] serializedGroupMappingAndRoleMap);


  /**
   * Return the cache manager's local view of the user map for the authorizer named `authorizerPrefix`.
   *
   * @param authorizerPrefix The name of the authorizer
   * @return User map
   */
  Map<String, BasicAuthorizerUser> getUserMap(String authorizerPrefix);

  /**
   * Return the cache manager's local view of the role map for the authorizer named `authorizerPrefix`.
   *
   * @param authorizerPrefix The name of the authorizer
   * @return Role map
   */
  Map<String, BasicAuthorizerRole> getRoleMap(String authorizerPrefix);

  /**
   * Return the cache manager's local view of the groupMapping map for the authorizer named `authorizerPrefix`.
   *
   * @param authorizerPrefix The name of the authorizer
   * @return GroupMapping map
   */
  Map<String, BasicAuthorizerGroupMapping> getGroupMappingMap(String authorizerPrefix);

  /**
   * Return the cache manager's local view of the groupMapping-role map for the authorizer named `authorizerPrefix`.
   *
   * @param authorizerPrefix The name of the authorizer
   * @return Role map
   */
  Map<String, BasicAuthorizerRole> getGroupMappingRoleMap(String authorizerPrefix);
}
