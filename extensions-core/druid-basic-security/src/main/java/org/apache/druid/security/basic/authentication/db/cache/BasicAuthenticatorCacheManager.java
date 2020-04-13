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

package org.apache.druid.security.basic.authentication.db.cache;

import org.apache.druid.security.basic.authentication.entity.BasicAuthenticatorUser;

import java.util.Map;

/**
 * This class is reponsible for maintaining a cache of the authenticator database state. The BasicHTTPAuthenticator
 * uses an injected BasicAuthenticatorCacheManager to make its authentication decisions.
 */
public interface BasicAuthenticatorCacheManager
{
  /**
   * Update this cache manager's local state of user map with fresh information pushed by the coordinator.
   * @param authenticatorPrefix The name of the authenticator this update applies to.
   * @param serializedUserMap The updated, serialized user map
   */
  void handleAuthenticatorUserMapUpdate(String authenticatorPrefix, byte[] serializedUserMap);

  /**
   * Return the cache manager's local view of the user map for the authenticator named `authenticatorPrefix`.
   *
   * @param authenticatorPrefix The name of the authenticator
   * @return User map
   */
  Map<String, BasicAuthenticatorUser> getUserMap(String authenticatorPrefix);
}
