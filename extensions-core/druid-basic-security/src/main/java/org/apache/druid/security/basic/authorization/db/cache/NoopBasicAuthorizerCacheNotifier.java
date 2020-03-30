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

/**
 * Noop basic authorizer cache notifier.
 * No notification is sent on user/role udpate.
 * Might be used as a config option to override default authorizer cache notifier.
 */
public class NoopBasicAuthorizerCacheNotifier implements BasicAuthorizerCacheNotifier
{
  /**
   * Send the user map state contained in updatedUserMap to all non-coordinator Druid services
   *
   * @param authorizerPrefix Name of authorizer being updated
   * @param userAndRoleMap   User/role map state
   */
  @Override
  public void addUpdateUser(String authorizerPrefix, byte[] userAndRoleMap)
  {
    // Do nothing as this is a noop implementation
  }

  /**
   * Send the groupMapping map state contained in updatedGroupMappingMap to all non-coordinator Druid services
   *
   * @param authorizerPrefix       Name of authorizer being updated
   * @param groupMappingAndRoleMap Group/role map state
   */
  @Override
  public void addUpdateGroupMapping(String authorizerPrefix, byte[] groupMappingAndRoleMap)
  {
    // Do nothing as this is a noop implementation
  }
}
