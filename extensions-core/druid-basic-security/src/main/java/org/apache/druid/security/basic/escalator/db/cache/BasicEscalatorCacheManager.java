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

package org.apache.druid.security.basic.escalator.db.cache;

import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;

/**
 * This class is reponsible for maintaining a cache of the escalator database state. The BasicHTTPEscalator
 * uses an injected BasicEscalatorCacheManager to set escalator credentials.
 */
public interface BasicEscalatorCacheManager
{
  /**
   * Update this cache manager's local state of escalator credential with fresh information pushed by the coordinator.
   *
   * @param serializedEscalatorCredentialConfig The updated, serialized escalator credential
   */
  void handleEscalatorCredentialUpdate(byte[] serializedEscalatorCredentialConfig);

  /**
   * Return the cache manager's local view of escalator credential.
   *
   * @return Escalator credential
   */
  BasicEscalatorCredential getEscalatorCredential();
}
