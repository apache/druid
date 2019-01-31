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

package org.apache.druid.security.basic.escalator.db.updater;

import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;

/**
 * Implementations of this interface are responsible for connecting directly to the metadata storage,
 * modifying the escalator database state or reading it. This interface is used by the
 * MetadataStoragePollingBasicEscalatorCacheManager (for reads) and the CoordinatorBasicEscalatorResourceHandler
 * (for handling configuration read/writes).
 */
public interface BasicEscalatorMetadataStorageUpdater
{
  void updateEscalatorCredential(BasicEscalatorCredential escalatorCredential);

  BasicEscalatorCredential getCachedEscalatorCredential();

  byte[] getCachedSerializedEscalatorCredential();

  byte[] getCurrentEscalatorCredentialBytes();

  void refreshAllNotification();

}
