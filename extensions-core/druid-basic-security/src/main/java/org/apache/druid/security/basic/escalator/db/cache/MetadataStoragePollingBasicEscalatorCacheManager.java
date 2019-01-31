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

import com.google.inject.Inject;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.basic.escalator.db.updater.BasicEscalatorMetadataStorageUpdater;
import org.apache.druid.security.basic.escalator.entity.BasicEscalatorCredential;

/**
 * Used on coordinator nodes, reading from a BasicEscalatorMetadataStorageUpdater that has direct access to the
 * metadata store.
 */
public class MetadataStoragePollingBasicEscalatorCacheManager implements BasicEscalatorCacheManager
{
  private static final Logger log = new Logger(MetadataStoragePollingBasicEscalatorCacheManager.class);

  private final BasicEscalatorMetadataStorageUpdater storageUpdater;

  @Inject
  public MetadataStoragePollingBasicEscalatorCacheManager(
      BasicEscalatorMetadataStorageUpdater storageUpdater
  )
  {
    this.storageUpdater = storageUpdater;

    log.info("Starting coordinator basic escalator cache manager.");
  }

  @Override
  public void handleEscalatorCredentialUpdate(byte[] serializedEscalatorCredentialConfig)
  {
  }

  @Override
  public BasicEscalatorCredential getEscalatorCredential()
  {
    return storageUpdater.getCachedEscalatorCredential();
  }
}
