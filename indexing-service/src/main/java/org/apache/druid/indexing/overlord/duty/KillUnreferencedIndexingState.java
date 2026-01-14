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

package org.apache.druid.indexing.overlord.duty;

import org.apache.druid.indexing.overlord.config.OverlordMetadataCleanupConfig;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.metadata.IndexingStateStorage;
import org.joda.time.DateTime;

import javax.inject.Inject;
import java.util.List;

/**
 * Duty that cleans up unreferenced indexing states from the indexing state storage.
 * <p>
 * The cleanup process involves:
 * <ol>
 *   <li>Marking unreferenced indexing states as unused.</li>
 *   <li>Repairing any unused states that are still referenced by segments.</li>
 *   <li>Deleting unused indexing states older than the configured retention duration.</li>
 *   <li>Deleting any pending indexing states that are older than the configured retention duration.</li>
 * </ol>
 */
public class KillUnreferencedIndexingState extends OverlordMetadataCleanupDuty
{
  private static final Logger log = new Logger(KillUnreferencedIndexingState.class);
  private final IndexingStateStorage indexingStateStorage;

  @Inject
  public KillUnreferencedIndexingState(
      OverlordMetadataCleanupConfig config,
      IndexingStateStorage indexingStateStorage
  )
  {
    super("indexingStates", config);
    this.indexingStateStorage = indexingStateStorage;
  }

  @Override
  protected int cleanupEntriesCreatedBeforeDurationToRetain(DateTime minCreatedTime)
  {
    // 1: Mark unreferenced states as unused
    int unused = indexingStateStorage.markUnreferencedIndexingStatesAsUnused();
    log.info("Marked [%s] unreferenced indexing states as unused.", unused);

    // 2: Repair - find unused states still referenced by segments
    List<String> stateFingerprints = indexingStateStorage.findReferencedIndexingStateMarkedAsUnused();
    if (!stateFingerprints.isEmpty()) {
      int numUpdated = indexingStateStorage.markIndexingStatesAsUsed(stateFingerprints);
      log.info("Marked [%s] unused indexing states referenced by used segments as used.", numUpdated);
    }

    // 3: Delete unused states older than threshold
    return indexingStateStorage.deleteUnusedIndexingStatesOlderThan(minCreatedTime.getMillis());
  }

  @Override
  protected int cleanupEntriesCreatedBeforePendingDurationToRetain(DateTime minCreatedTime)
  {
    return indexingStateStorage.deletePendingIndexingStatesOlderThan(minCreatedTime.getMillis());
  }
}
