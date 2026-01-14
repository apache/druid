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
import org.apache.druid.segment.metadata.CompactionStateStorage;
import org.joda.time.DateTime;

import javax.inject.Inject;
import java.util.List;

public class KillUnreferencedIndexingState extends OverlordMetadataCleanupDuty
{
  private static final Logger log = new Logger(KillUnreferencedIndexingState.class);
  private final CompactionStateStorage compactionStateStorage;

  @Inject
  public KillUnreferencedIndexingState(
      OverlordMetadataCleanupConfig config,
      CompactionStateStorage compactionStateStorage
  )
  {
    super("indexingStates", config);
    this.compactionStateStorage = compactionStateStorage;
  }

  @Override
  protected int cleanupEntriesCreatedBeforeDurationToRetain(DateTime minCreatedTime)
  {
    // 1: Mark unreferenced states as unused
    int unused = compactionStateStorage.markUnreferencedCompactionStatesAsUnused();
    log.info("Marked [%s] unreferenced indexing states as unused.", unused);

    // 2: Repair - find unused states still referenced by segments
    List<String> stateFingerprints = compactionStateStorage.findReferencedCompactionStateMarkedAsUnused();
    if (!stateFingerprints.isEmpty()) {
      int numUpdated = compactionStateStorage.markCompactionStatesAsUsed(stateFingerprints);
      log.info("Marked [%s] unused indexing states referenced by used segments as used.", numUpdated);
    }

    // 3: Delete unused states older than threshold
    return compactionStateStorage.deleteUnusedCompactionStatesOlderThan(minCreatedTime.getMillis());
  }

  @Override
  protected int cleanupEntriesCreatedBeforePendingDurationToRetain(DateTime minCreatedTime)
  {
    return compactionStateStorage.deletePendingCompactionStatesOlderThan(minCreatedTime.getMillis());
  }
}
