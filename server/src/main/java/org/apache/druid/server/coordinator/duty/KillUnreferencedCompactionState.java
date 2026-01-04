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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.metadata.CompactionStateManager;
import org.apache.druid.server.coordinator.config.MetadataCleanupConfig;
import org.apache.druid.server.coordinator.stats.Stats;
import org.joda.time.DateTime;

import java.util.List;

/**
 * Coordinator duty that cleans up old, unused compaction state entries from the database.
 * <p>
 * This duty performs a three-step cleanup process:
 * <ol>
 *   <li>Marks compaction states not referenced by any segments as unused</li>
 *   <li>Repairs any incorrectly marked unused states that are still referenced by used segments</li>
 *   <li>Deletes unused compaction states older than the configured retention period</li>
 * </ol>
 * <p>
 * This prevents unbounded growth of the compaction states table while ensuring that
 * states referenced by active segments are preserved.
 */
public class KillUnreferencedCompactionState extends MetadataCleanupDuty
{
  private static final Logger log = new Logger(KillUnreferencedCompactionState.class);
  private final CompactionStateManager compactionStateManager;

  public KillUnreferencedCompactionState(
      MetadataCleanupConfig config,
      CompactionStateManager compactionStateManager
  )
  {
    super("compactionState", config, Stats.Kill.COMPACTION_STATE);
    this.compactionStateManager = compactionStateManager;
  }

  @Override
  protected int cleanupEntriesCreatedBefore(DateTime minCreatedTime)
  {
    // 1: Mark unreferenced states as unused
    int unused = compactionStateManager.markUnreferencedCompactionStatesAsUnused();
    log.info("Marked [%s] unreferenced compaction states as unused.", unused);

    // 2: Repair - find unused states still referenced by segments
    List<String> stateFingerprints = compactionStateManager.findReferencedCompactionStateMarkedAsUnused();
    if (!stateFingerprints.isEmpty()) {
      int numUpdated = compactionStateManager.markCompactionStatesAsUsed(stateFingerprints);
      log.info("Marked [%s] unused compaction states referenced by used segments as used.", numUpdated);
    }

    // 3: Delete unused states older than threshold
    return compactionStateManager.deleteUnusedCompactionStatesOlderThan(minCreatedTime.getMillis());
  }
}
