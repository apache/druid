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
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;

/**
 * Performs cleanup of stale metadata entries created before a configured retain duration.
 * <p>
 * In every invocation of {@link #run}, the duty checks if the {@code cleanupPeriod}
 * has elapsed since the {@link #lastCleanupTime}. If it has, then the method
 * {@link #cleanupEntriesCreatedBeforeDurationToRetain(DateTime)} is invoked. Otherwise, the duty
 * completes immediately without making any changes.
 */
public abstract class OverlordMetadataCleanupDuty implements OverlordDuty
{
  private static final Logger log = new Logger(OverlordMetadataCleanupDuty.class);

  private final String entryType;
  private final OverlordMetadataCleanupConfig cleanupConfig;

  private DateTime lastCleanupTime = DateTimes.utc(0);

  protected OverlordMetadataCleanupDuty(String entryType, OverlordMetadataCleanupConfig cleanupConfig)
  {
    this.entryType = entryType;
    this.cleanupConfig = cleanupConfig;

    if (cleanupConfig.isCleanupEnabled()) {
      log.debug(
          "Enabled cleanup of [%s] with period [%s] and durationToRetain [%s].",
          entryType, cleanupConfig.getCleanupPeriod(), cleanupConfig.getDurationToRetain()
      );
    }
  }

  @Override
  public void run()
  {
    if (!cleanupConfig.isCleanupEnabled()) {
      return;
    }

    final DateTime now = getCurrentTime();

    // Perform cleanup only if cleanup period has elapsed
    if (lastCleanupTime.plus(cleanupConfig.getCleanupPeriod()).isBefore(now)) {
      lastCleanupTime = now;

      try {
        DateTime minCreatedTime = now.minus(cleanupConfig.getDurationToRetain());
        int deletedEntries = cleanupEntriesCreatedBeforeDurationToRetain(minCreatedTime);
        if (deletedEntries > 0) {
          log.info("Removed [%,d] [%s] created before [%s].", deletedEntries, entryType, minCreatedTime);
        }
        DateTime pendingMinCreatedTime = now.minus(cleanupConfig.getPendingDurationToRetain());
        int deletedPendingEntries = cleanupEntriesCreatedBeforePendingDurationToRetain(pendingMinCreatedTime);
        if (deletedPendingEntries > 0) {
          log.info("Removed [%,d] pending entries [%s] created before [%s].", deletedPendingEntries, entryType, pendingMinCreatedTime);
        }
      }
      catch (Exception e) {
        log.error(e, "Failed to perform cleanup of [%s]", entryType);
      }
    }
  }

  @Override
  public boolean isEnabled()
  {
    return cleanupConfig.isCleanupEnabled();
  }

  @Override
  public DutySchedule getSchedule()
  {
    if (isEnabled()) {
      return new DutySchedule(cleanupConfig.getCleanupPeriod().getMillis(), 0);
    } else {
      return new DutySchedule(0, 0);
    }
  }

  /**
   * Cleans up metadata entries created before the {@code minCreatedTime} calculated with {@link OverlordMetadataCleanupConfig#durationToRetain}.
   * <p>
   * This method is not invoked if the {@code cleanupPeriod} has not elapsed since the {@link #lastCleanupTime}.
   *
   * @return Number of deleted metadata entries
   */
  protected abstract int cleanupEntriesCreatedBeforeDurationToRetain(DateTime minCreatedTime);

  /**
   * Cleans up pending metadata entries created before the {@code minCreatedTime} calculated with {@link OverlordMetadataCleanupConfig#pendingDurationToRetain}.
   * <p>
   * This method is not invoked if the {@code cleanupPeriod} has not elapsed since the {@link #lastCleanupTime}.
   *
   * @return Number of deleted pending metadata entries
   */
  protected abstract int cleanupEntriesCreatedBeforePendingDurationToRetain(DateTime minCreatedTime);

  protected DateTime getCurrentTime()
  {
    return DateTimes.nowUtc();
  }
}
