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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;

/**
 * Performs cleanup of stale metadata entries created before a configured retain duration.
 * <p>
 * In every invocation of {@link #run}, the duty checks if the {@link #cleanupPeriod}
 * has elapsed since the {@link #lastCleanupTime}. If it has, then the method
 * {@link #cleanupEntriesCreatedBefore(DateTime)} is invoked. Otherwise, the duty
 * completes immediately without making any changes.
 */
public abstract class MetadataCleanupDuty implements CoordinatorDuty
{
  private static final Logger log = new Logger(MetadataCleanupDuty.class);

  private final String propertyPrefix;
  private final String entryType;
  private final CoordinatorStat cleanupCountStat;

  private final boolean cleanupEnabled;
  private final Duration cleanupPeriod;
  private final Duration retainDuration;

  private DateTime lastCleanupTime = DateTimes.utc(0);

  protected MetadataCleanupDuty(
      String entryType,
      String propertyPrefix,
      boolean cleanupEnabled,
      Duration cleanupPeriod,
      Duration retainDuration,
      CoordinatorStat cleanupCountStat,
      DruidCoordinatorConfig coordinatorConfig
  )
  {
    this.propertyPrefix = propertyPrefix;
    this.entryType = entryType;
    this.cleanupEnabled = cleanupEnabled;
    this.cleanupPeriod = cleanupPeriod;
    this.retainDuration = retainDuration;
    this.cleanupCountStat = cleanupCountStat;

    if (this.cleanupEnabled) {
      validatePeriod(cleanupPeriod, coordinatorConfig.getCoordinatorMetadataStoreManagementPeriod());
      validateRetainDuration(retainDuration);

      log.debug(
          "Enabled cleanup of [%s] with period [%s] and durationToRetain [%s].",
          entryType, cleanupPeriod, retainDuration
      );
    }
  }

  @Nullable
  @Override
  public DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params)
  {
    if (!cleanupEnabled) {
      return params;
    }

    final DateTime now = getCurrentTime();

    // Perform cleanup only if cleanup period has elapsed
    if (lastCleanupTime.plus(cleanupPeriod).isBefore(now)) {
      lastCleanupTime = now;

      try {
        DateTime minCreatedTime = now.minus(retainDuration);
        int deletedEntries = cleanupEntriesCreatedBefore(minCreatedTime);
        log.info("Removed [%,d] [%s] created before [%s].", deletedEntries, entryType, minCreatedTime);

        params.getCoordinatorStats().add(cleanupCountStat, deletedEntries);
      }
      catch (Exception e) {
        log.error(e, "Failed to perform cleanup of [%s]", entryType);
      }
    }

    return params;
  }

  /**
   * Cleans up metadata entries created before the {@code minCreatedTime}.
   * <p>
   * This method is not invoked if the {@link #cleanupPeriod} has not elapsed
   * since the {@link #lastCleanupTime}.
   *
   * @return Number of deleted metadata entries
   */
  protected abstract int cleanupEntriesCreatedBefore(DateTime minCreatedTime);

  private void validatePeriod(Duration period, Duration metadataManagementPeriod)
  {
    Preconditions.checkArgument(
        period != null && period.getMillis() >= metadataManagementPeriod.getMillis(),
        "[%s.period] must be greater than [druid.coordinator.period.metadataStoreManagementPeriod]",
        propertyPrefix
    );
  }

  private void validateRetainDuration(Duration retainDuration)
  {
    Preconditions.checkArgument(
        retainDuration != null && retainDuration.getMillis() >= 0,
        "[%s.durationToRetain] must be 0 milliseconds or higher",
        propertyPrefix
    );
    Preconditions.checkArgument(
        retainDuration.getMillis() < System.currentTimeMillis(),
        "[%s.durationToRetain] cannot be greater than current time in milliseconds",
        propertyPrefix
    );
  }

  protected DateTime getCurrentTime()
  {
    return DateTimes.nowUtc();
  }
}
