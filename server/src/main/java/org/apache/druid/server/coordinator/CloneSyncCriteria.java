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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.InvalidInput;

import javax.annotation.Nullable;

/**
 * Criteria definining when a clone historical should be considered as "synced"
 * to its source server. The criteria is a function of the number or percentage
 * of segments "pending sync", i.e. segments already loaded on the source server
 * but not present on the target server. Segments which are yet to be loaded on
 * the source server itself do not affect the sync status.
 */
public class CloneSyncCriteria
{
  private final int maxSegmentsPendingSync;
  private final double maxPercentPendingSync;

  @JsonCreator
  public CloneSyncCriteria(
      @JsonProperty("maxSegmentsPendingSync") @Nullable Integer maxSegmentsPendingSync,
      @JsonProperty("maxPercentPendingSync") @Nullable Double maxPercentPendingSync
  )
  {
    this.maxSegmentsPendingSync = Configs.valueOrDefault(maxSegmentsPendingSync, 100);
    this.maxPercentPendingSync = Configs.valueOrDefault(maxPercentPendingSync, 1.0f);

    InvalidInput.conditionalException(
        this.maxSegmentsPendingSync >= 0,
        "'maxSegmentsPendingSync' must be greater than or equal to 0"
    );
    InvalidInput.conditionalException(
        this.maxPercentPendingSync >= 0.0 && this.maxPercentPendingSync <= 100.0,
        "'maxPercentPendingSync' must be in the range [0.0, 100.0]"
    );
  }

  /**
   * For a clone to be considered SYNCED, the number of segments pending sync
   * must be less than or equal to this value.
   */
  @JsonProperty
  public int getMaxSegmentsPendingSync()
  {
    return maxSegmentsPendingSync;
  }

  /**
   * For a clone to be considered SYNCED, the percentage of segments pending sync
   * must be less than or equal to this value.
   */
  @JsonProperty
  public double getMaxPercentPendingSync()
  {
    return maxPercentPendingSync;
  }
}
