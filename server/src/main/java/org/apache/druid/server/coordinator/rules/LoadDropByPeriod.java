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

package org.apache.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Period;

import java.util.NavigableSet;

/**
 * Segments not in the loading range will be cleared.
 * One LoadDropByPeriod can only correspond to one tier.
 */
public class LoadDropByPeriod extends PeriodLoadRule
{
  private static final EmittingLogger log = new EmittingLogger(LoadDropByPeriod.class);
  static final String TYPE = "loadDropByPeriod";
  private final String tier;
  private final int replicants;

  @JsonCreator
  public LoadDropByPeriod(
      @JsonProperty("period") Period period,
      @JsonProperty("includeFuture") Boolean includeFuture,
      @JsonProperty("tier") String tier,
      @JsonProperty("replicants") int replicants
  )
  {
    super(period, includeFuture, tier == null ? ImmutableMap.of(DruidServer.DEFAULT_TIER, DruidServer.DEFAULT_NUM_REPLICANTS) : ImmutableMap.of(tier, replicants));
    this.tier = tier == null ? DruidServer.DEFAULT_TIER : tier;
    this.replicants = tier == null ? DruidServer.DEFAULT_NUM_REPLICANTS : replicants;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty
  public String getTier()
  {
    return tier;
  }

  @JsonProperty
  public int getReplicants()
  {
    return replicants;
  }

  @Override
  public void cleanExpireReplicant(
      final DruidCoordinatorRuntimeParams params,
      final DataSegment segment
  )
  {
    try {
      targetReplicants.putAll(getTieredReplicants());
      currentReplicants.putAll(params.getSegmentReplicantLookup().getClusterTiers(segment.getId()));
      final CoordinatorStats stats = new CoordinatorStats();

      final DruidCluster druidCluster = params.getDruidCluster();
      final boolean isLoading = loadingInProgress(druidCluster);

      for (final Object2IntMap.Entry<String> entry : currentReplicants.object2IntEntrySet()) {
        final String tier = entry.getKey();

        final NavigableSet<ServerHolder> holders = druidCluster.getHistoricalsByTier(tier);

        final int numDropped;
        if (holders == null) {
          log.makeAlert("No holders found for tier[%s]", tier).emit();
          numDropped = 0;
        } else {
          final int currentReplicantsInTier = entry.getIntValue();
          if (currentReplicantsInTier > 0) {
            // This enforces that loading is completed before we attempt to drop stuffs as a safety measure.
            if (isLoading) {
              log.info(
                  "Loading in progress for segment [%s], skipping drop from tier [%s] until loading is complete! %s",
                  segment.getId(),
                  tier,
                  getReplicationLogString()
              );
              break;
            }
            numDropped = dropForTier(
                currentReplicantsInTier,
                holders,
                segment,
                params.getBalancerStrategy(),
                getReplicationLogString()
            );
          } else {
            numDropped = 0;
          }
        }

        stats.addToTieredStat(DROPPED_COUNT, tier, numDropped);
      }

    }
    finally {
      targetReplicants.clear();
      currentReplicants.clear();
    }
  }
}
