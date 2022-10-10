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

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.SegmentLoader;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.timeline.DataSegment;

import java.util.Map;

public abstract class BroadcastDistributionRule implements Rule
{

  @Override
  public void run(DataSegment segment, SegmentLoader loader)
  {
    loader.broadcastSegment(segment);
  }

  @Override
  public boolean canLoadSegments()
  {
    return true;
  }

  @Override
  public void updateUnderReplicated(
      Map<String, Object2LongMap<String>> underReplicatedPerTier,
      SegmentReplicantLookup segmentReplicantLookup,
      DataSegment segment
  )
  {
    Object2LongMap<String> underReplicatedBroadcastTiers =
        segmentReplicantLookup.getBroadcastUnderReplication(segment.getId());
    for (final Object2LongMap.Entry<String> entry : underReplicatedBroadcastTiers.object2LongEntrySet()) {
      final String tier = entry.getKey();
      final long underReplicatedCount = entry.getLongValue();
      underReplicatedPerTier.compute(tier, (_tier, existing) -> {
        Object2LongMap<String> underReplicationPerDataSource = existing;
        if (existing == null) {
          underReplicationPerDataSource = new Object2LongOpenHashMap<>();
        }
        underReplicationPerDataSource.compute(
            segment.getDataSource(),
            (_datasource, count) -> count != null ? count + underReplicatedCount : underReplicatedCount
        );
        return underReplicationPerDataSource;
      });
    }
  }

  @Override
  public void updateUnderReplicatedWithClusterView(
      Map<String, Object2LongMap<String>> underReplicatedPerTier,
      SegmentReplicantLookup segmentReplicantLookup,
      DruidCluster cluster,
      DataSegment segment
  )
  {
    updateUnderReplicated(
        underReplicatedPerTier,
        segmentReplicantLookup,
        segment
    );
  }

}
