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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import org.apache.druid.server.coordinator.CoordinatorStats;
import org.apache.druid.server.coordinator.DruidCoordinator;
import org.apache.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import org.apache.druid.server.coordinator.SegmentReplicantLookup;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Map;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "loadByPeriod", value = PeriodLoadRule.class),
    @JsonSubTypes.Type(name = "loadByInterval", value = IntervalLoadRule.class),
    @JsonSubTypes.Type(name = "loadForever", value = ForeverLoadRule.class),
    @JsonSubTypes.Type(name = "dropByPeriod", value = PeriodDropRule.class),
    @JsonSubTypes.Type(name = "dropBeforeByPeriod", value = PeriodDropBeforeRule.class),
    @JsonSubTypes.Type(name = "dropByInterval", value = IntervalDropRule.class),
    @JsonSubTypes.Type(name = "dropForever", value = ForeverDropRule.class),
    @JsonSubTypes.Type(name = ForeverBroadcastDistributionRule.TYPE, value = ForeverBroadcastDistributionRule.class),
    @JsonSubTypes.Type(name = IntervalBroadcastDistributionRule.TYPE, value = IntervalBroadcastDistributionRule.class),
    @JsonSubTypes.Type(name = PeriodBroadcastDistributionRule.TYPE, value = PeriodBroadcastDistributionRule.class)
})
public interface Rule
{
  String getType();

  boolean appliesTo(DataSegment segment, DateTime referenceTimestamp);

  boolean appliesTo(Interval interval, DateTime referenceTimestamp);

  /**
   * Return true if this Rule can load segment onto one or more type of Druid node, otherwise return false.
   * Any Rule that returns true for this method should implement logic for calculating segment under replicated
   * in {@link Rule#updateUnderReplicated}
   */
  boolean canLoadSegments();

  /**
   * This method should update the {@param underReplicatedPerTier} with the replication count of the
   * {@param segment}. Rule that returns true for {@link Rule#canLoadSegments()} must override this method.
   * Note that {@param underReplicatedPerTier} is a map of tier -> { dataSource -> underReplicationCount }
   */
  default void updateUnderReplicated(
      Map<String, Object2LongMap<String>> underReplicatedPerTier,
      SegmentReplicantLookup segmentReplicantLookup,
      DataSegment segment
  )
  {
    Preconditions.checkArgument(!canLoadSegments());
  }

  /**
   * {@link DruidCoordinatorRuntimeParams#getUsedSegments()} must not be called in Rule's code, because the used
   * segments are not specified for the {@link DruidCoordinatorRuntimeParams} passed into Rule's code. This is because
   * {@link DruidCoordinatorRuntimeParams} entangles two slightly different (nonexistent yet) abstractions:
   * "CoordinatorDutyParams" and "RuleParams" which contain params that only {@link
   * org.apache.druid.server.coordinator.duty.CoordinatorDuty} objects and Rules need, respectively. For example,
   * {@link org.apache.druid.server.coordinator.ReplicationThrottler} needs to belong only to "RuleParams", but not to
   * "CoordinatorDutyParams". The opposite for the collection of used segments and {@link
   * org.apache.druid.client.DataSourcesSnapshot}.
   *
   * See https://github.com/apache/druid/issues/7228
   */
  CoordinatorStats run(DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment);
}
