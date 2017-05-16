/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.server.coordinator.CoordinatorStats;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "loadByPeriod", value = PeriodLoadRule.class),
    @JsonSubTypes.Type(name = "loadByInterval", value = IntervalLoadRule.class),
    @JsonSubTypes.Type(name = "loadForever", value = ForeverLoadRule.class),
    @JsonSubTypes.Type(name = "dropByPeriod", value = PeriodDropRule.class),
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

  CoordinatorStats run(DruidCoordinator coordinator, DruidCoordinatorRuntimeParams params, DataSegment segment);
}
