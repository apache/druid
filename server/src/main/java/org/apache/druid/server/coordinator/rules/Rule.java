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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;

/**
 * Retention rule that governs retention and distribution of segments in a cluster.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "loadByPeriod", value = PeriodLoadRule.class),
    @JsonSubTypes.Type(name = "loadByInterval", value = IntervalLoadRule.class),
    @JsonSubTypes.Type(name = "loadForever", value = ForeverLoadRule.class),
    @JsonSubTypes.Type(name = PeriodPartialLoadRule.TYPE, value = PeriodPartialLoadRule.class),
    @JsonSubTypes.Type(name = IntervalPartialLoadRule.TYPE, value = IntervalPartialLoadRule.class),
    @JsonSubTypes.Type(name = ForeverPartialLoadRule.TYPE, value = ForeverPartialLoadRule.class),
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
   * @return Whether {@link #appliesTo(DataSegment, DateTime)} is equivalent to {@link #appliesTo(Interval, DateTime)}
   * for this rule, i.e. the rule's applicability decision depends only on the segment's interval and not on any other
   * segment-specific information. Defaults to {@code true}; subclasses that consult the segment beyond its interval
   * (such as {@link PartialLoadRule} which inspects the segment to decide what to load) override to return
   * {@code false}.
   * <p>
   * Callers that only have an interval available can use this to safely use the cheaper interval-only path when every
   * rule in the cascade is interval-based.
   */
  @JsonIgnore
  default boolean isIntervalBased()
  {
    return true;
  }

  void run(DataSegment segment, SegmentActionHandler segmentHandler);
}
