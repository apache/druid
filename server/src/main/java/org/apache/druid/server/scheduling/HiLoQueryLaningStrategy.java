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

package org.apache.druid.server.scheduling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.client.SegmentServerSelector;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.QueryLaningStrategy;

import java.util.Optional;
import java.util.Set;

/**
 * Query laning strategy which associates all {@link Query} with priority lower than 0 into a 'low' lane
 */
public class HiLoQueryLaningStrategy implements QueryLaningStrategy
{
  public static final String LOW = "low";

  @JsonProperty
  private final int maxLowPercent;

  @JsonCreator
  public HiLoQueryLaningStrategy(
      @JsonProperty("maxLowPercent") Integer maxLowPercent
  )
  {
    this.maxLowPercent = Preconditions.checkNotNull(maxLowPercent, "maxLowPercent must be set");
    Preconditions.checkArgument(
        0 < maxLowPercent && maxLowPercent <= 100,
        "maxLowPercent must be in the range 1 to 100"
    );
  }

  @Override
  public Object2IntMap<String> getLaneLimits(int totalLimit)
  {
    Object2IntMap<String> onlyLow = new Object2IntArrayMap<>(1);
    onlyLow.put(LOW, computeLimitFromPercent(totalLimit, maxLowPercent));
    return onlyLow;
  }

  @Override
  public <T> Optional<String> computeLane(QueryPlus<T> query, Set<SegmentServerSelector> segments)
  {
    final Query<T> theQuery = query.getQuery();
    // QueryContexts.getPriority gives a default, since we are setting priority
    final Integer priority = theQuery.getContextValue(QueryContexts.PRIORITY_KEY);
    final String lane = theQuery.getContextValue(QueryContexts.LANE_KEY);
    if (lane == null && priority != null && priority < 0) {
      return Optional.of(LOW);
    }
    return Optional.ofNullable(lane);
  }
}
