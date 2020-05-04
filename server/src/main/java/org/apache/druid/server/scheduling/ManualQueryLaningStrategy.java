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
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.server.QueryLaningStrategy;
import org.apache.druid.server.QueryScheduler;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ManualQueryLaningStrategy implements QueryLaningStrategy
{
  @JsonProperty
  private Map<String, Integer> lanes;

  @JsonProperty
  private boolean isLimitPercent;

  @JsonCreator
  public ManualQueryLaningStrategy(
      @JsonProperty("lanes") Map<String, Integer> lanes,
      @JsonProperty("isLimitPercent") @Nullable Boolean isLimitPercent
  )
  {
    this.lanes = Preconditions.checkNotNull(lanes, "lanes must be set");
    this.isLimitPercent = isLimitPercent != null ? isLimitPercent : false;
    Preconditions.checkArgument(lanes.size() > 0, "lanes must define at least one lane");
    Preconditions.checkArgument(
        lanes.values().stream().allMatch(x -> this.isLimitPercent ? 0 < x && x <= 100 : x > 0),
        this.isLimitPercent ? "All lane limits must be in the range 1 to 100" : "All lane limits must be greater than 0"
    );
    Preconditions.checkArgument(
        lanes.keySet().stream().noneMatch(QueryScheduler.TOTAL::equals),
        "Lane cannot be named 'total'"
    );

    // 'default' has special meaning for resilience4j bulkhead used by query scheduler, this restriction
    // can potentially be relaxed if we ever change enforcement mechanism
    Preconditions.checkArgument(
        lanes.keySet().stream().noneMatch("default"::equals),
        "Lane cannot be named 'default'"
    );
  }

  @Override
  public Object2IntMap<String> getLaneLimits(int totalLimit)
  {

    if (isLimitPercent) {
      Object2IntMap<String> laneLimits = new Object2IntArrayMap<>(lanes.size());
      lanes.forEach((key, value) -> laneLimits.put(key, computeLimitFromPercent(totalLimit, value)));
      return laneLimits;
    }
    return new Object2IntArrayMap<>(lanes);
  }

  @Override
  public <T> Optional<String> computeLane(QueryPlus<T> query, Set<SegmentServerSelector> segments)
  {
    return Optional.ofNullable(QueryContexts.getLane(query.getQuery()));
  }
}
