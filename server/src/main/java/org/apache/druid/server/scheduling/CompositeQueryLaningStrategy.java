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

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A laning strategy that allows you to combine multiple {@link QueryLaningStrategy}. To use this strategy,
 * an admin must configure lane groups and their associated strategy.
 *
 * For example:
 * druid.query.scheduler.laning.strategy=composite
 * druid.query.scheduler.laning.strategies={"manualLanes": {"strategy":"manual", "lanes":{"one": 1}},"hiLoLanes": {"strategy":"hilo", "maxLowPercent":1}}
 *
 * This strategy does *not* support nesting a composite strategy within this strategy.
 */
public class CompositeQueryLaningStrategy implements QueryLaningStrategy
{
  @JsonProperty
  private final Map<String, QueryLaningStrategy> strategies;

  @JsonCreator
  public CompositeQueryLaningStrategy(@JsonProperty("strategies") Map<String, QueryLaningStrategy> strategies)
  {
    this.strategies = Preconditions.checkNotNull(strategies, "strategies must be set.");
    Preconditions.checkArgument(!strategies.isEmpty(), "strategies must define at least one strategy.");
    Preconditions.checkArgument(
        strategies.values().stream().noneMatch(s -> s instanceof CompositeQueryLaningStrategy),
        "strategies can not contain a composite strategy."
    );
  }

  @Override
  public Object2IntMap<String> getLaneLimits(int totalLimit)
  {
    Object2IntArrayMap<String> laneLimits = new Object2IntArrayMap<>();
    for (QueryLaningStrategy strategy : strategies.values()) {
      laneLimits.putAll(strategy.getLaneLimits(totalLimit));
    }
    return laneLimits;
  }

  @Override
  public <T> Optional<String> computeLane(QueryPlus<T> query, Set<SegmentServerSelector> segments)
  {
    String laneStrategyKey = QueryContexts.getCompositeLaneStrategy(query.getQuery());
    if (laneStrategyKey == null) {
      return Optional.empty();
    }
    QueryLaningStrategy strategy = strategies.get(laneStrategyKey);
    if (strategy == null) {
      return Optional.empty();
    }
    return strategy.computeLane(query, segments);
  }
}
