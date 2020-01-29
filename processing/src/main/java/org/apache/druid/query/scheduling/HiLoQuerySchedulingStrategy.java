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

package org.apache.druid.query.scheduling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import it.unimi.dsi.fastutil.objects.Object2IntArrayMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QuerySchedulingStrategy;
import org.apache.druid.query.SegmentDescriptor;

import java.util.Set;

public class HiLoQuerySchedulingStrategy implements QuerySchedulingStrategy
{
  @JsonProperty
  private int maxLowThreads;

  @JsonCreator
  public HiLoQuerySchedulingStrategy(
      @JsonProperty("maxLowThreads") Integer maxLowThreads
  )
  {
    this.maxLowThreads = maxLowThreads;
  }

  @Override
  public Object2IntMap<String> getLaneLimits()
  {
    Object2IntMap<String> onlyLow = new Object2IntArrayMap<>(1);
    onlyLow.put("low", maxLowThreads);
    return onlyLow;
  }

  @Override
  public <T> Query<T> prioritizeQuery(QueryPlus<T> query, Set<SegmentDescriptor> segments)
  {
    final Query<T> theQuery = query.getQuery();
    final Integer priority = theQuery.getContextValue("priority");
    final String lane = theQuery.getContextValue("queryLane");
    if (lane == null && priority != null && priority < 0) {
      return theQuery.withOverriddenContext(ImmutableMap.<String, Object>builder().putAll(theQuery.getContext()).put("queryLane", "low").build());
    }
    return theQuery;
  }
}
