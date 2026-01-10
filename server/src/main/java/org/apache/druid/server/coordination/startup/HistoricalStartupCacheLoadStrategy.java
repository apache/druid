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

package org.apache.druid.server.coordination.startup;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.timeline.DataSegment;

/**
 * Strategy for determining whether segments should be loaded lazily or eagerly during
 * Historical process startup. Lazy loading can help to lower Historical startup time at the
 * expense of query latency, by deferring the loading process to the first access of that segment.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = LoadAllEagerlyStrategy.STRATEGY_NAME, value = LoadAllEagerlyStrategy.class),
    @JsonSubTypes.Type(name = LoadAllLazilyStrategy.STRATEGY_NAME, value = LoadAllLazilyStrategy.class),
    @JsonSubTypes.Type(name = LoadEagerlyBeforePeriod.STRATEGY_NAME, value = LoadEagerlyBeforePeriod.class)
})
public interface HistoricalStartupCacheLoadStrategy
{
  /**
   * Indicates whether the provided segment should be loaded lazily during Historical startup.
   *
   * @param segment the segment being evaluated
   * @return {@code true} if the segment should be loaded lazily, {@code false} if it should be loaded eagerly.
   */
  boolean shouldLoadLazily(DataSegment segment);
}
