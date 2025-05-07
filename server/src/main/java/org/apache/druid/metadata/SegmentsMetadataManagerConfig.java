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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.metadata.segment.cache.SegmentMetadataCache;
import org.joda.time.Period;

/**
 * Config that dictates polling and caching of segment metadata on leader
 * Coordinator or Overlord services.
 */
public class SegmentsMetadataManagerConfig
{
  public static final String CONFIG_PREFIX = "druid.manager.segments";

  @JsonProperty
  private final Period pollDuration;

  @JsonProperty
  private final SegmentMetadataCache.UsageMode useIncrementalCache;

  @JsonCreator
  public SegmentsMetadataManagerConfig(
      @JsonProperty("pollDuration") Period pollDuration,
      @JsonProperty("useIncrementalCache") SegmentMetadataCache.UsageMode useIncrementalCache
  )
  {
    this.pollDuration = Configs.valueOrDefault(pollDuration, Period.minutes(1));
    this.useIncrementalCache = Configs.valueOrDefault(useIncrementalCache, SegmentMetadataCache.UsageMode.NEVER);
  }

  /**
   * Usage mode of the incremental cache.
   */
  public SegmentMetadataCache.UsageMode getCacheUsageMode()
  {
    return useIncrementalCache;
  }

  public Period getPollDuration()
  {
    return pollDuration;
  }
}
