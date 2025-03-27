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
import org.joda.time.Period;

/**
 * Config that dictates polling and caching of segment metadata on leader
 * Coordinator or Overlord services.
 */
public class SegmentsMetadataManagerConfig
{
  public static final String CONFIG_PREFIX = "druid.manager.segments";

  /**
   * Cache usage modes.
   */
  public enum UseCache
  {
    /**
     * Always read from the cache. Service start-up may be blocked until cache
     * has synced with the metadata store at least once. Transactions may block
     * until cache has synced with the metadata store at least once after
     * becoming leader.
     */
    ALWAYS,

    /**
     * Cache is disabled.
     */
    NEVER,

    /**
     * Read from the cache only if it is already synced with the metadata store.
     * Does not block service start-up or transactions. Writes may still go to
     * cache to reduce sync times.
     */
    IF_SYNCED
  }

  @JsonProperty
  private final Period pollDuration;

  @JsonProperty
  private final UseCache useCache;

  @JsonCreator
  public SegmentsMetadataManagerConfig(
      @JsonProperty("pollDuration") Period pollDuration,
      @JsonProperty("useCache") UseCache useCache
  )
  {
    this.pollDuration = Configs.valueOrDefault(pollDuration, Period.minutes(1));
    this.useCache = Configs.valueOrDefault(useCache, UseCache.NEVER);
  }

  public UseCache getUseCache()
  {
    return useCache;
  }

  public Period getPollDuration()
  {
    return pollDuration;
  }
}
