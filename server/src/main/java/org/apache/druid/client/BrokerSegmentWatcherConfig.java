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

package org.apache.druid.client;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Set;

/**
 */
public class BrokerSegmentWatcherConfig
{
  @JsonProperty
  private Set<String> watchedTiers = null;

  @JsonProperty
  private Set<String> ignoredTiers = null;

  @JsonProperty
  private Set<String> watchedDataSources = null;

  @JsonProperty
  private boolean watchRealtimeTasks = true;

  @JsonProperty
  private boolean awaitInitializationOnStart = true;

  /**
   * Delay in milliseconds before removing a segment from the broker timeline after the last server
   * drops it. This helps prevent the segment load/drop race condition described in
   * <a href="https://github.com/apache/druid/issues/18738">#18738</a> where a segment moved from
   * one historical to another can temporarily disappear from the broker timeline, causing silent
   * partial query results.
   * <p>
   * Set to 0 to disable the delay (immediate removal, matching the pre-fix behavior).
   * Default is 30000 (30 seconds).
   */
  @JsonProperty
  private long segmentDropDelayMillis = 30_000;

  public Set<String> getWatchedTiers()
  {
    return watchedTiers;
  }

  public Set<String> getIgnoredTiers()
  {
    return ignoredTiers;
  }

  public Set<String> getWatchedDataSources()
  {
    return watchedDataSources;
  }

  public boolean isWatchRealtimeTasks()
  {
    return watchRealtimeTasks;
  }

  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
  }

  public long getSegmentDropDelayMillis()
  {
    return segmentDropDelayMillis;
  }
}
