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
import org.joda.time.Duration;
import org.joda.time.Period;

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

  @JsonProperty
  private UnavailableSegmentPolicy unavailableSegmentPolicy = UnavailableSegmentPolicy.ALERT;

  @JsonProperty
  private Period unavailableRetentionPeriod = new Period("PT15M");

  @JsonProperty
  private Period unavailableCheckPeriod = new Period("PT5S");

  @JsonProperty
  private int maxUnavailableSegments = 1_000_000;

  public Set<String> getWatchedTiers()
  {
    return watchedTiers;
  }

  /**
   * What to do when a query touches a segment that should be available but has no server.
   */
  public UnavailableSegmentPolicy getUnavailableSegmentPolicy()
  {
    return unavailableSegmentPolicy;
  }

  /**
   * How long a segment with no server is kept in the timeline while the Broker cannot establish whether it should be
   * available. Bounds how long an unreachable Coordinator can keep segments pinned.
   */
  public Duration getUnavailableRetentionPeriod()
  {
    return unavailableRetentionPeriod.toStandardDuration();
  }

  /**
   * How often the Broker asks the Coordinator about the segments it has no server for. Also how quickly a segment
   * that has since been marked unused stops being reported.
   */
  public Duration getUnavailableCheckPeriod()
  {
    return unavailableCheckPeriod.toStandardDuration();
  }

  /**
   * Cap on how many segments with no server the Broker tracks at once. Beyond this it stops tracking and reverts to
   * dropping them from the timeline, so that a large-scale outage cannot grow the set without bound.
   */
  public int getMaxUnavailableSegments()
  {
    return maxUnavailableSegments;
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
}
