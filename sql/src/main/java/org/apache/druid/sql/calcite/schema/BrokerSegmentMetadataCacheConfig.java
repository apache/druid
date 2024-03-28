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

package org.apache.druid.sql.calcite.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.segment.metadata.SegmentMetadataCacheConfig;
import org.joda.time.Period;

/**
 * Broker-side configuration class for managing segment polling from the Coordinator and
 * customizing properties related to the SegmentMetadata cache which is used to infer datasources for SQL.
 *
 * <p>See {@link BrokerSegmentMetadataCache}, {@link MetadataSegmentView}.</p>
 *
 * <p>This class shares the same config root as {@link org.apache.druid.sql.calcite.planner.PlannerConfig}
 * to maintain backward compatibility for when the properties here resided in {@code PlannerConfig}.</p>
 *
 * <p> The property {@link #awaitInitializationOnStart} is overridden in this class with a default value
 * of {@code true}, which differs from the parent class. This ensures that the SegmentMetadata cache is
 * fully initialized before other startup processes proceed.</p>
 */
public class BrokerSegmentMetadataCacheConfig extends SegmentMetadataCacheConfig
{
  // A flag indicating whether to cache polled segments from the Coordinator.
  @JsonProperty
  private boolean metadataSegmentCacheEnable = false;

  // Interval for polling segments from the coordinator.
  @JsonProperty
  private long metadataSegmentPollPeriod = 60000;

  // A flag indicating whether to wait for cache initialization during startup.
  @JsonProperty
  private boolean awaitInitializationOnStart = true;

  public static BrokerSegmentMetadataCacheConfig create()
  {
    return new BrokerSegmentMetadataCacheConfig();
  }

  public static BrokerSegmentMetadataCacheConfig create(
      String metadataRefreshPeriod
  )
  {
    BrokerSegmentMetadataCacheConfig config = new BrokerSegmentMetadataCacheConfig();
    config.setMetadataRefreshPeriod(new Period(metadataRefreshPeriod));
    return config;
  }

  public boolean isMetadataSegmentCacheEnable()
  {
    return metadataSegmentCacheEnable;
  }

  public long getMetadataSegmentPollPeriod()
  {
    return metadataSegmentPollPeriod;
  }

  /**
   * This property is overriden on the broker, so that the cache initialization blocks startup.
   */
  @Override
  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
  }

  @Override
  public String toString()
  {
    return "BrokerSegmentMetadataCacheConfig{" +
           "metadataSegmentCacheEnable=" + metadataSegmentCacheEnable +
           ", metadataSegmentPollPeriod=" + metadataSegmentPollPeriod +
           ", awaitInitializationOnStart=" + awaitInitializationOnStart +
           ", metadataRefreshPeriod=" + getMetadataRefreshPeriod() +
           ", metadataColumnTypeMergePolicy=" + getMetadataColumnTypeMergePolicy() +
           '}';
  }
}
