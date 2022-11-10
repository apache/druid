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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Period;

import java.util.Objects;

/**
 * Configuration properties for the Broker-side cache of segment metadata
 * used to infer datasources for SQL. This class shares the same config root
 * as {@link PlannerConfig} to maintain backward compatibility for when
 * the properties here resided in {@code PlannerConfig}.
 */
public class SegmentMetadataCacheConfig
{
  @JsonProperty
  private boolean metadataSegmentCacheEnable = false;

  @JsonProperty
  private long metadataSegmentPollPeriod = 60000;

  @JsonProperty
  private Period metadataRefreshPeriod = new Period("PT1M");

  @JsonProperty
  private boolean awaitInitializationOnStart = true;

  public static SegmentMetadataCacheConfig create()
  {
    return new SegmentMetadataCacheConfig();
  }

  public static SegmentMetadataCacheConfig create(
      String metadataRefreshPeriod
  )
  {
    SegmentMetadataCacheConfig config = new SegmentMetadataCacheConfig();
    config.metadataRefreshPeriod = new Period(metadataRefreshPeriod);
    return config;
  }

  public boolean isMetadataSegmentCacheEnable()
  {
    return metadataSegmentCacheEnable;
  }

  public Period getMetadataRefreshPeriod()
  {
    return metadataRefreshPeriod;
  }

  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
  }

  public long getMetadataSegmentPollPeriod()
  {
    return metadataSegmentPollPeriod;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SegmentMetadataCacheConfig that = (SegmentMetadataCacheConfig) o;
    return awaitInitializationOnStart == that.awaitInitializationOnStart &&
           metadataSegmentCacheEnable == that.metadataSegmentCacheEnable &&
           metadataSegmentPollPeriod == that.metadataSegmentPollPeriod &&
           Objects.equals(metadataRefreshPeriod, that.metadataRefreshPeriod);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        metadataRefreshPeriod,
        awaitInitializationOnStart,
        metadataSegmentCacheEnable,
        metadataSegmentPollPeriod
    );
  }

  @Override
  public String toString()
  {
    return "SegmentCacheConfig{" +
           "metadataRefreshPeriod=" + metadataRefreshPeriod +
           ", metadataSegmentCacheEnable=" + metadataSegmentCacheEnable +
           ", metadataSegmentPollPeriod=" + metadataSegmentPollPeriod +
           ", awaitInitializationOnStart=" + awaitInitializationOnStart +
           '}';
  }
}
