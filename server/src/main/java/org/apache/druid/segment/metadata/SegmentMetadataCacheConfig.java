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

package org.apache.druid.segment.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.joda.time.Period;

/**
 * Coordinator-side configuration class for customizing properties related to the SegmentMetadata cache.
 * See {@link CoordinatorSegmentMetadataCache}
 */
public class SegmentMetadataCacheConfig
{
  // A flag indicating whether to wait for cache initialization during startup.
  @JsonProperty
  private boolean awaitInitializationOnStart = false;

  // Cache refresh interval.
  @JsonProperty
  private Period metadataRefreshPeriod = new Period("PT1M");

  // This is meant to be used for testing purpose.
  @JsonProperty
  private boolean disableSegmentMetadataQueries = false;

  @JsonProperty
  private AbstractSegmentMetadataCache.ColumnTypeMergePolicy metadataColumnTypeMergePolicy =
      new AbstractSegmentMetadataCache.LeastRestrictiveTypeMergePolicy();

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

  @VisibleForTesting
  public void setMetadataRefreshPeriod(Period metadataRefreshPeriod)
  {
    this.metadataRefreshPeriod = metadataRefreshPeriod;
  }

  public boolean isAwaitInitializationOnStart()
  {
    return awaitInitializationOnStart;
  }

  public boolean isDisableSegmentMetadataQueries()
  {
    return disableSegmentMetadataQueries;
  }

  public AbstractSegmentMetadataCache.ColumnTypeMergePolicy getMetadataColumnTypeMergePolicy()
  {
    return metadataColumnTypeMergePolicy;
  }

  public Period getMetadataRefreshPeriod()
  {
    return metadataRefreshPeriod;
  }

  public void setDisableSegmentMetadataQueries(boolean disableSegmentMetadataQueries)
  {
    this.disableSegmentMetadataQueries = disableSegmentMetadataQueries;
  }

  @Override
  public String toString()
  {
    return "SegmentMetadataCacheConfig{" +
           "awaitInitializationOnStart=" + awaitInitializationOnStart +
           ", metadataRefreshPeriod=" + metadataRefreshPeriod +
           ", metadataColumnTypeMergePolicy=" + metadataColumnTypeMergePolicy +
           '}';
  }
}
