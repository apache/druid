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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

/**
 * Configuration settings related to segment timeline management
 */
public class SegmentTimelineConfig
{
  @JsonProperty
  private final boolean fastIntervalSearch;

  // Default constructor in cases where the class is not being constructed from JSON properties (such as direct
  // injection from Guice without the JsonConfigProvider)
  @SuppressWarnings("unused")
  public SegmentTimelineConfig()
  {
    this(Boolean.FALSE);
  }

  @JsonCreator
  public SegmentTimelineConfig(@JsonProperty("fastIntervalSearch") @Nullable Boolean fastIntervalSearch)
  {
    this.fastIntervalSearch = fastIntervalSearch != null && fastIntervalSearch;
  }

  /**
   * Whether an indexing mechanism based on an interval tree for organizing segments in memory is being used, that
   * leads to faster search
   */
  public boolean isFastIntervalSearch()
  {
    return fastIntervalSearch;
  }
}
