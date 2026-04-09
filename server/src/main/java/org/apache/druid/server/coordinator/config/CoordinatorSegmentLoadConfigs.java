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

package org.apache.druid.server.coordinator.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;

public class CoordinatorSegmentLoadConfigs
{
  /**
   * Default fill-fraction threshold applied to all tiers with no per-tier override in
   * {@link org.apache.druid.server.coordinator.CoordinatorDynamicConfig#getTierServerFillThreshold()}.
   * Servers whose fill fraction ({@code sizeUsed / maxSize}) exceeds this value are deprioritized
   * for segment assignment. {@code 1.0} means no threshold (current behavior).
   */
  @JsonProperty
  private final double defaultServerFillThreshold;

  @JsonCreator
  public CoordinatorSegmentLoadConfigs(
      @JsonProperty("defaultServerFillThreshold") Double defaultServerFillThreshold
  )
  {
    this.defaultServerFillThreshold = Configs.valueOrDefault(defaultServerFillThreshold, 1.0);
  }

  public double getDefaultServerFillThreshold()
  {
    return defaultServerFillThreshold;
  }
}
