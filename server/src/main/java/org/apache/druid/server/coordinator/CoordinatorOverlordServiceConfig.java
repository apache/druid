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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

/**
 */
public class CoordinatorOverlordServiceConfig
{
  @JsonProperty
  private final boolean enabled;

  @JsonProperty
  private final String overlordService;

  public CoordinatorOverlordServiceConfig(
      @JsonProperty("enabled") Boolean enabled,
      @JsonProperty("overlordService") String overlordService
  )
  {
    this.enabled = enabled == null ? false : enabled.booleanValue();
    this.overlordService = overlordService;

    Preconditions.checkArgument((this.enabled && this.overlordService != null) || !this.enabled,
                                "coordinator is enabled to be overlord but overlordService is not specified");
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  public String getOverlordService()
  {
    return overlordService;
  }
}
