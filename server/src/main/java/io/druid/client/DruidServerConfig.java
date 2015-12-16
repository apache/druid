/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.client;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

/**
 */
public class DruidServerConfig
{
  @JsonProperty
  @Min(0)
  private long maxSize = 0;

  @JsonProperty
  private String tier = DruidServer.DEFAULT_TIER;

  @JsonProperty
  private int priority = DruidServer.DEFAULT_PRIORITY;

  public long getMaxSize()
  {
    return maxSize;
  }

  public String getTier()
  {
    return tier;
  }

  public int getPriority()
  {
    return priority;
  }
}
