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

package io.druid.storage.azure;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

public class AzureTaskLogsConfig
{
  @JsonProperty
  @NotNull
  private String container = null;

  @JsonProperty
  @NotNull
  private String prefix = null;

  @JsonProperty
  @Min(1)
  private int maxTries = 3;

  public AzureTaskLogsConfig()
  {
  }

  public AzureTaskLogsConfig(String container, String prefix, int maxTries)
  {
    this.container = container;
    this.prefix = prefix;
    this.maxTries = maxTries;
  }

  public String getContainer()
  {
    return container;
  }

  public String getPrefix()
  {
    return prefix;
  }

  public int getMaxTries()
  {
    return maxTries;
  }
}
