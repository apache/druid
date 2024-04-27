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

package org.apache.druid.storage.azure;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 * Stores the configuration for writing task logs to Azure .
 */
public class AzureTaskLogsConfig
{
  @JsonProperty
  @NotNull
  private String container = null;

  @JsonProperty
  @NotNull
  private String prefix = null;

  public AzureTaskLogsConfig()
  {
  }

  public AzureTaskLogsConfig(String container, String prefix)
  {
    this.container = container;
    this.prefix = prefix;
  }

  public String getContainer()
  {
    return container;
  }

  public String getPrefix()
  {
    return prefix;
  }
}
