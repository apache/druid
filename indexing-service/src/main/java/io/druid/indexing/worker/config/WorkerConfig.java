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

package io.druid.indexing.worker.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.server.DruidNode;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 */
public class WorkerConfig
{
  @JsonProperty
  @NotNull
  private String ip = DruidNode.getDefaultHost();

  @JsonProperty
  @NotNull
  private String version = "0";

  @JsonProperty
  @Min(1)
  private int capacity = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);

  public String getIp()
  {
    return ip;
  }

  public String getVersion()
  {
    return version;
  }

  public int getCapacity()
  {
    return capacity;
  }

  public WorkerConfig setCapacity(int capacity)
  {
    this.capacity = capacity;
    return this;
  }
}
