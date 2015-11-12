/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.curator;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

/**
 */
public class CuratorConfig
{
  @JsonProperty("host")
  private String zkHosts = "localhost";

  @JsonProperty("sessionTimeoutMs")
  @Min(0)
  private int zkSessionTimeoutMs = 30000;

  @JsonProperty("compress")
  private boolean enableCompression = true;

  public String getZkHosts()
  {
    return zkHosts;
  }

  public void setZkHosts(String zkHosts)
  {
    this.zkHosts = zkHosts;
  }

  public Integer getZkSessionTimeoutMs()
  {
    return zkSessionTimeoutMs;
  }

  public void setZkSessionTimeoutMs(Integer zkSessionTimeoutMs)
  {
    this.zkSessionTimeoutMs = zkSessionTimeoutMs;
  }

  public Boolean getEnableCompression()
  {
    return enableCompression;
  }

  public void setEnableCompression(Boolean enableCompression)
  {
    this.enableCompression = enableCompression;
  }
}
