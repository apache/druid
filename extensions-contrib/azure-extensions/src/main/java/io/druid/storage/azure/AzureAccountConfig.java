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

public class AzureAccountConfig
{
  @JsonProperty
  private String protocol = "https";

  @JsonProperty
  @Min(1)
  private int maxTries = 3;

  @JsonProperty
  @NotNull
  private String account;

  @JsonProperty
  @NotNull
  private String key;

  @JsonProperty
  @NotNull
  private String container;

  public void setProtocol(String protocol)
  {
    this.protocol = protocol;
  }

  public void setMaxTries(int maxTries)
  {
    this.maxTries = maxTries;
  }

  public void setAccount(String account)
  {
    this.account = account;
  }

  public void setKey(String key)
  {
    this.key = key;
  }

  public void setContainer(String container)
  {
    this.container = container;
  }

  public String getProtocol()
  {
    return protocol;
  }

  public int getMaxTries()
  {
    return maxTries;
  }

  public String getAccount()
  {
    return account;
  }

  public String getKey()
  {
    return key;
  }

  public String getContainer()
  {
    return container;
  }
}
