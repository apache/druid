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


package org.apache.druid.client.selector;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;

public class PreferredTierSelectorStrategyConfig
{
  @JsonProperty
  private String tier;

  /**
   * Only two options: high or low
   */
  @JsonProperty
  private String priority;

  @JsonCreator
  public PreferredTierSelectorStrategyConfig(
      @JsonProperty("tier") String tier,
      @JsonProperty("priority") String priority
  )
  {
    Preconditions.checkState(tier != null && !tier.isEmpty(),
                             "druid.broker.select.tier.preferred.tier can't be empty");
    this.tier = tier.trim();
    this.priority = priority;
  }

  public String getTier()
  {
    return tier;
  }

  public void setTier(String tier)
  {
    this.tier = tier;
  }

  public String getPriority()
  {
    return priority;
  }

  public void setPriority(String priority)
  {
    this.priority = priority;
  }
}
