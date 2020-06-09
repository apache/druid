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

package org.apache.druid.server.router;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.DruidServer;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;

/**
 */
public class TieredBrokerConfig
{
  public static final String DEFAULT_COORDINATOR_SERVICE_NAME = "druid/coordinator";
  public static final String DEFAULT_BROKER_SERVICE_NAME = "druid/broker";
  public static final String DEFAULT_RULE_NAME = "_default";

  @JsonProperty
  @NotNull
  private String defaultBrokerServiceName = DEFAULT_BROKER_SERVICE_NAME;

  @JsonProperty
  private LinkedHashMap<String, String> tierToBrokerMap;

  @JsonProperty
  @NotNull
  private String defaultRule = DEFAULT_RULE_NAME;

  @JsonProperty
  @NotNull
  private Period pollPeriod = new Period("PT1M");

  @JsonProperty
  @NotNull
  private List<TieredBrokerSelectorStrategy> strategies = Arrays.asList(
      new TimeBoundaryTieredBrokerSelectorStrategy(),
      new PriorityTieredBrokerSelectorStrategy(0, 1)
  );

  // tier, <bard, numThreads>
  public LinkedHashMap<String, String> getTierToBrokerMap()
  {
    return tierToBrokerMap == null ? new LinkedHashMap<>(
        ImmutableMap.of(
            DruidServer.DEFAULT_TIER, defaultBrokerServiceName
        )
    ) : tierToBrokerMap;
  }

  public String getDefaultBrokerServiceName()
  {
    return defaultBrokerServiceName;
  }

  public String getDefaultRule()
  {
    return defaultRule;
  }

  public Period getPollPeriod()
  {
    return pollPeriod;
  }

  public List<TieredBrokerSelectorStrategy> getStrategies()
  {
    return ImmutableList.copyOf(strategies);
  }
}
