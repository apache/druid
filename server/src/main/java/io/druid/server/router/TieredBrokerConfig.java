/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.router;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.client.DruidServer;
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

  @JsonProperty
  @NotNull
  private String defaultBrokerServiceName = DEFAULT_BROKER_SERVICE_NAME;

  @JsonProperty
  private LinkedHashMap<String, String> tierToBrokerMap;

  @JsonProperty
  @NotNull
  private String defaultRule = "_default";

  @JsonProperty
  @NotNull
  private String rulesEndpoint = "/druid/coordinator/v1/rules";

  @JsonProperty
  @NotNull
  private String coordinatorServiceName = DEFAULT_COORDINATOR_SERVICE_NAME;

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

  public String getRulesEndpoint()
  {
    return rulesEndpoint;
  }

  public String getCoordinatorServiceName()
  {
    return coordinatorServiceName;
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
