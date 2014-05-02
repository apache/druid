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
import com.google.common.collect.ImmutableMap;
import io.druid.client.DruidServer;
import org.joda.time.Period;

import javax.validation.constraints.NotNull;
import java.util.LinkedHashMap;

/**
 */
public class TieredBrokerConfig
{
  @JsonProperty
  @NotNull
  private String defaultBrokerServiceName = "";

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
  private String coordinatorServiceName = null;

  @JsonProperty
  @NotNull
  private Period pollPeriod = new Period("PT1M");

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
}
