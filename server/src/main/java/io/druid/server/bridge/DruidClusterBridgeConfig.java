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

package io.druid.server.bridge;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;

/**
 */
public abstract class DruidClusterBridgeConfig
{
  @JsonProperty
  private Duration startDelay = new Duration("PT300s");
  @JsonProperty
  private Duration period = new Duration("PT60s");
  @JsonProperty
  private String brokerServiceName = "broker";

  public Duration getStartDelay()
  {
    return startDelay;
  }

  public void setStartDelay(Duration startDelay)
  {
    this.startDelay = startDelay;
  }

  public Duration getPeriod()
  {
    return period;
  }

  public void setPeriod(Duration period)
  {
    this.period = period;
  }

  public String getBrokerServiceName()
  {
    return brokerServiceName;
  }

  public void setBrokerServiceName(String brokerServiceName)
  {
    this.brokerServiceName = brokerServiceName;
  }
}
