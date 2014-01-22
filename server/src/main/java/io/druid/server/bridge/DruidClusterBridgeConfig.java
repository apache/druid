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

import io.druid.client.DruidServer;
import io.druid.server.initialization.ZkPathsConfig;
import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class DruidClusterBridgeConfig extends ZkPathsConfig
{
  @Config("druid.server.tier")
  @Default(DruidServer.DEFAULT_TIER)
  public abstract String getTier();

  @Config("druid.bridge.startDelay")
  @Default("PT300s")
  public abstract Duration getStartDelay();

  @Config("druid.bridge.period")
  @Default("PT60s")
  public abstract Duration getPeriod();

  @Config("druid.bridge.broker.serviceName")
  public abstract String getBrokerServiceName();

  @Config("druid.server.priority")
  public int getPriority()
  {
    return DruidServer.DEFAULT_PRIORITY;
  }
}
