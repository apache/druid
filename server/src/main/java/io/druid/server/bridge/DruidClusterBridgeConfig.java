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
