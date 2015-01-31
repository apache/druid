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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import io.druid.client.DruidServer;
import io.druid.client.DruidServerConfig;
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
