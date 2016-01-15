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

package io.druid.server.coordinator;

import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class DruidCoordinatorConfig
{
  @Config("druid.coordinator.startDelay")
  @Default("PT300s")
  public abstract Duration getCoordinatorStartDelay();

  @Config("druid.coordinator.period")
  @Default("PT60s")
  public abstract Duration getCoordinatorPeriod();

  @Config("druid.coordinator.period.indexingPeriod")
  @Default("PT1800s")
  public abstract Duration getCoordinatorIndexingPeriod();

  @Config("druid.coordinator.merge.on")
  public boolean isMergeSegments()
  {
    return false;
  }

  @Config("druid.coordinator.conversion.on")
  public boolean isConvertSegments()
  {
    return false;
  }

  @Config("druid.coordinator.kill.on")
  public boolean isKillSegments()
  {
    return false;
  }

  @Config("druid.coordinator.kill.period")
  @Default("P1D")
  public abstract Duration getCoordinatorKillPeriod();

  @Config("druid.coordinator.kill.durationToRetain")
  @Default("PT-1s")
  public abstract Duration getCoordinatorKillDurationToRetain();

  @Config("druid.coordinator.kill.maxSegments")
  @Default("0")
  public abstract int getCoordinatorKillMaxSegments();

  @Config("druid.coordinator.load.timeout")
  public Duration getLoadTimeoutDelay()
  {
    return new Duration(15 * 60 * 1000);
  }

  @Config("druid.coordinator.console.static")
  public String getConsoleStatic()
  {
    return null;
  }
}
