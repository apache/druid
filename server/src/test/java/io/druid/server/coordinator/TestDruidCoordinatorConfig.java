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

public class TestDruidCoordinatorConfig extends DruidCoordinatorConfig
{

  private final Duration coordinatorStartDelay;
  private final Duration coordinatorPeriod;
  private final Duration coordinatorIndexingPeriod;
  private final Duration loadTimeoutDelay;
  private final Duration coordinatorKillPeriod;
  private final Duration coordinatorKillDurationToRetain;
  private final int coordinatorKillMaxSegments;

  private final String consoleStatic;

  private final boolean mergeSegments;
  private final boolean convertSegments;

  public TestDruidCoordinatorConfig(
      Duration coordinatorStartDelay,
      Duration coordinatorPeriod,
      Duration coordinatorIndexingPeriod,
      Duration loadTimeoutDelay,
      Duration coordinatorKillPeriod,
      Duration coordinatorKillDurationToRetain,
      int coordinatorKillMaxSegments,
      String consoleStatic,
      boolean mergeSegments,
      boolean convertSegments
  )
  {
    this.coordinatorStartDelay = coordinatorStartDelay;
    this.coordinatorPeriod = coordinatorPeriod;
    this.coordinatorIndexingPeriod = coordinatorIndexingPeriod;
    this.loadTimeoutDelay = loadTimeoutDelay;
    this.coordinatorKillPeriod = coordinatorKillPeriod;
    this.coordinatorKillDurationToRetain = coordinatorKillDurationToRetain;
    this.coordinatorKillMaxSegments = coordinatorKillMaxSegments;
    this.consoleStatic = consoleStatic;
    this.mergeSegments = mergeSegments;
    this.convertSegments = convertSegments;
  }

  @Override
  public Duration getCoordinatorStartDelay()
  {
    return coordinatorStartDelay;
  }

  @Override
  public Duration getCoordinatorPeriod()
  {
    return coordinatorPeriod;
  }

  @Override
  public Duration getCoordinatorIndexingPeriod()
  {
    return coordinatorIndexingPeriod;
  }

  @Override
  public boolean isMergeSegments()
  {
    return mergeSegments;
  }

  @Override
  public boolean isConvertSegments()
  {
    return convertSegments;
  }

  @Override
  public Duration getCoordinatorKillPeriod()
  {
    return coordinatorKillPeriod;
  }

  @Override
  public Duration getCoordinatorKillDurationToRetain()
  {
    return coordinatorKillDurationToRetain;
  }

  @Override
  public int getCoordinatorKillMaxSegments()
  {
    return coordinatorKillMaxSegments;
  }

  @Override
  public Duration getLoadTimeoutDelay()
  {
    return loadTimeoutDelay == null ? super.getLoadTimeoutDelay() : loadTimeoutDelay;
  }

  @Override
  public String getConsoleStatic()
  {
    return consoleStatic;
  }
}
