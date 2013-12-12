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

package io.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.Duration;
import org.joda.time.Period;

public class TaskQueueConfig
{
  @JsonProperty
  private int maxSize;

  @JsonProperty
  private Duration startDelay;

  @JsonProperty
  private Duration restartDelay;

  @JsonProperty
  private Duration storageSyncRate;

  @JsonCreator
  public TaskQueueConfig(
      @JsonProperty("maxSize") final Integer maxSize,
      @JsonProperty("startDelay") final Period startDelay,
      @JsonProperty("restartDelay") final Period restartDelay,
      @JsonProperty("storageSyncRate") final Period storageSyncRate
  )
  {
    this.maxSize = maxSize == null ? Integer.MAX_VALUE : maxSize;
    this.startDelay = defaultDuration(startDelay, "PT1M");
    this.restartDelay = defaultDuration(restartDelay, "PT30S");
    this.storageSyncRate = defaultDuration(storageSyncRate, "PT1M");
  }

  public int getMaxSize()
  {
    return maxSize;
  }

  public Duration getStartDelay()
  {
    return startDelay;
  }

  public Duration getRestartDelay()
  {
    return restartDelay;
  }

  public Duration getStorageSyncRate()
  {
    return storageSyncRate;
  }

  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }
}
