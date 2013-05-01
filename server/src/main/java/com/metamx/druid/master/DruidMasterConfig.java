/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.master;

import org.joda.time.Duration;
import org.skife.config.Config;
import org.skife.config.Default;

/**
 */
public abstract class DruidMasterConfig
{
  @Config("druid.host")
  public abstract String getHost();

  @Config("druid.master.startDelay")
  @Default("PT600s")
  public abstract Duration getMasterStartDelay();

  @Config("druid.master.period")
  @Default("PT60s")
  public abstract Duration getMasterPeriod();

  @Config("druid.master.period.segmentMerger")
  @Default("PT1800s")
  public abstract Duration getMasterSegmentMergerPeriod();

  @Config("druid.master.millisToWaitBeforeDeleting")
  public long getMillisToWaitBeforeDeleting()
  {
    return 15 * 60 * 1000L;
  }

  @Config("druid.master.merger.on")
  public boolean isMergeSegments()
  {
    return false;
  }

  @Config("druid.master.conversion.on")
  public boolean isConvertSegments()
  {
    return false;
  }

  @Config("druid.master.merger.service")
  public String getMergerServiceName()
  {
    return null;
  }

  @Config("druid.master.merge.threshold")
  public long getMergeBytesLimit()
  {
    return 100000000L;
  }

  @Config("druid.master.merge.maxSegments")
  public int getMergeSegmentsLimit()
  {
    return Integer.MAX_VALUE;
  }

  @Config("druid.master.balancer.maxSegmentsToMove")
  @Default("5")
  public abstract int getMaxSegmentsToMove();

  @Config("druid.master.replicant.lifetime")
  @Default("15")
  public abstract int getReplicantLifetime();

  @Config("druid.master.replicant.throttleLimit")
  @Default("10")
  public abstract int getReplicantThrottleLimit();
}
