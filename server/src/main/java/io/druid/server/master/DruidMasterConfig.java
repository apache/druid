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

package io.druid.server.master;

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
  @Default("PT60s")
  public abstract Duration getMasterStartDelay();

  @Config("druid.master.period")
  @Default("PT60s")
  public abstract Duration getMasterPeriod();

  @Config("druid.master.period.segmentMerger")
  @Default("PT1800s")
  public abstract Duration getMasterSegmentMergerPeriod();

  @Config("druid.master.merge.on")
  public boolean isMergeSegments()
  {
    return false;
  }

  @Config("druid.master.conversion.on")
  public boolean isConvertSegments()
  {
    return false;
  }

  @Config("druid.master.load.timeout")
  public Duration getLoadTimeoutDelay()
  {
    return new Duration(15 * 60 * 1000);
  }
}
