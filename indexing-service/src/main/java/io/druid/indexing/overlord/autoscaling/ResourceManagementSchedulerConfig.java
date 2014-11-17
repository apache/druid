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

package io.druid.indexing.overlord.autoscaling;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Period;

/**
 */
public class ResourceManagementSchedulerConfig
{
  @JsonProperty
  private boolean doAutoscale = false;

  @JsonProperty
  private Period provisionPeriod = new Period("PT1M");

  @JsonProperty
  private Period terminatePeriod = new Period("PT1H");

  @JsonProperty
  private DateTime originTime = new DateTime("2012-01-01T00:55:00.000Z");

  public boolean isDoAutoscale()
  {
    return doAutoscale;
  }

  public Period getProvisionPeriod()
  {
    return provisionPeriod;
  }

  public Period getTerminatePeriod()
  {
    return terminatePeriod;
  }

  public DateTime getOriginTime()
  {
    return originTime;
  }
}
