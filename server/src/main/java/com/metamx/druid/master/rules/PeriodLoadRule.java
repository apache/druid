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

package com.metamx.druid.master.rules;

import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

/**
 */
public class PeriodLoadRule extends LoadRule
{
  private static final Logger log = new Logger(PeriodLoadRule.class);

  private final Period period;
  private final Integer replicationFactor;
  private final String tier;

  @JsonCreator
  public PeriodLoadRule(
      @JsonProperty("period") String period,
      @JsonProperty("replicationFactor") Integer replicationFactor,
      @JsonProperty("tier") String tier
  )
  {
    this.period = (period == null || period.equalsIgnoreCase("all")) ? new Period("P1000Y") : new Period(period);
    this.replicationFactor = (replicationFactor == null) ? 2 : replicationFactor; //TODO:config
    this.tier = tier;
  }

  @Override
  @JsonProperty
  public String getType()
  {
    return "loadByPeriod";
  }

  @JsonProperty
  public Period getPeriod()
  {
    return period;
  }

  @JsonProperty
  public int getReplicationFactor()
  {
    return replicationFactor;
  }

  @Override
  public int getReplicationFactor(String tier)
  {
    return (this.tier.equalsIgnoreCase(tier)) ? replicationFactor : 0;
  }

  @JsonProperty
  public String gettier()
  {
    return tier;
  }

  @Override
  public boolean appliesTo(DataSegment segment)
  {
    final Interval currInterval = new Interval(new DateTime().minus(period), period);
    return currInterval.overlaps(segment.getInterval());
  }
}
