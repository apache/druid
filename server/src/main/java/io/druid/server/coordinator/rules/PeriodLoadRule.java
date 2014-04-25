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

package io.druid.server.coordinator.rules;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Map;

/**
 */
public class PeriodLoadRule extends LoadRule
{
  private static final Logger log = new Logger(PeriodLoadRule.class);

  private final Period period;
  private final Period futurePeriod;
  private final Map<String, Integer> tieredReplicants;

  @JsonCreator
  public PeriodLoadRule(
      @JsonProperty("period") Period period,
      @JsonProperty("futurePeriod") Period futurePeriod,
      @JsonProperty("tieredReplicants") Map<String, Integer> tieredReplicants,
      // The following two vars need to be deprecated
      @JsonProperty("replicants") int replicants,
      @JsonProperty("tier") String tier
  )
  {
    this.period = period;
    this.futurePeriod = futurePeriod == null ? Period.seconds(0) : futurePeriod;

    if (tieredReplicants != null) {
      this.tieredReplicants = tieredReplicants;
    } else {     // Backwards compatible
      this.tieredReplicants = ImmutableMap.of(tier, replicants);
    }
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
  public Period getFuturePeriod()
  {
    return futurePeriod;
  }

  @Override
  @JsonProperty
  public Map<String, Integer> getTieredReplicants()
  {
    return tieredReplicants;
  }

  @Override
  public int getNumReplicants(String tier)
  {
    final Integer retVal = tieredReplicants.get(tier);
    return retVal == null ? 0 : retVal;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return appliesTo(segment.getInterval(), referenceTimestamp);
  }

  @Override
  public boolean appliesTo(Interval interval, DateTime referenceTimestamp)
  {
    final Interval currInterval = new Interval(referenceTimestamp.minus(period), referenceTimestamp.plus(futurePeriod));
    return currInterval.overlaps(interval);
  }
}
