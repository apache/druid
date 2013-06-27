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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Range;
import com.metamx.druid.client.DataSegment;
import org.joda.time.DateTime;

/**
 */
public class SizeLoadRule extends LoadRule
{
  private final long low;
  private final long high;
  private final Integer replicants;
  private final String tier;
  private final Range<Long> range;

  @JsonCreator
  public SizeLoadRule(
      @JsonProperty("low") long low,
      @JsonProperty("high") long high,
      @JsonProperty("replicants") Integer replicants,
      @JsonProperty("tier") String tier
  )
  {
    this.low = low;
    this.high = high;
    this.replicants = replicants;
    this.tier = tier;
    this.range = Range.closedOpen(low, high);
  }

  @Override
  @JsonProperty
  public int getReplicants()
  {
    return replicants;
  }

  @Override
  public int getReplicants(String tier)
  {
    return (this.tier.equalsIgnoreCase(tier)) ? replicants : 0;
  }

  @Override
  @JsonProperty
  public String getTier()
  {
    return tier;
  }

  @Override
  public String getType()
  {
    return "loadBySize";
  }

  @JsonProperty
  public long getLow()
  {
    return low;
  }

  @JsonProperty
  public long getHigh()
  {
    return high;
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return range.contains(segment.getSize());
  }
}
