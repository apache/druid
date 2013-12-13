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
import io.druid.timeline.DataSegment;
import org.joda.time.DateTime;

/**
 */
public class ForeverLoadRule extends LoadRule
{
  private final Integer replicants;
  private final String tier;

  @JsonCreator
  public ForeverLoadRule(
      @JsonProperty("replicants") Integer replicants,
      @JsonProperty("tier") String tier
  )
  {
    this.replicants = (replicants == null) ? 2 : replicants;
    this.tier = tier;
  }

  @Override
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
  public String getTier()
  {
    return null;
  }

  @Override
  public String getType()
  {
    return "loadForever";
  }

  @Override
  public boolean appliesTo(DataSegment segment, DateTime referenceTimestamp)
  {
    return true;
  }
}
