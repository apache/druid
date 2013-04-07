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

package com.metamx.druid.indexer.granularity;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;

import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.SortedSet;

/**
 * Tells the indexer how to group events based on timestamp. The events may then be further partitioned based
 *  on anything, using a ShardSpec.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = UniformGranularitySpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "uniform", value = UniformGranularitySpec.class),
    @JsonSubTypes.Type(name = "arbitrary", value = ArbitraryGranularitySpec.class)
})
public interface GranularitySpec
{
  /** Set of all time groups, broken up on segment boundaries. Should be sorted by interval start and non-overlapping.*/
  public SortedSet<Interval> bucketIntervals();

  /** Time-grouping interval corresponding to some instant, if any. */
  public Optional<Interval> bucketInterval(DateTime dt);
}
