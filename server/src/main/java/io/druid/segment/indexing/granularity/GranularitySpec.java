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

package io.druid.segment.indexing.granularity;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Optional;
import com.metamx.common.Granularity;
import io.druid.granularity.QueryGranularity;
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
  /**
   * Set of all time groups, broken up on segment boundaries. Should be sorted by interval start and non-overlapping.
   *
   * @return set of all time groups
   */
   public Optional<SortedSet<Interval>> bucketIntervals();

  /**
   * Time-grouping interval corresponding to some instant, if any.
   *
   * @param dt instant to return time interval for
   * @return optional time interval
   * */
  public Optional<Interval> bucketInterval(DateTime dt);

  public Granularity getSegmentGranularity();

  public QueryGranularity getQueryGranularity();

  @Deprecated
  public GranularitySpec withQueryGranularity(QueryGranularity queryGranularity);
}
