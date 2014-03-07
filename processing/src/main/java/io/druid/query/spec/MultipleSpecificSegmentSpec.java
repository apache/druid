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

package io.druid.query.spec;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import io.druid.common.utils.JodaUtils;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.List;

/**
 */
public class MultipleSpecificSegmentSpec implements QuerySegmentSpec
{
  private final List<SegmentDescriptor> descriptors;

  private volatile List<Interval> intervals = null;

  @JsonCreator
  public MultipleSpecificSegmentSpec(
      @JsonProperty("segments") List<SegmentDescriptor> descriptors
  )
  {
    this.descriptors = descriptors;
  }

  @JsonProperty("segments")
  public List<SegmentDescriptor> getDescriptors()
  {
    return descriptors;
  }

  @Override
  public List<Interval> getIntervals()
  {
    if (intervals != null) {
      return intervals;
    }

    intervals = JodaUtils.condenseIntervals(
        Iterables.transform(
            descriptors,
            new Function<SegmentDescriptor, Interval>()
            {
              @Override
              public Interval apply(SegmentDescriptor input)
              {
                return input.getInterval();
              }
            }
        )
    );

    return intervals;
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForSegments(query, descriptors);
  }

  @Override
  public String toString()
  {
    return "MultipleSpecificSegmentSpec{" +
           "descriptors=" + descriptors +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MultipleSpecificSegmentSpec that = (MultipleSpecificSegmentSpec) o;

    if (descriptors != null ? !descriptors.equals(that.descriptors) : that.descriptors != null) return false;
    if (intervals != null ? !intervals.equals(that.intervals) : that.intervals != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = descriptors != null ? descriptors.hashCode() : 0;
    result = 31 * result + (intervals != null ? intervals.hashCode() : 0);
    return result;
  }
}
