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
import io.druid.common.utils.JodaUtils;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.List;

/**
 */
public class MultipleIntervalSegmentSpec implements QuerySegmentSpec
{
  private final List<Interval> intervals;

  @JsonCreator
  public MultipleIntervalSegmentSpec(
      @JsonProperty("intervals") List<Interval> intervals
  )
  {
    this.intervals = Collections.unmodifiableList(JodaUtils.condenseIntervals(intervals));
  }

  @Override
  @JsonProperty("intervals")
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "intervals=" + intervals +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    MultipleIntervalSegmentSpec that = (MultipleIntervalSegmentSpec) o;

    if (intervals != null ? !intervals.equals(that.intervals) : that.intervals != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return intervals != null ? intervals.hashCode() : 0;
  }
}
