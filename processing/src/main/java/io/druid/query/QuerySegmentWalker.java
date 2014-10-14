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

package io.druid.query;

import org.joda.time.Interval;

/**
 */
public interface QuerySegmentWalker
{
  /**
   * Gets the Queryable for a given interval, the Queryable returned can be any version(s) or partitionNumber(s)
   * such that it represents the interval.
   *
   * @param <T> query result type
   * @param query the query to find a Queryable for
   * @param intervals the intervals to find a Queryable for
   * @return a Queryable object that represents the interval
   */
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals);

  /**
   * Gets the Queryable for a given list of SegmentSpecs.
   *
   * @param <T> the query result type
   * @param query the query to return a Queryable for
   * @param specs the list of SegmentSpecs to find a Queryable for
   * @return the Queryable object with the given SegmentSpecs
   */
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs);
}
