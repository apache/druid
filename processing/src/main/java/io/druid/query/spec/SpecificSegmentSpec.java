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

import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;

/**
*/
public class SpecificSegmentSpec implements QuerySegmentSpec
{
  private final SegmentDescriptor descriptor;

  public SpecificSegmentSpec(
      SegmentDescriptor descriptor
  ) {
    this.descriptor = descriptor;
  }

  @Override
  public List<Interval> getIntervals()
  {
    return Arrays.asList(descriptor.getInterval());
  }

  @Override
  public <T> QueryRunner<T> lookup(Query<T> query, QuerySegmentWalker walker)
  {
    return walker.getQueryRunnerForSegments(query, Arrays.asList(descriptor));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SpecificSegmentSpec that = (SpecificSegmentSpec) o;

    if (descriptor != null ? !descriptor.equals(that.descriptor) : that.descriptor != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return descriptor != null ? descriptor.hashCode() : 0;
  }
}
