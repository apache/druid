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

package io.druid.query.timeboundary;

import io.druid.timeline.LogicalSegment;
import junit.framework.Assert;
import org.joda.time.Interval;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

/**
 */
public class TimeBoundaryQueryQueryToolChestTest
{
  @Test
  public void testFilterSegments() throws Exception
  {
    List<LogicalSegment> segments = new TimeBoundaryQueryQueryToolChest().filterSegments(
        null,
        Arrays.asList(
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01/P1D");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01T01/PT1H");
              }
            },
            new LogicalSegment()
            {
              @Override
              public Interval getInterval()
              {
                return new Interval("2013-01-01T02/PT1H");
              }
            }
        )
    );

    Assert.assertEquals(segments.size(), 3);

    List<LogicalSegment> expected = Arrays.asList(
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01/P1D");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01T01/PT1H");
          }
        },
        new LogicalSegment()
        {
          @Override
          public Interval getInterval()
          {
            return new Interval("2013-01-01T02/PT1H");
          }
        }
    );

    for (int i = 0; i < segments.size(); i++) {
       Assert.assertEquals(segments.get(i).getInterval(), expected.get(i).getInterval());
    }
  }
}
