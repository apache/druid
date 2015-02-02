/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
