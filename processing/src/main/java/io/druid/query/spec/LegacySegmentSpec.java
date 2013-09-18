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
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.IAE;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 */
public class LegacySegmentSpec extends MultipleIntervalSegmentSpec
{
  private static List<Interval> convertValue(Object intervals)
  {
    final List<?> intervalStringList;
    if (intervals instanceof String) {
      intervalStringList = Arrays.asList((((String) intervals).split(",")));
    } else if (intervals instanceof Interval) {
      intervalStringList = Arrays.asList(intervals.toString());
    } else if (intervals instanceof Map) {
      intervalStringList = (List) ((Map) intervals).get("intervals");
    } else if (intervals instanceof List) {
      intervalStringList = (List) intervals;
    } else {
      throw new IAE("Unknown type[%s] for intervals[%s]", intervals.getClass(), intervals);
    }

    return Lists.transform(
        intervalStringList,
        new Function<Object, Interval>()
        {
          @Override
          public Interval apply(Object input)
          {
            return new Interval(input);
          }
        }
    );
  }

  @JsonCreator
  public LegacySegmentSpec(
      Object intervals
  )
  {
    super(convertValue(intervals));
  }
}
