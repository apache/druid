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

package io.druid.granularity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.metamx.common.IAE;
import org.joda.time.DateTime;
import org.joda.time.ReadableDuration;

public abstract class QueryGranularity
{
  public abstract long next(long offset);

  public abstract long truncate(long offset);

  public abstract byte[] cacheKey();

  public abstract DateTime toDateTime(long offset);

  public abstract Iterable<Long> iterable(final long start, final long end);

  public static final QueryGranularity ALL = new AllGranularity();
  public static final QueryGranularity NONE = new NoneGranularity();

  public static final QueryGranularity MINUTE = fromString("MINUTE");
  public static final QueryGranularity HOUR   = fromString("HOUR");
  public static final QueryGranularity DAY    = fromString("DAY");
  public static final QueryGranularity SECOND = fromString("SECOND");

  @JsonCreator
  public static QueryGranularity fromString(String str)
  {
    String name = str.toUpperCase();
    if(name.equals("ALL"))
    {
      return QueryGranularity.ALL;
    }
    else if(name.equals("NONE"))
    {
      return QueryGranularity.NONE;
    }
    return new DurationGranularity(convertValue(str), 0);
  }

  private static enum MillisIn
  {
    SECOND         (            1000),
    MINUTE         (       60 * 1000),
    FIFTEEN_MINUTE (15 *   60 * 1000),
    THIRTY_MINUTE  (30 *   60 * 1000),
    HOUR           (     3600 * 1000),
    DAY            (24 * 3600 * 1000);

    private final long millis;
    MillisIn(final long millis) { this.millis = millis; }
  }

  private static long convertValue(Object o)
  {
    if(o instanceof String)
    {
      return MillisIn.valueOf(((String) o).toUpperCase()).millis;
    }
    else if(o instanceof ReadableDuration)
    {
      return ((ReadableDuration)o).getMillis();
    }
    else if(o instanceof Number)
    {
      return ((Number)o).longValue();
    }
    throw new IAE("Cannot convert [%s] to QueryGranularity", o.getClass());
  }
}
