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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class BaseQueryGranularity extends QueryGranularity
{
  public abstract long next(long offset);

  public abstract long truncate(long offset);

  public abstract byte[] cacheKey();

  public DateTime toDateTime(long offset)
  {
    return new DateTime(offset, DateTimeZone.UTC);
  }

  public Iterable<Long> iterable(final long start, final long end)
  {
    return new Iterable<Long>()
    {
      @Override
      public Iterator<Long> iterator()
      {
        return new Iterator<Long>()
        {
          long curr = truncate(start);
          long next = BaseQueryGranularity.this.next(curr);

          @Override
          public boolean hasNext()
          {
            return curr < end;
          }

          @Override
          public Long next()
          {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }

            long retVal = curr;

            curr = next;
            next = BaseQueryGranularity.this.next(curr);

            return retVal;
          }

          @Override
          public void remove()
          {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }
}
