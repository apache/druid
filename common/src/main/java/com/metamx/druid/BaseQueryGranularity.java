package com.metamx.druid;

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
