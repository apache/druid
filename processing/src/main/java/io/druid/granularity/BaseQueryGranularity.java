/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
