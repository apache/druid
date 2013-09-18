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

import com.google.common.collect.ImmutableList;

public final class AllGranularity extends BaseQueryGranularity
{
  @Override
  public long next(long offset)
  {
    return Long.MAX_VALUE;
  }

  @Override
  public long truncate(long offset)
  {
    return Long.MIN_VALUE;
  }

  @Override
  public byte[] cacheKey()
  {
    return new byte[]{0x7f};
  }

  @Override
  public Iterable<Long> iterable(long start, long end)
  {
    return ImmutableList.of(start);
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return 0;
  }

  @Override
  public String toString()
  {
    return "AllGranularity";
  }
}
