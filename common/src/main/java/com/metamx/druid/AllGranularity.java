package com.metamx.druid;

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
