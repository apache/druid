package com.metamx.druid;

public final class NoneGranularity extends BaseQueryGranularity
{
  @Override
  public long next(long offset)
  {
    return offset + 1;
  }

  @Override
  public long truncate(long offset)
  {
    return offset;
  }

  @Override
  public byte[] cacheKey()
  {
    return new byte[]{0x0};
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
    return "NoneGranularity";
  }
}
