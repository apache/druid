package com.metamx.druid;

import com.google.common.primitives.Longs;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;

public class DurationGranularity extends BaseQueryGranularity
{
  private final long length;
  private final long origin;

  @JsonCreator
  public DurationGranularity(
    @JsonProperty("duration") long duration,
    @JsonProperty("origin") DateTime origin
  )
  {
    this(duration, origin == null ? 0 : origin.getMillis());
  }

  public DurationGranularity(long millis, long origin)
  {
    this.length = millis;
    this.origin = origin % length;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return length;
  }

  @JsonProperty("origin")
  public DateTime getOrigin()
  {
    return new DateTime(origin);
  }

  public long getOriginMillis()
  {
    return origin;
  }

  @Override
  public long next(long t)
  {
    return t + getDurationMillis();
  }

  @Override
  public long truncate(long t)
  {
    final long duration = getDurationMillis();
    return t - t % duration + origin;
  }

  @Override
  public byte[] cacheKey()
  {
    return ByteBuffer.allocate(2 * Longs.BYTES).putLong(length).putLong(origin).array();
  }

  public long getDurationMillis()
  {
    return length;
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

    DurationGranularity that = (DurationGranularity) o;

    if (length != that.length) {
      return false;
    }
    if (origin != that.origin) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = (int) (length ^ (length >>> 32));
    result = 31 * result + (int) (origin ^ (origin >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "DurationGranularity{" +
           "length=" + length +
           ", origin=" + origin +
           '}';
  }
}
