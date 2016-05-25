package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Doubles;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class BucketExtractionFn implements ExtractionFn
{

  private final double size;
  private final double offset;

  @JsonCreator
  public BucketExtractionFn(
      @Nullable
      @JsonProperty("size") Double size,
      @Nullable
      @JsonProperty("offset") Double offset
  )
  {
    this.size = size == null ? 1 : size;
    this.offset = offset == null ? 0 : offset;
  }

  @JsonProperty
  public double getSize()
  {
    return size;
  }

  @JsonProperty
  public double getOffset()
  {
    return offset;
  }

  @Override
  public String apply(Object value)
  {
    if (value instanceof Number) {
      return bucket((Double) value);
    } else if (value instanceof String) {
      return apply(value);
    }
    return null;
  }

  @Override
  public String apply(String value)
  {
    try {
      return apply(Double.parseDouble(value));
    } catch (NumberFormatException | NullPointerException ex) {
      return null;
    }
  }

  @Override
  public String apply(long value)
  {
    return bucket(value);
  }

  private String bucket(double value) {
    double ret = Math.floor((value - offset) / size) * size + offset;
    return ret == (long)ret ? String.valueOf((long)ret) : String.valueOf(ret);
  }

  @Override
  public boolean preservesOrdering()
  {
    return false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
  }

  @Override
  public byte[] getCacheKey()
  {
    return ByteBuffer.allocate(1 + 2 * Doubles.BYTES)
                     .put(ExtractionCacheHelper.CACHE_TYPE_ID_BUCKET)
                     .putDouble(size)
                     .putDouble(offset)
                     .array();
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

    BucketExtractionFn that = (BucketExtractionFn) o;

    return size == that.size && offset == that.offset;

  }
}
