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

package io.druid.query.extraction;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Doubles;
import io.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class BucketExtractionFn implements ExtractionFn
{

  private final double size;
  private final double offset;

  @JsonCreator
  public BucketExtractionFn(
      @Nullable @JsonProperty("size") Double size,
      @Nullable @JsonProperty("offset") Double offset
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
  @Nullable
  public String apply(@Nullable Object value)
  {
    if (value == null) {
      return null;
    }

    if (value instanceof Number) {
      return bucket(((Number) value).doubleValue());
    } else if (value instanceof String) {
      return apply((String) value);
    }
    return null;
  }

  @Override
  @Nullable
  public String apply(@Nullable String value)
  {
    if (value == null) {
      return null;
    }

    try {
      return bucket(Double.parseDouble(value));
    }
    catch (NumberFormatException | NullPointerException ex) {
      return null;
    }
  }

  @Override
  public String apply(long value)
  {
    return bucket(value);
  }

  private String bucket(double value)
  {
    double ret = Math.floor((value - offset) / size) * size + offset;
    return ret == (long) ret ? String.valueOf((long) ret) : String.valueOf(ret);
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

    if (Double.compare(that.size, size) != 0) {
      return false;
    }
    return Double.compare(that.offset, offset) == 0;

  }

  @Override
  public int hashCode()
  {
    int result;
    long temp;
    temp = Double.doubleToLongBits(size);
    result = (int) (temp ^ (temp >>> 32));
    temp = Double.doubleToLongBits(offset);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("bucket(%f, %f)", size, offset);
  }
}
