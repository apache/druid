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

package io.druid.query.filter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.StringUtils;
import com.sun.org.apache.xpath.internal.operations.Bool;
import io.druid.segment.ObjectColumnSelector;
import org.apache.commons.lang.math.NumberUtils;

import javax.annotation.Nonnull;
import java.nio.ByteBuffer;

/**
 * Between filter, support "less or equal than upper bound and great or equal than lower bound"
 * on String values and float values.
 * String comparisons are implemented through String#compareTo method which is case sensitive.
 * Float comparisons are implemented through Float#compare method.
 * Created by zhxiaog on 15/11/12.
 **/
public class BetweenDimFilter implements DimFilter
{
  private String dimension;
  private Object lower;
  private Object upper;
  private boolean numerically;

  @JsonCreator
  public BetweenDimFilter(
      @JsonProperty("dimension") @Nonnull String dimension,
      @JsonProperty("lower") @Nonnull Object lower,
      @JsonProperty("upper") @Nonnull Object upper,
      @JsonProperty("numerically") Boolean numerically
  )
  {
    // Preconditions.checkArgument(dimension != null, "dimension must not be blank.");
    // Preconditions.checkArgument(lower != null && upper != null, "dimension must not be blank.");
    this.dimension = dimension;
    this.numerically = numerically != null ?
                       numerically : (Number.class.isInstance(lower) && Number.class.isInstance(upper) ? true : false);

    if (this.numerically) {
      this.lower = Float.parseFloat(lower.toString());
      this.upper = Float.parseFloat(upper.toString());
      Preconditions.checkArgument(
          Float.compare((float) this.lower, (float) this.upper) <= 0,
          "required: lower <= upper"
      );
    } else {
      this.lower = lower.toString();
      this.upper = upper.toString();
      Preconditions.checkArgument(
          ((String) this.lower).compareTo((String) this.upper) <= 0,
          "required: lower <= upper"
      );
    }
  }

  @JsonProperty("dimension")
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty("upper")
  public Object getUpper()
  {
    return upper;
  }

  @JsonProperty("lower")
  public Object getLower()
  {
    return lower;
  }

  @JsonProperty("numerically")
  public boolean getNumerically()
  {
    return numerically;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    if (numerically) {
      return ByteBuffer.allocate(1 + dimensionBytes.length + 8)
                       .put(DimFilterCacheHelper.BETWEEN_CACHE_ID)
                       .putInt(1)
                       .put(dimensionBytes)
                       .putFloat((float) lower)
                       .putFloat((float) upper)
                       .array();
    } else {
      final byte[] lower_bytes = StringUtils.toUtf8(this.lower.toString());
      final byte[] upper_bytes = StringUtils.toUtf8(this.upper.toString());
      return ByteBuffer.allocate(1 + dimensionBytes.length + lower_bytes.length + upper_bytes.length)
                       .put(DimFilterCacheHelper.BETWEEN_CACHE_ID)
                       .putInt(0)
                       .put(dimensionBytes)
                       .put(lower_bytes)
                       .put(upper_bytes)
                       .array();
    }
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

    BetweenDimFilter that = (BetweenDimFilter) o;

    if (numerically != that.numerically) {
      return false;
    }
    if (!dimension.equals(that.dimension)) {
      return false;
    }
    if (!lower.equals(that.lower)) {
      return false;
    }
    return upper.equals(that.upper);

  }

  @Override
  public int hashCode()
  {
    int result = dimension.hashCode();
    result = 31 * result + lower.hashCode();
    result = 31 * result + upper.hashCode();
    result = 31 * result + (numerically ? 1 : 0);
    return result;
  }
}
