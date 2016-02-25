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
import com.google.common.base.Strings;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;

/**
 */
public class SelectorDimFilter implements DimFilter
{
  private final String dimension;
  private final String value;
  private final BinaryOperator operator;

  @JsonCreator
  public SelectorDimFilter(
      @JsonProperty("dimension") String dimension,
      @JsonProperty("value") String value,
      @JsonProperty("operator") String operator
  )
  {
    Preconditions.checkArgument(dimension != null, "dimension must not be null");

    this.dimension = dimension;
    this.value = value;
    this.operator = BinaryOperator.get(operator);

    // don't allow null comparison, for now
    Preconditions.checkArgument(
        !(this.operator != BinaryOperator.EQ && this.operator != BinaryOperator.NE && Strings.isNullOrEmpty(value)),
        "null comparison is not allowed, except equals/not-equals"
    );
  }

  public SelectorDimFilter(String dimension, String value)
  {
    this(dimension, value, null);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    byte[] valueBytes = (value == null) ? new byte[]{} : StringUtils.toUtf8(value);

    return ByteBuffer.allocate(3 + dimensionBytes.length + valueBytes.length)
                     .put(DimFilterCacheHelper.SELECTOR_CACHE_ID)
                     .put((byte) operator.ordinal())
                     .put(dimensionBytes)
                     .put(DimFilterCacheHelper.STRING_SEPARATOR)
                     .put(valueBytes)
                     .array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public String getOperator()
  {
    return operator.name();
  }

  @JsonProperty
  public String getValue()
  {
    return value;
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

    SelectorDimFilter that = (SelectorDimFilter) o;

    if (dimension != null ? !dimension.equals(that.dimension) : that.dimension != null) {
      return false;
    }
    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }
    if (!operator.equals(that.operator)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dimension != null ? dimension.hashCode() : 0;
    result = 31 * result + (value != null ? value.hashCode() : 0);
    result = 31 * result + operator.ordinal();
    return result;
  }

  @Override
  public String toString()
  {
    return String.format("%s %s %s", dimension, operator.name(), value);
  }
}
