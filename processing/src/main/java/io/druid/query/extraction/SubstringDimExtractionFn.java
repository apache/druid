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
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 */
public class SubstringDimExtractionFn extends DimExtractionFn
{
  private static final byte CACHE_TYPE_ID = 0x8;

  private final int index;
  private final int end;

  @JsonCreator
  public SubstringDimExtractionFn(
      @JsonProperty("index") int index,
      @Nullable
      @JsonProperty("length") Integer length
  )
  {

    Preconditions.checkArgument(length == null || length > 0, "length must be strictly positive");

    this.index = index;
    this.end = length != null ? index + length : -1;
  }

  @Override
  public byte[] getCacheKey()
  {
    return ByteBuffer.allocate(1 + 8)
                     .put(CACHE_TYPE_ID)
                     .putInt(this.index)
                     .putInt(this.end)
                     .array();
  }

  @Nullable
  @Override
  public String apply(@Nullable String dimValue)
  {
    if (Strings.isNullOrEmpty(dimValue)) {
      return null;
    }

    int len = dimValue.length();

    if (index < len) {
      if (end > 0) {
        return dimValue.substring(index, Math.min(end, len));
      } else {
        return dimValue.substring(index);
      }
    } else {
      return null;
    }
  }

  @JsonProperty
  public int getIndex()
  {
    return index;
  }

  @JsonProperty
  public Integer getLength()
  {
    return end > -1 ? end - index : null;
  }

  @Override
  public boolean preservesOrdering()
  {
    return index == 0 ? true : false;
  }

  @Override
  public ExtractionType getExtractionType()
  {
    return ExtractionType.MANY_TO_ONE;
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

    SubstringDimExtractionFn that = (SubstringDimExtractionFn) o;

    if (index != that.index) {
      return false;
    }
    return end == that.end;

  }

  @Override
  public int hashCode()
  {
    int result = index;
    result = 31 * result + end;
    return result;
  }

  @Override
  public String toString()
  {
    return StringUtils.format("substring(%s, %s)", index, getLength());
  }
}
