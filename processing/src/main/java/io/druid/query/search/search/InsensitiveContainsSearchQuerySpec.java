/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.metamx.common.StringUtils;

import java.nio.ByteBuffer;

/**
 */
public class InsensitiveContainsSearchQuerySpec implements SearchQuerySpec
{
  private static final byte CACHE_TYPE_ID = 0x1;

  private final String value;

  @JsonCreator
  public InsensitiveContainsSearchQuerySpec(
      @JsonProperty("value") String value
  )
  {
    this.value = value;
  }

  @JsonProperty
  public String getValue()
  {
    return value;
  }

  @Override
  public boolean accept(String dimVal)
  {
    if (dimVal == null) {
      return false;
    }
    return dimVal.toLowerCase().contains(value.toLowerCase());
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] valueBytes = StringUtils.toUtf8(value);

    return ByteBuffer.allocate(1 + valueBytes.length)
                     .put(CACHE_TYPE_ID)
                     .put(valueBytes)
                     .array();
  }

  @Override
  public String toString()
  {
    return "InsensitiveContainsSearchQuerySpec{" +
           "value=" + value +
           "}";
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

    InsensitiveContainsSearchQuerySpec that = (InsensitiveContainsSearchQuerySpec) o;

    if (value != null ? !value.equals(that.value) : that.value != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return value != null ? value.hashCode() : 0;
  }
}
