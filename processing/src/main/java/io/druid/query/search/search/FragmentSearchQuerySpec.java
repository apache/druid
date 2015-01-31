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
import java.util.List;

/**
 */
public class FragmentSearchQuerySpec implements SearchQuerySpec
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final List<String> values;

  @JsonCreator
  public FragmentSearchQuerySpec(
      @JsonProperty("values") List<String> values
  )
  {
    this.values = values;
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  @Override
  public boolean accept(String dimVal)
  {
    for (String value : values) {
      if (dimVal == null || !dimVal.toLowerCase().contains(value.toLowerCase())) {
        return false;
      }
    }
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[][] valuesBytes = new byte[values.size()][];
    int valuesBytesSize = 0;
    int index = 0;
    for (String value : values) {
      valuesBytes[index] = StringUtils.toUtf8(value);
      valuesBytesSize += valuesBytes[index].length;
      ++index;
    }

    final ByteBuffer queryCacheKey = ByteBuffer.allocate(1 + valuesBytesSize)
                                               .put(CACHE_TYPE_ID);

    for (byte[] bytes : valuesBytes) {
      queryCacheKey.put(bytes);
    }

    return queryCacheKey.array();
  }

  @Override
  public String toString()
  {
    return "FragmentSearchQuerySpec{" +
           "values=" + values +
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

    FragmentSearchQuerySpec that = (FragmentSearchQuerySpec) o;

    if (values != null ? !values.equals(that.values) : that.values != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    return values != null ? values.hashCode() : 0;
  }
}
