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

package io.druid.query.search.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 */
public class FragmentSearchQuerySpec implements SearchQuerySpec
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final List<String> values;
  private final boolean caseSensitive;

  private final String[] target;

  public FragmentSearchQuerySpec(
      List<String> values
  )
  {
    this(values, false);
  }

  @JsonCreator
  public FragmentSearchQuerySpec(
      @JsonProperty("values") List<String> values,
      @JsonProperty("caseSensitive") boolean caseSensitive
  )
  {
    this.values = values;
    this.caseSensitive = caseSensitive;
    Set<String> set = new TreeSet<>();
    if (values != null) {
      for (String value : values) {
        set.add(value);
      }
    }
    target = set.toArray(new String[set.size()]);
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  @JsonProperty
  public boolean isCaseSensitive()
  {
    return caseSensitive;
  }

  @Override
  public boolean accept(@Nullable String dimVal)
  {
    if (dimVal == null || values == null) {
      return false;
    }
    if (caseSensitive) {
      return containsAny(target, dimVal);
    }
    for (String search : target) {
      if (!org.apache.commons.lang.StringUtils.containsIgnoreCase(dimVal, search)) {
        return false;
      }
    }
    return true;
  }

  private boolean containsAny(String[] target, String input)
  {
    for (String value : target) {
      if (!input.contains(value)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    if (values == null) {
      return ByteBuffer.allocate(2)
                       .put(CACHE_TYPE_ID)
                       .put(caseSensitive ? (byte) 1 : 0).array();
    }

    final byte[][] valuesBytes = new byte[values.size()][];
    int valuesBytesSize = 0;
    int index = 0;
    for (String value : values) {
      valuesBytes[index] = StringUtils.toUtf8(value);
      valuesBytesSize += valuesBytes[index].length;
      ++index;
    }

    final ByteBuffer queryCacheKey = ByteBuffer.allocate(2 + valuesBytesSize)
                                               .put(CACHE_TYPE_ID)
                                               .put(caseSensitive ? (byte) 1 : 0);

    for (byte[] bytes : valuesBytes) {
      queryCacheKey.put(bytes);
    }

    return queryCacheKey.array();
  }

  @Override
  public String toString()
  {
    return "FragmentSearchQuerySpec{" +
           "values=" + values + ", caseSensitive=" + caseSensitive +
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

    if (caseSensitive ^ that.caseSensitive) {
      return false;
    }

    if (values == null && that.values == null) {
      return true;
    }

    return values != null && Arrays.equals(target, that.target);
  }

  @Override
  public int hashCode()
  {
    return Arrays.hashCode(target) + (caseSensitive ? (byte) 1 : 0);
  }
}
