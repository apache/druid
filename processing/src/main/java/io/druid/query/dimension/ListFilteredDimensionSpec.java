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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import io.druid.java.util.common.StringUtils;
import io.druid.query.filter.DimFilterUtils;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 */
public class ListFilteredDimensionSpec extends BaseFilteredDimensionSpec
{

  private static final byte CACHE_TYPE_ID = 0x3;

  private final Set<String> values;
  private final boolean isWhitelist;

  public ListFilteredDimensionSpec(
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("values") Set<String> values,
      @JsonProperty("isWhitelist") Boolean isWhitelist
  )
  {
    super(delegate);

    Preconditions.checkArgument(values != null && values.size() > 0, "values list must be non-empty");
    this.values = values;

    this.isWhitelist = isWhitelist == null ? true : isWhitelist.booleanValue();
  }

  @JsonProperty
  public Set<String> getValues()
  {
    return values;
  }

  @JsonProperty("isWhitelist")
  public boolean isWhitelist()
  {
    return isWhitelist;
  }

  @Override
  public DimensionSelector decorate(final DimensionSelector selector)
  {
    if (selector == null) {
      return selector;
    }

    final int selectorCardinality = selector.getValueCardinality();
    if (selectorCardinality < 0) {
      throw new UnsupportedOperationException("Cannot decorate a selector with no dictionary");
    }

    // Upper bound on cardinality of the filtered spec.
    final int cardinality = isWhitelist ? values.size() : selectorCardinality;

    int count = 0;
    final Map<Integer,Integer> forwardMapping = new HashMap<>(cardinality);
    final int[] reverseMapping = new int[cardinality];

    if (isWhitelist) {
      for (String value : values) {
        int i = selector.lookupId(value);
        if (i >= 0) {
          forwardMapping.put(i, count);
          reverseMapping[count++] = i;
        }
      }
    } else {
      for (int i = 0; i < selectorCardinality; i++) {
        if (!values.contains(Strings.nullToEmpty(selector.lookupName(i)))) {
          forwardMapping.put(i, count);
          reverseMapping[count++] = i;
        }
      }
    }

    return BaseFilteredDimensionSpec.decorate(selector, forwardMapping, reverseMapping);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] delegateCacheKey = delegate.getCacheKey();

    byte[][] valuesBytes = new byte[values.size()][];
    int valuesBytesSize = 0;
    int index = 0;
    for (String value : values) {
      valuesBytes[index] = StringUtils.toUtf8(value);
      valuesBytesSize += valuesBytes[index].length + 1;
      ++index;
    }

    ByteBuffer filterCacheKey = ByteBuffer.allocate(3 + delegateCacheKey.length + valuesBytesSize)
                                          .put(CACHE_TYPE_ID)
                                          .put(delegateCacheKey)
                                          .put((byte) (isWhitelist ? 1 : 0))
                                          .put(DimFilterUtils.STRING_SEPARATOR);
    for (byte[] bytes : valuesBytes) {
      filterCacheKey.put(bytes)
                    .put(DimFilterUtils.STRING_SEPARATOR);
    }
    return filterCacheKey.array();
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

    ListFilteredDimensionSpec that = (ListFilteredDimensionSpec) o;

    if (isWhitelist != that.isWhitelist) {
      return false;
    }
    return values.equals(that.values);

  }

  @Override
  public int hashCode()
  {
    int result = values.hashCode();
    result = 31 * result + (isWhitelist ? 1 : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ListFilteredDimensionSpec{" +
           "values=" + values +
           ", isWhitelist=" + isWhitelist +
           '}';
  }
}
