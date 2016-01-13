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
import com.metamx.common.StringUtils;
import io.druid.query.filter.DimFilterCacheHelper;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ListBasedIndexedInts;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
public class ListFilteredDimensionSpec extends BaseFilteredDimensionSpec
{

  private static final byte CACHE_TYPE_ID = 0x3;

  private final List<String> values;
  private final boolean isWhitelist;

  public ListFilteredDimensionSpec(
      @JsonProperty("delegate") DimensionSpec delegate,
      @JsonProperty("values") List<String> values,
      @JsonProperty("isWhitelist") Boolean isWhitelist
  )
  {
    super(delegate);

    Preconditions.checkArgument(values != null && values.size() > 0, "values list must be non-empty");
    this.values = values;

    this.isWhitelist = isWhitelist == null ? true : isWhitelist.booleanValue();
  }

  @JsonProperty
  public List<String> getValues()
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

    final Set<Integer> matched = new HashSet<>(values.size());
    for (String value : values) {
      int i = selector.lookupId(value);
      if (i >= 0) {
        matched.add(i);
      }
    };

    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        IndexedInts baseRow = selector.getRow();
        List<Integer> result = new ArrayList<>(baseRow.size());

        for (int i : baseRow) {
          if (matched.contains(i)) {
            if (isWhitelist) {
              result.add(i);
            }
          } else {
            if (!isWhitelist) {
              result.add(i);
            }
          }
        }

        return new ListBasedIndexedInts(result);
      }

      @Override
      public int getValueCardinality()
      {
        return selector.getValueCardinality();
      }

      @Override
      public String lookupName(int id)
      {
        return selector.lookupName(id);
      }

      @Override
      public int lookupId(String name)
      {
        return selector.lookupId(name);
      }
    };
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
                                          .put(DimFilterCacheHelper.STRING_SEPARATOR);
    for (byte[] bytes : valuesBytes) {
      filterCacheKey.put(bytes)
                    .put(DimFilterCacheHelper.STRING_SEPARATOR);
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
