/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.query.search;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.Lists;



import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class FragmentSearchQuerySpec implements SearchQuerySpec
{
  private static final byte CACHE_TYPE_ID = 0x2;

  private final List<String> values;
  private final SearchSortSpec sortSpec;

  @JsonCreator
  public FragmentSearchQuerySpec(
      @JsonProperty("values") List<String> values,
      @JsonProperty("sort") SearchSortSpec sortSpec
  )
  {
    this.values = Lists.transform(
        values,
        new Function<String, String>()
        {
          @Override
          public String apply(String s)
          {
            return s.toLowerCase();
          }
        }
    );
    this.sortSpec = (sortSpec == null) ? new LexicographicSearchSortSpec() : sortSpec;
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  @JsonProperty("sort")
  @Override
  public SearchSortSpec getSearchSortSpec()
  {
    return sortSpec;
  }

  @Override
  public boolean accept(String dimVal)
  {
    for (String value : values) {
      if (dimVal == null || !dimVal.toLowerCase().contains(value)) {
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
      valuesBytes[index] = value.getBytes();
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
             ", sortSpec=" + sortSpec +
           "}";
  }
}
