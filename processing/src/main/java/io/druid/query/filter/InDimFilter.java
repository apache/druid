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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

public class InDimFilter implements DimFilter
{
  private final List<String> values;
  private final String dimension;

  @JsonCreator
  public InDimFilter(@JsonProperty("dimension") String dimension, @JsonProperty("values") List<String> values)
  {
    Preconditions.checkNotNull(dimension, "dimension can not be null");
    this.values = (values == null) ? Collections.<String>emptyList() : values;
    this.dimension = dimension;
  }

  @JsonProperty
  public String getDimension()
  {
    return dimension;
  }

  @JsonProperty
  public List<String> getValues()
  {
    return values;
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] dimensionBytes = StringUtils.toUtf8(dimension);
    final byte[][] valuesBytes = new byte[values.size()][];
    int valuesBytesSize = 0;
    int index = 0;
    for (String value : values) {
      valuesBytes[index] = StringUtils.toUtf8(value);
      valuesBytesSize += valuesBytes[index].length + 1;
      ++index;
    }

    ByteBuffer filterCacheKey = ByteBuffer.allocate(2 + dimensionBytes.length + valuesBytesSize)
                                          .put(DimFilterCacheHelper.IN_CACHE_ID)
                                          .put(dimensionBytes)
                                          .put(DimFilterCacheHelper.STRING_SEPARATOR);
    for (byte [] bytes: valuesBytes) {
      filterCacheKey.put(bytes)
                    .put((byte) 0xFF);
    }
    return filterCacheKey.array();
  }

  @Override
  public DimFilter optimize()
  {
    return this;
  }

  @Override
  public int hashCode()
  {
    int result = getValues().hashCode();
    result = 31 * result + getDimension().hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof InDimFilter)) {
      return false;
    }

    InDimFilter that = (InDimFilter) o;

    if (!values.equals(that.values)) {
      return false;
    }
    return dimension.equals(that.dimension);

  }
}
