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

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.data.input.Row;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.groupby.RowBasedValueMatcherFactory;

import java.nio.ByteBuffer;

public class DimFilterHavingSpec implements HavingSpec
{
  private static final byte CACHE_KEY = (byte) 0x9;

  private final DimFilter dimFilter;
  private final RowBasedValueMatcherFactory valueMatcherFactory;
  private final ValueMatcher valueMatcher;

  @JsonCreator
  public DimFilterHavingSpec(
      @JsonProperty("filter") final DimFilter dimFilter
  )
  {
    this.dimFilter = Preconditions.checkNotNull(dimFilter, "filter");
    this.valueMatcherFactory = new RowBasedValueMatcherFactory();
    this.valueMatcher = dimFilter.toFilter().makeMatcher(valueMatcherFactory);
  }

  @JsonProperty("filter")
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @Override
  public boolean eval(final Row row)
  {
    // Not thread safe, but it doesn't have to be.
    valueMatcherFactory.setRow(row);
    try {
      return valueMatcher.matches();
    }
    finally {
      valueMatcherFactory.setRow(null);
    }
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] filterBytes = dimFilter.getCacheKey();
    return ByteBuffer.allocate(1 + filterBytes.length)
                     .put(CACHE_KEY)
                     .put(filterBytes)
                     .array();
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

    DimFilterHavingSpec that = (DimFilterHavingSpec) o;

    return dimFilter.equals(that.dimFilter);
  }

  @Override
  public int hashCode()
  {
    return dimFilter.hashCode();
  }

  @Override
  public String toString()
  {
    return "DimFilterHavingSpec{" +
           "dimFilter=" + dimFilter +
           '}';
  }
}
