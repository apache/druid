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

package io.druid.query.aggregation.distinctcount;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.AggregatorUtil;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class DistinctCountAggregatorFactory extends AggregatorFactory
{
  private static final BitMapFactory DEFAULT_BITMAP_FACTORY = new RoaringBitMapFactory();

  private final String name;
  private final String fieldName;
  private final BitMapFactory bitMapFactory;

  @JsonCreator
  public DistinctCountAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName,
      @JsonProperty("bitmapFactory") BitMapFactory bitMapFactory
  )
  {
    Preconditions.checkNotNull(name);
    Preconditions.checkNotNull(fieldName);
    this.name = name;
    this.fieldName = fieldName;
    this.bitMapFactory = bitMapFactory == null ? DEFAULT_BITMAP_FACTORY : bitMapFactory;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    DimensionSelector selector = makeDimensionSelector(columnFactory);
    if (selector == null) {
      return new EmptyDistinctCountAggregator();
    } else {
      return new DistinctCountAggregator(
          selector,
          bitMapFactory.makeEmptyMutableBitmap()
      );
    }
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    DimensionSelector selector = makeDimensionSelector(columnFactory);
    if (selector == null) {
      return EmptyDistinctCountBufferAggregator.instance();
    } else {
      return new DistinctCountBufferAggregator(makeDimensionSelector(columnFactory));
    }
  }

  private DimensionSelector makeDimensionSelector(final ColumnSelectorFactory columnFactory)
  {
    return columnFactory.makeDimensionSelector(new DefaultDimensionSpec(fieldName, fieldName));
  }

  @Override
  public Comparator getComparator()
  {
    return new Comparator()
    {
      @Override
      public int compare(Object o, Object o1)
      {
        return Longs.compare(((Number) o).longValue(), ((Number) o1).longValue());
      }
    };
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (lhs == null && rhs == null) {
      return 0L;
    }
    if (rhs == null) {
      return ((Number) lhs).longValue();
    }
    if (lhs == null) {
      return ((Number) rhs).longValue();
    }
    return ((Number) lhs).longValue() + ((Number) rhs).longValue();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new LongSumAggregatorFactory(name, name);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Arrays.<AggregatorFactory>asList(new DistinctCountAggregatorFactory(fieldName, fieldName, bitMapFactory));
  }

  @Override
  public Object deserialize(Object object)
  {
    return object;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty("bitmapFactory")
  public BitMapFactory getBitMapFactory()
  {
    return bitMapFactory;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public byte[] getCacheKey()
  {
    byte[] fieldNameBytes = StringUtils.toUtf8(fieldName);
    byte[] bitMapFactoryCacheKey = StringUtils.toUtf8(bitMapFactory.toString());
    return ByteBuffer.allocate(2 + fieldNameBytes.length + bitMapFactoryCacheKey.length)
                     .put(AggregatorUtil.DISTINCT_COUNT_CACHE_KEY)
                     .put(fieldNameBytes)
                     .put(AggregatorUtil.STRING_SEPARATOR)
                     .put(bitMapFactoryCacheKey)
                     .array();
  }

  @Override
  public String getTypeName()
  {
    return "distinctCount";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Longs.BYTES;
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

    DistinctCountAggregatorFactory that = (DistinctCountAggregatorFactory) o;

    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + fieldName.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "DistinctCountAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           '}';
  }
}
