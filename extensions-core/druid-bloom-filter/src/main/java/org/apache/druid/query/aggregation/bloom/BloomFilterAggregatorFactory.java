/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.aggregation.bloom;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.codec.binary.Base64;
import org.apache.druid.io.ByteBufferInputStream;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.bloom.types.BloomFilterAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.bloom.types.BloomFilterAggregatorColumnSelectorStrategyFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.hive.common.util.BloomKFilter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class BloomFilterAggregatorFactory extends AggregatorFactory
{
  private static final int DEFAULT_NUM_ENTRIES = 1500;
  protected static final BloomFilterAggregatorColumnSelectorStrategyFactory STRATEGY_FACTORY =
      new BloomFilterAggregatorColumnSelectorStrategyFactory();

  private final String name;
  private final DimensionSpec field;
  private final int maxNumEntries;

  @JsonCreator
  public BloomFilterAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("field") final DimensionSpec field,
      @Nullable @JsonProperty("maxNumEntries") Integer maxNumEntries
  )
  {
    this.name = name;
    this.field = field;
    this.maxNumEntries = maxNumEntries != null ? maxNumEntries : DEFAULT_NUM_ENTRIES;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    ColumnSelectorPlus<BloomFilterAggregatorColumnSelectorStrategy> selectorPlus =
        DimensionHandlerUtils.createColumnSelectorPlus(
            STRATEGY_FACTORY,
            field,
            columnFactory
        );

    return new BloomFilterAggregator(selectorPlus, maxNumEntries);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    ColumnSelectorPlus<BloomFilterAggregatorColumnSelectorStrategy> selectorPlus =
        DimensionHandlerUtils.createColumnSelectorPlus(
            STRATEGY_FACTORY,
            field,
            columnFactory
        );

    return new BloomFilterBufferAggregator(selectorPlus, maxNumEntries);
  }

  @Override
  public Comparator getComparator()
  {
    // idk how to compare?
    return (Comparator<BloomKFilter>) (o1, o2) -> 0;
  }

  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    ((BloomKFilter) lhs).merge((BloomKFilter) rhs);
    return lhs;
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new BloomFilterMergeAggregatorFactory(name, name, maxNumEntries);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(new BloomFilterAggregatorFactory(name, field, maxNumEntries));
  }

  @Override
  public Object deserialize(Object object)
  {
    final ByteBuffer buffer;

    if (object instanceof byte[]) {
      buffer = ByteBuffer.wrap((byte[]) object);
    } else if (object instanceof ByteBuffer) {
      // Be conservative, don't assume we own this buffer.
      buffer = ((ByteBuffer) object).duplicate();
    } else if (object instanceof String) {
      buffer = ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }

    ByteBufferInputStream byteBufferInputStream = new ByteBufferInputStream(buffer);
    try {
      return BloomKFilter.deserialize(byteBufferInputStream);
    }
    catch (IOException ex) {
      throw new RuntimeException("Failed to deserialize bloomK filter", ex);
    }
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public DimensionSpec getField()
  {
    return field;
  }

  @JsonProperty
  public int getMaxNumEntries()
  {
    return maxNumEntries;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(field.getDimension());
  }

  @Override
  public String getTypeName()
  {
    return "bloomFilter";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    BloomKFilter throwaway = new BloomKFilter(maxNumEntries);
    return (throwaway.getBitSet().length * Long.BYTES) + 5;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.BLOOM_FILTER_CACHE_TYPE_ID)
        .appendCacheable(field)
        .appendInt(maxNumEntries)
        .build();
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
    BloomFilterAggregatorFactory that = (BloomFilterAggregatorFactory) o;
    return maxNumEntries == that.maxNumEntries &&
           Objects.equals(name, that.name) &&
           Objects.equals(field, that.field);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, field, maxNumEntries);
  }

  @Override
  public String toString()
  {
    return "BloomFilterAggregatorFactory{" +
           "name='" + name + '\'' +
           ", field=" + field +
           ", maxNumEntries=" + maxNumEntries +
           '}';
  }
}
