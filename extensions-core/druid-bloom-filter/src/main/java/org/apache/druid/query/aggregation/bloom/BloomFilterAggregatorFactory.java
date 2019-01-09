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
import com.google.common.primitives.Ints;
import org.apache.commons.codec.binary.Base64;
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.java.util.common.RE;
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
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;

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
    return (Comparator<Object>) (o1, o2) -> {
      try {
        if (o1 instanceof ByteBuffer && o2 instanceof ByteBuffer) {
          BloomKFilter o1f = BloomKFilter.deserialize((ByteBuffer) o1);
          BloomKFilter o2f = BloomKFilter.deserialize((ByteBuffer) o2);
          return Ints.compare(o1f.getNumSetBits(), o2f.getNumSetBits());
        } else if (o1 instanceof BloomKFilter && o2 instanceof BloomKFilter) {
          BloomKFilter o1f = (BloomKFilter) o1;
          BloomKFilter o2f = (BloomKFilter) o2;
          return Ints.compare(o1f.getNumSetBits(), o2f.getNumSetBits());
        } else {
          throw new RE("Unable to compare unexpected types [%s]", o1.getClass().getName());
        }
      }
      catch (IOException ioe) {
        throw new RuntimeException("Failed to deserialize BloomKFilter");
      }
    };
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
    if (rhs instanceof BloomKFilter) {
      ((BloomKFilter) lhs).merge((BloomKFilter) rhs);
      return lhs;
    } else {
      ByteBuffer buf = (ByteBuffer) lhs;
      ByteBuffer other = (ByteBuffer) rhs;
      BloomKFilter.mergeBloomFilterByteBuffers(buf, buf.position(), other, other.position());
      return lhs;
    }
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
    if (object instanceof String) {
      return ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    try {
      if (object instanceof ByteBuffer) {
        return BloomKFilter.deserialize((ByteBuffer) object);
      } else if (object instanceof byte[]) {
        return BloomKFilter.deserialize(ByteBuffer.wrap((byte[]) object));
      } else {
        return object;
      }
    }
    catch (IOException ioe) {
      throw new RuntimeException("Failed to deserialize BloomKFilter");
    }
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
    return BloomFilterSerializersModule.BLOOM_FILTER_TYPE_NAME;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return BloomKFilter.computeSizeBytes(maxNumEntries);
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
