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
import org.apache.druid.guice.BloomFilterSerializersModule;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.BaseNullableColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class BloomFilterAggregatorFactory extends AggregatorFactory
{
  private static final int DEFAULT_NUM_ENTRIES = 1500;

  private static final Comparator COMPARATOR = Comparator.nullsFirst((o1, o2) -> {
    if (o1 instanceof ByteBuffer && o2 instanceof ByteBuffer) {
      ByteBuffer buf1 = (ByteBuffer) o1;
      ByteBuffer buf2 = (ByteBuffer) o2;
      return Integer.compare(
          BloomKFilter.getNumSetBits(buf1, buf1.position()),
          BloomKFilter.getNumSetBits(buf2, buf2.position())
      );
    } else {
      throw new RE("Unable to compare unexpected types [%s]", o1.getClass().getName());
    }
  });

  private final String name;
  private final DimensionSpec field;
  private final int maxNumEntries;

  @JsonCreator
  public BloomFilterAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("field") final DimensionSpec field,
      @JsonProperty("maxNumEntries") @Nullable Integer maxNumEntries
  )
  {
    this.name = name;
    this.field = field;
    this.maxNumEntries = maxNumEntries != null ? maxNumEntries : DEFAULT_NUM_ENTRIES;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory columnFactory)
  {
    return factorizeInternal(columnFactory, true);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory columnFactory)
  {
    return factorizeInternal(columnFactory, false);
  }

  @Override
  public Comparator getComparator()
  {
    return COMPARATOR;
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
    BloomKFilter.mergeBloomFilterByteBuffers(
        (ByteBuffer) lhs,
        ((ByteBuffer) lhs).position(),
        (ByteBuffer) rhs,
        ((ByteBuffer) rhs).position()
    );
    return lhs;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    throw new UnsupportedOperationException("Bloom filter aggregators are query-time only");
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
      return ByteBuffer.wrap(StringUtils.decodeBase64String((String) object));
    } else if (object instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) object);
    } else {
      return object;
    }
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
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

  private BaseBloomFilterAggregator factorizeInternal(ColumnSelectorFactory columnFactory, boolean onHeap)
  {
    if (field == null || field.getDimension() == null) {
      return new NoopBloomFilterAggregator(maxNumEntries, onHeap);
    }

    ColumnCapabilities capabilities = columnFactory.getColumnCapabilities(field.getDimension());

    if (capabilities != null) {
      ValueType type = capabilities.getType();
      switch (type) {
        case STRING:
          return new StringBloomFilterAggregator(
              columnFactory.makeDimensionSelector(field),
              maxNumEntries,
              onHeap
          );
        case LONG:
          return new LongBloomFilterAggregator(
              columnFactory.makeColumnValueSelector(field.getDimension()),
              maxNumEntries,
              onHeap
          );
        case FLOAT:
          return new FloatBloomFilterAggregator(
              columnFactory.makeColumnValueSelector(field.getDimension()),
              maxNumEntries,
              onHeap
          );
        case DOUBLE:
          return new DoubleBloomFilterAggregator(
              columnFactory.makeColumnValueSelector(field.getDimension()),
              maxNumEntries,
              onHeap
          );
        case COMPLEX:
          // in an ideal world, we would check complex type, but until then assume it's a bloom filter
          return new BloomFilterMergeAggregator(
              columnFactory.makeColumnValueSelector(field.getDimension()),
              maxNumEntries,
              onHeap
          );
        default:
          throw new IAE(
              "Cannot create bloom filter %s for invalid column type [%s]",
              onHeap ? "aggregator" : "buffer aggregator",
              type
          );
      }
    } else {
      BaseNullableColumnValueSelector selector = columnFactory.makeColumnValueSelector(field.getDimension());
      if (selector instanceof NilColumnValueSelector) {
        return new NoopBloomFilterAggregator(maxNumEntries, onHeap);
      }
      // no column capabilities, use fallback 'object' aggregator
      return new ObjectBloomFilterAggregator(
          columnFactory.makeColumnValueSelector(field.getDimension()),
          maxNumEntries,
          onHeap
      );
    }
  }
}
