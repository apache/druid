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

package org.apache.druid.query.aggregation.cardinality.accurate;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongBitmapCollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.LongRoaringBitmapCollectorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class BitmapAggregatorFactory extends AggregatorFactory
{
  private static final LongBitmapCollectorFactory DEFAULT_BITMAP_FACTORY = new LongRoaringBitmapCollectorFactory();

  private final String name;
  private final DimensionSpec field;

  public BitmapAggregatorFactory(String name, String field)
  {
    this(name, DefaultDimensionSpec.of(field));
  }

  @JsonCreator
  public BitmapAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") DimensionSpec field
  )
  {
    this.name = name;
    this.field = field;
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

  private BaseAccurateCardinalityAggregator factorizeInternal(ColumnSelectorFactory columnFactory, boolean onHeap)
  {
    if (field == null || field.getDimension() == null) {
      return new NoopAccurateCardinalityAggregator(DEFAULT_BITMAP_FACTORY, onHeap);
    }
    ColumnCapabilities capabilities = columnFactory.getColumnCapabilities(field.getDimension());
    if (capabilities != null) {
      ValueType type = capabilities.getType();
      switch (type) {
        case COMPLEX:
          return new BitmapAggregator(
              columnFactory.makeColumnValueSelector(field.getDimension()),
              DEFAULT_BITMAP_FACTORY,
              onHeap
          );
        default:
          throw new IAE(
              "Cannot create bitmap aggregator %s for invalid column type [%s]",
              onHeap ? "aggregator" : "buffer aggregator",
              type
          );
      }
    } else {
      ColumnValueSelector columnValueSelector = columnFactory.makeColumnValueSelector(field.getDimension());
      if (columnValueSelector instanceof NilColumnValueSelector) {
        return new NoopAccurateCardinalityAggregator(DEFAULT_BITMAP_FACTORY, onHeap);
      }
      return new ObjectAccurateCardinalityAggregator(
          columnFactory.makeColumnValueSelector(field.getDimension()),
          DEFAULT_BITMAP_FACTORY,
          onHeap
      );
    }
  }

  @Override
  public Comparator getComparator()
  {
    return Comparators.naturalNullsFirst();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    if (rhs == null) {
      return lhs;
    }
    if (lhs == null) {
      return rhs;
    }
    return ((LongRoaringBitmapCollector) lhs).fold((LongRoaringBitmapCollector) rhs);
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new BitmapAggregatorFactory(name, name);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new BitmapAggregatorCombiner(DEFAULT_BITMAP_FACTORY);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.<AggregatorFactory>singletonList(new BitmapAggregatorFactory(
        name,
        field
    ));
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
      buffer = ByteBuffer.wrap(StringUtils.decodeBase64(StringUtils.toUtf8((String) object)));
    } else {
      return object;
    }
    return DEFAULT_BITMAP_FACTORY.makeCollector(buffer);
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    if (object == null) {
      return 0;
    }
    return ((LongBitmapCollector) object).getCardinality();
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return this.name;
  }

  @JsonProperty
  public DimensionSpec getField()
  {
    return field;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(field.getDimension());
  }

  @Override
  public String getTypeName()
  {
    return AccurateCardinalityModule.BITMAP_COLLECTOR;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 1;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.BITMAP_AGG_CACHE_TYPE_ID)
        .appendCacheable(field)
        .build();
  }
}
