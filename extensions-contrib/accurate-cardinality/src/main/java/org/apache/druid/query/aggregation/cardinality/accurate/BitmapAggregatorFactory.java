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
import com.google.common.collect.ImmutableList;
import org.apache.commons.codec.binary.Base64;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.NoopAggregator;
import org.apache.druid.query.aggregation.NoopBufferAggregator;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.Collector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.CollectorFactory;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.RoaringBitmapCollector;
import org.apache.druid.query.aggregation.cardinality.accurate.collector.RoaringBitmapCollectorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.NilColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;


public class BitmapAggregatorFactory extends AggregatorFactory
{
  private static final CollectorFactory DEFAULT_BITMAP_FACTORY = new RoaringBitmapCollectorFactory();

  private final String name;
  private final String fieldName;

  @JsonCreator
  public BitmapAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") String fieldName
  )
  {
    this.name = name;
    this.fieldName = fieldName;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return NoopAggregator.instance();
    }
    final Class classOfObject = selector.classOfObject();
    if (classOfObject.equals(Object.class) || RoaringBitmapCollector.class.isAssignableFrom(classOfObject)) {
      return new BitmapAggregator(selector, DEFAULT_BITMAP_FACTORY.makeEmptyCollector());
    }
    throw new IAE("Incompatible type for metric[%s], expected a Bitmap, got a %s", fieldName, classOfObject);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    BaseObjectColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return NoopBufferAggregator.instance();
    }
    final Class classOfObject = selector.classOfObject();
    if (classOfObject.equals(Object.class) || RoaringBitmapCollector.class.isAssignableFrom(classOfObject)) {
      return new BitmapBufferAggregator(selector, DEFAULT_BITMAP_FACTORY);
    }
    throw new IAE("Incompatible type for metric[%s], expected a Bitmap, got a %s", fieldName, classOfObject);
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
    return ((RoaringBitmapCollector) lhs).fold((RoaringBitmapCollector) rhs);
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
    return ImmutableList.of(new BitmapAggregatorFactory(
        fieldName,
        fieldName
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
      buffer = ByteBuffer.wrap(Base64.decodeBase64(StringUtils.toUtf8((String) object)));
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
    return ((Collector) object).getCardinality();
  }

  @JsonProperty
  @Override
  public String getName()
  {
    return this.name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public String getTypeName()
  {
    return AccurateCardinalityModule.BITMAP_COLLECTOR;
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return 1024;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(AggregatorUtil.BITMAP_AGG_CACHE_TYPE_ID)
        .appendString(fieldName)
        .build();
  }
}
