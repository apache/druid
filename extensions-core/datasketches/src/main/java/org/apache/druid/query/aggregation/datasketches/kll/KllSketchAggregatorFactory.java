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

package org.apache.druid.query.aggregation.datasketches.kll;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.kll.KllSketch;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

abstract class KllSketchAggregatorFactory<SketchType extends KllSketch, ValueType> extends AggregatorFactory
{
  public static final int DEFAULT_K = 200;

  // Used for sketch size estimation.
  public static final long DEFAULT_MAX_STREAM_LENGTH = 1_000_000_000;

  private final String name;
  private final String fieldName;
  private final int k;
  private final long maxStreamLength;
  private final byte cacheTypeId;

  KllSketchAggregatorFactory(
      final String name,
      final String fieldName,
      @Nullable final Integer k,
      @Nullable final Long maxStreamLength,
      final byte cacheTypeId
  )
  {
    if (name == null) {
      throw new IAE("Must have a valid, non-null aggregator name");
    }
    this.name = name;
    if (fieldName == null) {
      throw new IAE("Parameter fieldName must be specified");
    }
    this.fieldName = fieldName;
    this.k = k == null ? DEFAULT_K : k;
    this.maxStreamLength = maxStreamLength == null ? DEFAULT_MAX_STREAM_LENGTH : maxStreamLength;
    this.cacheTypeId = cacheTypeId;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    if (metricFactory.getColumnCapabilities(fieldName) != null
        && metricFactory.getColumnCapabilities(fieldName).isNumeric()) {
      final ColumnValueSelector<ValueType> selector = metricFactory.makeColumnValueSelector(fieldName);
      if (selector instanceof NilColumnValueSelector) {
        return new KllSketchNoOpAggregator<SketchType>(getEmptySketch());
      }
      return getBuildAggregator(selector);
    }
    final ColumnValueSelector<SketchType> selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return new KllSketchNoOpAggregator<SketchType>(getEmptySketch());
    }
    return getMergeAggregator(selector);
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    if (metricFactory.getColumnCapabilities(fieldName) != null
        && metricFactory.getColumnCapabilities(fieldName).isNumeric()) {
      final ColumnValueSelector<ValueType> selector = metricFactory.makeColumnValueSelector(fieldName);
      if (selector instanceof NilColumnValueSelector) {
        return new KllSketchNoOpBufferAggregator<SketchType>(getEmptySketch());
      }
      return getBuildBufferAggregator(selector);
    }
    final ColumnValueSelector<SketchType> selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return new KllSketchNoOpBufferAggregator<SketchType>(getEmptySketch());
    }
    return getMergeBufferAggregator(selector);
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  @Override
  public Object combine(final Object lhs, final Object rhs)
  {
    final SketchType sketch = newHeapInstance(k);
    sketch.merge((SketchType) lhs);
    sketch.merge((SketchType) rhs);
    return sketch;
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<SketchType>()
    {
      private final SketchType union = newHeapInstance(k);

      @Override
      public void reset(final ColumnValueSelector selector)
      {
        union.reset();
        fold(selector);
      }

      @Override
      public void fold(final ColumnValueSelector selector)
      {
        final SketchType sketch = (SketchType) selector.getObject();
        union.merge(sketch);
      }

      @Nullable
      @Override
      public SketchType getObject()
      {
        return union;
      }

      @Override
      public Class<SketchType> classOfObject()
      {
        return getSketchClass();
      }
    };
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public int getK()
  {
    return k;
  }

  @JsonProperty
  public long getMaxStreamLength()
  {
    return maxStreamLength;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public int guessAggregatorHeapFootprint(long rows)
  {
    return getMaxSerializedSizeBytes(k, rows);
  }

  // Quantiles sketches never stop growing, but they do so very slowly.
  // This size must suffice for overwhelming majority of sketches,
  // but some sketches may request more memory on heap and move there
  @Override
  public int getMaxIntermediateSize()
  {
    return getMaxSerializedSizeBytes(k, maxStreamLength);
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    return object == null ? null : ((SketchType) object).getN();
  }

  @Override
  public ColumnType getResultType()
  {
    return ColumnType.LONG;
  }

  @Override
  public byte[] getCacheKey()
  {
    // maxStreamLength is not included in the cache key as it does nothing with query result.
    return new CacheKeyBuilder(cacheTypeId).appendString(name).appendString(fieldName).appendInt(k).build();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KllSketchAggregatorFactory<SketchType, ValueType> that = (KllSketchAggregatorFactory<SketchType, ValueType>) o;
    return k == that.k
           && maxStreamLength == that.maxStreamLength
           && name.equals(that.name)
           && fieldName.equals(that.fieldName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, k, maxStreamLength); // no need to use cacheTypeId here
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
           + "name=" + name
           + ", fieldName=" + fieldName
           + ", k=" + k
           + "}";
  }

  abstract SketchType getEmptySketch();

  abstract SketchType newHeapInstance(int k);

  abstract Class<SketchType> getSketchClass();

  abstract int getMaxSerializedSizeBytes(int k, long n);

  abstract KllSketchBuildAggregator<SketchType, ValueType> getBuildAggregator(
      ColumnValueSelector<ValueType> selector);

  abstract KllSketchMergeAggregator<SketchType> getMergeAggregator(ColumnValueSelector selector);

  abstract KllSketchBuildBufferAggregator<SketchType, ValueType>
      getBuildBufferAggregator(ColumnValueSelector<ValueType> selector);

  abstract KllSketchMergeBufferAggregator<SketchType>
      getMergeBufferAggregator(ColumnValueSelector selector);
}
