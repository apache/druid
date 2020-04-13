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

package org.apache.druid.query.aggregation.datasketches.quantiles;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.Util;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class DoublesSketchAggregatorFactory extends AggregatorFactory
{
  public static final Comparator<DoublesSketch> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingLong(DoublesSketch::getN));

  public static final int DEFAULT_K = 128;

  // Used for sketch size estimation.
  private static final long MAX_STREAM_LENGTH = 1_000_000_000;

  private final String name;
  private final String fieldName;
  private final int k;
  private final byte cacheTypeId;

  @JsonCreator
  public DoublesSketchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("k") final Integer k)
  {
    this(name, fieldName, k, AggregatorUtil.QUANTILES_DOUBLES_SKETCH_BUILD_CACHE_TYPE_ID);
  }

  DoublesSketchAggregatorFactory(final String name, final String fieldName, final Integer k, final byte cacheTypeId)
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
    Util.checkIfPowerOf2(this.k, "k");
    this.cacheTypeId = cacheTypeId;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    if (metricFactory.getColumnCapabilities(fieldName) != null
        && ValueType.isNumeric(metricFactory.getColumnCapabilities(fieldName).getType())) {
      final ColumnValueSelector<Double> selector = metricFactory.makeColumnValueSelector(fieldName);
      if (selector instanceof NilColumnValueSelector) {
        return new NoopDoublesSketchAggregator();
      }
      return new DoublesSketchBuildAggregator(selector, k);
    }
    final ColumnValueSelector<DoublesSketch> selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return new NoopDoublesSketchAggregator();
    }
    return new DoublesSketchMergeAggregator(selector, k);
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory metricFactory)
  {
    if (metricFactory.getColumnCapabilities(fieldName) != null
        && ValueType.isNumeric(metricFactory.getColumnCapabilities(fieldName).getType())) {
      final ColumnValueSelector<Double> selector = metricFactory.makeColumnValueSelector(fieldName);
      if (selector instanceof NilColumnValueSelector) {
        return new NoopDoublesSketchBufferAggregator();
      }
      return new DoublesSketchBuildBufferAggregator(selector, k, getMaxIntermediateSizeWithNulls());
    }
    final ColumnValueSelector<DoublesSketch> selector = metricFactory.makeColumnValueSelector(fieldName);
    if (selector instanceof NilColumnValueSelector) {
      return new NoopDoublesSketchBufferAggregator();
    }
    return new DoublesSketchMergeBufferAggregator(selector, k, getMaxIntermediateSizeWithNulls());
  }

  @Override
  public Object deserialize(final Object object)
  {
    return DoublesSketchOperations.deserialize(object);
  }

  @Override
  public Comparator<DoublesSketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public Object combine(final Object lhs, final Object rhs)
  {
    final DoublesUnion union = DoublesUnion.builder().setMaxK(k).build();
    union.update((DoublesSketch) lhs);
    union.update((DoublesSketch) rhs);
    return union.getResultAndReset();
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return new ObjectAggregateCombiner<DoublesSketch>()
    {
      private final DoublesUnion union = DoublesUnion.builder().setMaxK(k).build();

      @Override
      public void reset(final ColumnValueSelector selector)
      {
        union.reset();
        fold(selector);
      }

      @Override
      public void fold(final ColumnValueSelector selector)
      {
        final DoublesSketch sketch = (DoublesSketch) selector.getObject();
        union.update(sketch);
      }

      @Nullable
      @Override
      public DoublesSketch getObject()
      {
        return union.getResult();
      }

      @Override
      public Class<DoublesSketch> classOfObject()
      {
        return DoublesSketch.class;
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

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  // Quantiles sketches never stop growing, but they do so very slowly.
  // This size must suffice for overwhelming majority of sketches,
  // but some sketches may request more memory on heap and move there
  @Override
  public int getMaxIntermediateSize()
  {
    return DoublesSketch.getUpdatableStorageBytes(k, MAX_STREAM_LENGTH);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new DoublesSketchAggregatorFactory(
            fieldName,
            fieldName,
            k)
        );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoublesSketchMergeAggregatorFactory(name, k);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof DoublesSketchAggregatorFactory) {
      // DoublesUnion supports inputs with different k.
      // The result will have effective k between the specified k and the minimum k from all input sketches
      // to achieve higher accuracy as much as possible.
      return new DoublesSketchMergeAggregatorFactory(name, Math.max(k, ((DoublesSketchAggregatorFactory) other).k));
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    return object == null ? null : ((DoublesSketch) object).getN();
  }

  @Override
  public String getTypeName()
  {
    return DoublesSketchModule.DOUBLES_SKETCH;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(cacheTypeId).appendString(name).appendString(fieldName).appendInt(k).build();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    final DoublesSketchAggregatorFactory that = (DoublesSketchAggregatorFactory) o;
    if (!name.equals(that.name)) {
      return false;
    }
    if (!fieldName.equals(that.fieldName)) {
      return false;
    }
    if (k != that.k) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, k); // no need to use cacheTypeId here
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

}
