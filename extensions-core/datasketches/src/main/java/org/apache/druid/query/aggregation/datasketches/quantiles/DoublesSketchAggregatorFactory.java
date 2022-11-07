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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.datasketches.Util;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.quantiles.DoublesUnion;
import org.apache.druid.jackson.DefaultTrueJsonIncludeFilter;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.ObjectAggregateCombiner;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.VectorColumnProcessorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

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
  public static final boolean DEFAULT_SHOULD_FINALIZE = true;

  // Used for sketch size estimation.
  public static final long DEFAULT_MAX_STREAM_LENGTH = 1_000_000_000;

  private final String name;
  private final String fieldName;
  private final int k;
  private final long maxStreamLength;
  private final boolean shouldFinalize;
  private final byte cacheTypeId;

  @JsonCreator
  public DoublesSketchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("k") @Nullable final Integer k,
      @JsonProperty("maxStreamLength") @Nullable final Long maxStreamLength,
      @JsonProperty("shouldFinalize") @Nullable final Boolean shouldFinalize
  )
  {
    this(
        name,
        fieldName,
        k,
        maxStreamLength,
        shouldFinalize,
        AggregatorUtil.QUANTILES_DOUBLES_SKETCH_BUILD_CACHE_TYPE_ID
    );
  }

  @VisibleForTesting
  public DoublesSketchAggregatorFactory(
      final String name,
      final String fieldName,
      @Nullable final Integer k
  )
  {
    this(name, fieldName, k, null, DEFAULT_SHOULD_FINALIZE);
  }

  DoublesSketchAggregatorFactory(
      final String name,
      final String fieldName,
      @Nullable final Integer k,
      @Nullable final Long maxStreamLength,
      @Nullable final Boolean shouldFinalize,
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
    Util.checkIfPowerOf2(this.k, "k");
    this.maxStreamLength = maxStreamLength == null ? DEFAULT_MAX_STREAM_LENGTH : maxStreamLength;
    this.shouldFinalize = shouldFinalize == null ? DEFAULT_SHOULD_FINALIZE : shouldFinalize;
    this.cacheTypeId = cacheTypeId;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory metricFactory)
  {
    if (metricFactory.getColumnCapabilities(fieldName) != null
        && metricFactory.getColumnCapabilities(fieldName).isNumeric()) {
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
        && metricFactory.getColumnCapabilities(fieldName).isNumeric()) {
      final BaseDoubleColumnValueSelector selector = metricFactory.makeColumnValueSelector(fieldName);
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
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    return ColumnProcessors.makeVectorProcessor(
        fieldName,
        new VectorColumnProcessorFactory<VectorAggregator>()
        {
          @Override
          public VectorAggregator makeSingleValueDimensionProcessor(
              ColumnCapabilities capabilities,
              SingleValueDimensionVectorSelector selector
          )
          {
            return new NoopDoublesSketchBufferAggregator();
          }

          @Override
          public VectorAggregator makeMultiValueDimensionProcessor(
              ColumnCapabilities capabilities,
              MultiValueDimensionVectorSelector selector
          )
          {
            return new NoopDoublesSketchBufferAggregator();
          }

          @Override
          public VectorAggregator makeFloatProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new DoublesSketchBuildVectorAggregator(selector, k, getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeDoubleProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new DoublesSketchBuildVectorAggregator(selector, k, getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeLongProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new DoublesSketchBuildVectorAggregator(selector, k, getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeObjectProcessor(ColumnCapabilities capabilities, VectorObjectSelector selector)
          {
            return new DoublesSketchMergeVectorAggregator(selector, k, getMaxIntermediateSizeWithNulls());
          }
        },
        selectorFactory
    );
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
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

  @JsonProperty
  public long getMaxStreamLength()
  {
    return maxStreamLength;
  }

  @JsonProperty
  @JsonInclude(value = JsonInclude.Include.CUSTOM, valueFilter = DefaultTrueJsonIncludeFilter.class)
  public boolean isShouldFinalize()
  {
    return shouldFinalize;
  }

  @Override
  public List<String> requiredFields()
  {
    return Collections.singletonList(fieldName);
  }

  @Override
  public int guessAggregatorHeapFootprint(long rows)
  {
    return DoublesSketch.getUpdatableStorageBytes(k, rows);
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new DoublesSketchAggregatorFactory(
        newName,
        getFieldName(),
        getK(),
        getMaxStreamLength(),
        shouldFinalize,
        cacheTypeId
    );
  }

  // Quantiles sketches never stop growing, but they do so very slowly.
  // This size must suffice for overwhelming majority of sketches,
  // but some sketches may request more memory on heap and move there
  @Override
  public int getMaxIntermediateSize()
  {
    return DoublesSketch.getUpdatableStorageBytes(k, maxStreamLength);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new DoublesSketchAggregatorFactory(
            fieldName,
            fieldName,
            k,
            maxStreamLength,
            shouldFinalize
        )
    );
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new DoublesSketchMergeAggregatorFactory(name, k, maxStreamLength, shouldFinalize);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof DoublesSketchAggregatorFactory) {
      final DoublesSketchAggregatorFactory castedOther = (DoublesSketchAggregatorFactory) other;

      if (castedOther.shouldFinalize == shouldFinalize) {
        // DoublesUnion supports inputs with different k.
        // The result will have effective k between the specified k and the minimum k from all input sketches
        // to achieve higher accuracy as much as possible.
        return new DoublesSketchMergeAggregatorFactory(
            name,
            Math.max(k, castedOther.k),
            Math.max(maxStreamLength, castedOther.maxStreamLength),
            shouldFinalize
        );
      }
    }

    throw new AggregatorFactoryNotMergeableException(this, other);
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable final Object object)
  {
    if (!shouldFinalize || object == null) {
      return object;
    }

    return ((DoublesSketch) object).getN();
  }

  /**
   * actual type is {@link DoublesSketch}
   */
  @Override
  public ColumnType getIntermediateType()
  {
    return DoublesSketchModule.TYPE;
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
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DoublesSketchAggregatorFactory that = (DoublesSketchAggregatorFactory) o;

    // no need to use cacheTypeId here
    return k == that.k
           && maxStreamLength == that.maxStreamLength
           && shouldFinalize == that.shouldFinalize
           && name.equals(that.name)
           && fieldName.equals(that.fieldName);
  }

  @Override
  public int hashCode()
  {
    // no need to use cacheTypeId here
    return Objects.hash(name, fieldName, k, maxStreamLength, shouldFinalize);
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
