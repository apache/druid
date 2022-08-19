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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.kll.KllDoublesSketch;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnValueSelector;
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

public class KllDoublesSketchAggregatorFactory extends KllSketchAggregatorFactory<KllDoublesSketch, Double>
{
  public static final Comparator<KllDoublesSketch> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingLong(KllDoublesSketch::getN));

  @JsonCreator
  public KllDoublesSketchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("k") @Nullable final Integer k,
      @JsonProperty("maxStreamLength") @Nullable final Long maxStreamLength
  )
  {
    this(name, fieldName, k, maxStreamLength, AggregatorUtil.KLL_DOUBLES_SKETCH_BUILD_CACHE_TYPE_ID);
  }

  KllDoublesSketchAggregatorFactory(
      final String name,
      final String fieldName,
      @Nullable final Integer k,
      @Nullable final Long maxStreamLength,
      final byte cacheTypeId
  )
  {
    super(
        name,
        fieldName,
        k,
        maxStreamLength,
        cacheTypeId
    );
  }

  @Override
  public Comparator<KllDoublesSketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new KllDoublesSketchAggregatorFactory(
            getFieldName(),
            getFieldName(),
            getK(),
            getMaxStreamLength()
        )
    );
  }

  @Override
  public AggregatorFactory getMergingFactory(final AggregatorFactory other)
      throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof KllDoublesSketchAggregatorFactory) {
      // KllSketch supports merging with different k.
      // The result will have effective k between the specified k and the minimum k from all input sketches
      // to achieve higher accuracy as much as possible.
      return new KllDoublesSketchMergeAggregatorFactory(
          getName(),
          Math.max(getK(), ((KllDoublesSketchAggregatorFactory) other).getK()),
          Math.max(getMaxStreamLength(), ((KllDoublesSketchAggregatorFactory) other).getMaxStreamLength())
      );
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new KllDoublesSketchMergeAggregatorFactory(getName(), getK(), getMaxStreamLength());
  }

  @Override
  KllDoublesSketch getEmptySketch()
  {
    return KllDoublesSketchOperations.EMPTY_SKETCH;
  }

  @Override
  KllDoublesSketch newHeapInstance(final int k)
  {
    return KllDoublesSketch.newHeapInstance(k);
  }

  @Override
  Class<KllDoublesSketch> getSketchClass()
  {
    return KllDoublesSketch.class;
  }

  @Override
  int getMaxSerializedSizeBytes(final int k, final long n)
  {
    return KllDoublesSketch.getMaxSerializedSizeBytes(k, n, true);
  }

  @Override
  KllSketchBuildAggregator<KllDoublesSketch, Double> getBuildAggregator(final ColumnValueSelector<Double> selector)
  {
    return new KllDoublesSketchBuildAggregator(selector, getK());
  }

  @Override
  KllSketchMergeAggregator<KllDoublesSketch> getMergeAggregator(final ColumnValueSelector selector)
  {
    return new KllDoublesSketchMergeAggregator(selector, getK());
  }

  @Override
  KllDoublesSketchBuildBufferAggregator getBuildBufferAggregator(final ColumnValueSelector<Double> selector)
  {
    return new KllDoublesSketchBuildBufferAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
  }

  @Override
  KllDoublesSketchMergeBufferAggregator getMergeBufferAggregator(final ColumnValueSelector selector)
  {
    return new KllDoublesSketchMergeBufferAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    return ColumnProcessors.makeVectorProcessor(
        getFieldName(),
        new VectorColumnProcessorFactory<VectorAggregator>()
        {
          @Override
          public VectorAggregator makeSingleValueDimensionProcessor(
              ColumnCapabilities capabilities,
              SingleValueDimensionVectorSelector selector
          )
          {
            return new KllSketchNoOpBufferAggregator<KllDoublesSketch>(getEmptySketch());
          }

          @Override
          public VectorAggregator makeMultiValueDimensionProcessor(
              ColumnCapabilities capabilities,
              MultiValueDimensionVectorSelector selector
          )
          {
            return new KllSketchNoOpBufferAggregator<KllDoublesSketch>(getEmptySketch());
          }

          @Override
          public VectorAggregator makeFloatProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new KllDoublesSketchBuildVectorAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeDoubleProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new KllDoublesSketchBuildVectorAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeLongProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new KllDoublesSketchBuildVectorAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeObjectProcessor(ColumnCapabilities capabilities, VectorObjectSelector selector)
          {
            return new KllDoublesSketchMergeVectorAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
          }
        },
        selectorFactory
    );
  }

  @Override
  public Object deserialize(final Object object)
  {
    return KllDoublesSketchOperations.deserialize(object);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return KllSketchModule.DOUBLES_TYPE;
  }

}
