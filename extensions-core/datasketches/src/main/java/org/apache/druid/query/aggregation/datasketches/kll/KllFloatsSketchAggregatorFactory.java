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
import org.apache.datasketches.kll.KllFloatsSketch;
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

public class KllFloatsSketchAggregatorFactory extends KllSketchAggregatorFactory<KllFloatsSketch, Float>
{
  public static final Comparator<KllFloatsSketch> COMPARATOR =
      Comparator.nullsFirst(Comparator.comparingLong(KllFloatsSketch::getN));

  @JsonCreator
  public KllFloatsSketchAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("k") @Nullable final Integer k,
      @JsonProperty("maxStreamLength") @Nullable final Long maxStreamLength
  )
  {
    this(name, fieldName, k, maxStreamLength, AggregatorUtil.KLL_FLOATS_SKETCH_BUILD_CACHE_TYPE_ID);
  }

  KllFloatsSketchAggregatorFactory(
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
  public Comparator<KllFloatsSketch> getComparator()
  {
    return COMPARATOR;
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return Collections.singletonList(
        new KllFloatsSketchAggregatorFactory(
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
    if (other.getName().equals(this.getName()) && other instanceof KllFloatsSketchAggregatorFactory) {
      // KllSketch supports merging with different k.
      // The result will have effective k between the specified k and the minimum k from all input sketches
      // to achieve higher accuracy as much as possible.
      return new KllFloatsSketchMergeAggregatorFactory(
          getName(),
          Math.max(getK(), ((KllFloatsSketchAggregatorFactory) other).getK()),
          Math.max(getMaxStreamLength(), ((KllFloatsSketchAggregatorFactory) other).getMaxStreamLength())
      );
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new KllFloatsSketchMergeAggregatorFactory(getName(), getK(), getMaxStreamLength());
  }

  @Override
  KllFloatsSketch getEmptySketch()
  {
    return KllFloatsSketchOperations.EMPTY_SKETCH;
  }

  @Override
  KllFloatsSketch newHeapInstance(final int k)
  {
    return KllFloatsSketch.newHeapInstance(k);
  }

  @Override
  Class<KllFloatsSketch> getSketchClass()
  {
    return KllFloatsSketch.class;
  }

  @Override
  int getMaxSerializedSizeBytes(final int k, final long n)
  {
    return KllFloatsSketch.getMaxSerializedSizeBytes(k, n, true);
  }

  @Override
  KllSketchBuildAggregator<KllFloatsSketch, Float> getBuildAggregator(final ColumnValueSelector<Float> selector)
  {
    return new KllFloatsSketchBuildAggregator(selector, getK());
  }

  @Override
  KllSketchMergeAggregator<KllFloatsSketch> getMergeAggregator(final ColumnValueSelector selector)
  {
    return new KllFloatsSketchMergeAggregator(selector, getK());
  }

  @Override
  KllFloatsSketchBuildBufferAggregator getBuildBufferAggregator(ColumnValueSelector<Float> selector)
  {
    return new KllFloatsSketchBuildBufferAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
  }

  @Override
  KllFloatsSketchMergeBufferAggregator getMergeBufferAggregator(ColumnValueSelector selector)
  {
    return new KllFloatsSketchMergeBufferAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
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
            return new KllSketchNoOpBufferAggregator<KllFloatsSketch>(getEmptySketch());
          }

          @Override
          public VectorAggregator makeMultiValueDimensionProcessor(
              ColumnCapabilities capabilities,
              MultiValueDimensionVectorSelector selector
          )
          {
            return new KllSketchNoOpBufferAggregator<KllFloatsSketch>(getEmptySketch());
          }

          @Override
          public VectorAggregator makeFloatProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new KllFloatsSketchBuildVectorAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeDoubleProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new KllFloatsSketchBuildVectorAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeLongProcessor(ColumnCapabilities capabilities, VectorValueSelector selector)
          {
            return new KllFloatsSketchBuildVectorAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
          }

          @Override
          public VectorAggregator makeObjectProcessor(ColumnCapabilities capabilities, VectorObjectSelector selector)
          {
            return new KllFloatsSketchMergeVectorAggregator(selector, getK(), getMaxIntermediateSizeWithNulls());
          }
        },
        selectorFactory
    );
  }

  @Override
  public Object deserialize(final Object object)
  {
    return KllFloatsSketchOperations.deserialize(object);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return KllSketchModule.FLOATS_TYPE;
  }

}
