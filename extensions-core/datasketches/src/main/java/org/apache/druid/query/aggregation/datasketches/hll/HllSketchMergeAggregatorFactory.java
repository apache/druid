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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.TgtHllType;
import org.apache.datasketches.hll.Union;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactoryNotMergeableException;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;

/**
 * This aggregator factory is for merging existing sketches.
 * The input column must contain {@link HllSketch}
 */
public class HllSketchMergeAggregatorFactory extends HllSketchAggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex(HllSketchModule.MERGE_TYPE_NAME);

  @JsonCreator
  public HllSketchMergeAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("lgK") @Nullable final Integer lgK,
      @JsonProperty("tgtHllType") @Nullable final String tgtHllType,
      @JsonProperty("shouldFinalize") final Boolean shouldFinalize,
      @JsonProperty("round") final boolean round
  )
  {
    super(name, fieldName, lgK, tgtHllType, shouldFinalize, round);
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && other instanceof HllSketchMergeAggregatorFactory) {
      HllSketchMergeAggregatorFactory castedOther = (HllSketchMergeAggregatorFactory) other;

      if (castedOther.isShouldFinalize() == isShouldFinalize()) {
        return new HllSketchMergeAggregatorFactory(
            getName(),
            getName(),
            Math.max(getLgK(), castedOther.getLgK()),
            getTgtHllType().compareTo(castedOther.getTgtHllType()) < 0 ? castedOther.getTgtHllType() : getTgtHllType(),
            isShouldFinalize(),
            isRound() || castedOther.isRound()
        );
      }
    }

    throw new AggregatorFactoryNotMergeableException(this, other);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  protected byte getCacheTypeId()
  {
    return AggregatorUtil.HLL_SKETCH_MERGE_CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<HllSketch> selector = columnSelectorFactory.makeColumnValueSelector(getFieldName());
    return new HllSketchMergeAggregator(selector, getLgK(), TgtHllType.valueOf(getTgtHllType()));
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<HllSketch> selector = columnSelectorFactory.makeColumnValueSelector(getFieldName());
    return new HllSketchMergeBufferAggregator(
        selector,
        getLgK(),
        TgtHllType.valueOf(getTgtHllType()),
        getMaxIntermediateSize()
    );
  }

  @Override
  public boolean canVectorize(ColumnInspector columnInspector)
  {
    return true;
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    return new HllSketchMergeVectorAggregator(
        selectorFactory,
        getFieldName(),
        getLgK(),
        TgtHllType.valueOf(getTgtHllType()),
        getMaxIntermediateSize()
    );
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Union.getMaxSerializationBytes(getLgK());
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new HllSketchMergeAggregatorFactory(
        newName,
        getFieldName(),
        getLgK(),
        getTgtHllType(),
        isShouldFinalize(),
        isRound()
    );
  }

}
