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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;

/**
 * This aggregator factory is for building sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 */
public class HllSketchBuildAggregatorFactory extends HllSketchAggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex(HllSketchModule.BUILD_TYPE_NAME);

  @JsonCreator
  public HllSketchBuildAggregatorFactory(
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
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

  @Override
  protected byte getCacheTypeId()
  {
    return AggregatorUtil.HLL_SKETCH_BUILD_CACHE_TYPE_ID;
  }

  @Override
  public Aggregator factorize(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<Object> selector = columnSelectorFactory.makeColumnValueSelector(getFieldName());
    validateInputs(columnSelectorFactory.getColumnCapabilities(getFieldName()));
    return new HllSketchBuildAggregator(selector, getLgK(), TgtHllType.valueOf(getTgtHllType()));
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnValueSelector<Object> selector = columnSelectorFactory.makeColumnValueSelector(getFieldName());
    validateInputs(columnSelectorFactory.getColumnCapabilities(getFieldName()));
    return new HllSketchBuildBufferAggregator(
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
    validateInputs(selectorFactory.getColumnCapabilities(getFieldName()));
    return new HllSketchBuildVectorAggregator(
        selectorFactory,
        getFieldName(),
        getLgK(),
        TgtHllType.valueOf(getTgtHllType()),
        getMaxIntermediateSize()
    );
  }

  /**
   * For the HLL_4 sketch type, this value can be exceeded slightly in extremely rare cases.
   * The sketch will request on-heap memory and move there. It is handled in HllSketchBuildBufferAggregator.
   */
  @Override
  public int getMaxIntermediateSize()
  {
    return HllSketch.getMaxUpdatableSerializationBytes(getLgK(), TgtHllType.valueOf(getTgtHllType()));
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new HllSketchBuildAggregatorFactory(
        newName,
        getFieldName(),
        getLgK(),
        getTgtHllType(),
        isShouldFinalize(),
        isRound()
    );
  }

  private void validateInputs(@Nullable ColumnCapabilities capabilities)
  {
    if (capabilities != null) {
      if (capabilities.is(ValueType.COMPLEX)) {
        throw new ISE(
            "Invalid input [%s] of type [%s] for [%s] aggregator [%s]",
            getFieldName(),
            capabilities.asTypeString(),
            HllSketchModule.BUILD_TYPE_NAME,
            getName()
        );
      }
    }
  }

}
