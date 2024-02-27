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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorUtil;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * This aggregator factory is for building sketches from raw data.
 * The input column can contain identifiers of type string, char[], byte[] or any numeric type.
 */
@SuppressWarnings("NullableProblems")
public class HllSketchBuildAggregatorFactory extends HllSketchAggregatorFactory
{
  public static final ColumnType TYPE = ColumnType.ofComplex(HllSketchModule.BUILD_TYPE_NAME);

  @JsonCreator
  public HllSketchBuildAggregatorFactory(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("lgK") @Nullable final Integer lgK,
      @JsonProperty("tgtHllType") @Nullable final String tgtHllType,
      @JsonProperty("stringEncoding") @Nullable final StringEncoding stringEncoding,
      @JsonProperty("shouldFinalize") final Boolean shouldFinalize,
      @JsonProperty("round") final boolean round
  )
  {
    super(name, fieldName, lgK, tgtHllType, stringEncoding, shouldFinalize, round);
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
    return new HllSketchBuildAggregator(
        formulateSketchUpdater(columnSelectorFactory),
        getLgK(),
        TgtHllType.valueOf(getTgtHllType())
    );
  }

  @Override
  public BufferAggregator factorizeBuffered(final ColumnSelectorFactory columnSelectorFactory)
  {
    return new HllSketchBuildBufferAggregator(
        formulateSketchUpdater(columnSelectorFactory),
        getLgK(),
        TgtHllType.valueOf(getTgtHllType()),
        getStringEncoding(),
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

    return HllSketchBuildVectorAggregator.create(
        selectorFactory,
        getFieldName(),
        getLgK(),
        TgtHllType.valueOf(getTgtHllType()),
        getStringEncoding(),
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
        getStringEncoding(),
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

  private HllSketchUpdater formulateSketchUpdater(ColumnSelectorFactory columnSelectorFactory)
  {
    final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(getFieldName());
    validateInputs(capabilities);

    HllSketchUpdater updater = null;
    if (capabilities != null &&
        StringEncoding.UTF8.equals(getStringEncoding()) && ValueType.STRING.equals(capabilities.getType())) {
      final DimensionSelector selector = columnSelectorFactory.makeDimensionSelector(
          DefaultDimensionSpec.of(getFieldName())
      );

      if (selector.supportsLookupNameUtf8()) {
        updater = sketch -> {
          final IndexedInts row = selector.getRow();
          final int sz = row.size();

          for (int i = 0; i < sz; i++) {
            final ByteBuffer buf = selector.lookupNameUtf8(row.get(i));

            if (buf != null) {
              sketch.get().update(buf);
            }
          }
        };
      }
    }

    if (updater == null) {
      @SuppressWarnings("unchecked")
      final ColumnValueSelector<Object> selector = columnSelectorFactory.makeColumnValueSelector(getFieldName());
      final ValueType type;

      if (capabilities == null) {
        // When ingesting data, the columnSelectorFactory returns null for column capabilities, so this doesn't
        // necessarily mean that the column doesn't exist.  We thus need to be prepared to accept anything in this
        // case.  As such, we pretend like the input is COMPLEX to get the logic to use the object-based aggregation
        type = ValueType.COMPLEX;
      } else {
        type = capabilities.getType();
      }


      switch (type) {
        case LONG:
          updater = sketch -> {
            if (!selector.isNull()) {
              sketch.get().update(selector.getLong());
            }
          };
          break;
        case FLOAT:
        case DOUBLE:
          updater = sketch -> {
            if (!selector.isNull()) {
              sketch.get().update(selector.getDouble());
            }
          };
          break;
        case ARRAY:
          throw InvalidInput.exception("ARRAY types are not supported for hll sketch");
        default:
          updater = sketch -> {
            Object obj = selector.getObject();
            if (obj != null) {
              HllSketchBuildUtil.updateSketch(sketch.get(), getStringEncoding(), obj);
            }
          };
      }
    }
    return updater;
  }
}
