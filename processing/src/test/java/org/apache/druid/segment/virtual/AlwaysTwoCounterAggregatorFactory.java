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

package org.apache.druid.segment.virtual;

import com.google.common.collect.ImmutableList;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.CountVectorAggregator;
import org.apache.druid.query.aggregation.VectorAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.junit.Assert;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Specialized aggregator factory for testing the selectors produced by {@link AlwaysTwoVectorizedVirtualColumn}, used
 * for counting the number of values read so that tests can ensure the correct number of values have been processed. A
 * {@link AlwaysTwoCounterVectorAggregator} will be produced, backed by a type appropriate selector for a given
 * {@link ColumnCapabilities}.
 */
public class AlwaysTwoCounterAggregatorFactory extends CountAggregatorFactory
{
  private final String fieldName;

  public AlwaysTwoCounterAggregatorFactory(String name, String field)
  {
    super(name);
    this.fieldName = field;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    throw new IllegalStateException(AlwaysTwoVectorizedVirtualColumn.DONT_CALL_THIS);
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    throw new IllegalStateException(AlwaysTwoVectorizedVirtualColumn.DONT_CALL_THIS);
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory selectorFactory)
  {
    ColumnCapabilities capabilities = selectorFactory.getColumnCapabilities(fieldName);
    switch (capabilities.getType()) {
      case LONG:
      case DOUBLE:
      case FLOAT:
        return new AlwaysTwoCounterVectorAggregator(selectorFactory.makeValueSelector(fieldName));
      case STRING:
        if (capabilities.isDictionaryEncoded().isTrue()) {
          if (capabilities.hasMultipleValues().isTrue()) {
            return new AlwaysTwoCounterVectorAggregator(selectorFactory.makeMultiValueDimensionSelector(
                DefaultDimensionSpec.of(fieldName)));
          }
          return new AlwaysTwoCounterVectorAggregator(selectorFactory.makeSingleValueDimensionSelector(
              DefaultDimensionSpec.of(fieldName)));
        }
        return new AlwaysTwoCounterVectorAggregator(selectorFactory.makeObjectSelector(fieldName));
      default:
        throw new IllegalStateException("how did this happen");
    }
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new AlwaysTwoCounterAggregatorFactory(newName, fieldName);
  }

  public static class AlwaysTwoCounterVectorAggregator extends CountVectorAggregator
  {
    @Nullable
    private final VectorValueSelector valueSelector;
    @Nullable
    private final VectorObjectSelector objectSelector;
    @Nullable
    private final SingleValueDimensionVectorSelector singleValueDimensionSelector;
    @Nullable
    private final MultiValueDimensionVectorSelector multiValueDimensionSelector;

    AlwaysTwoCounterVectorAggregator(VectorValueSelector valueSelector)
    {
      this(valueSelector, null, null, null);
    }

    AlwaysTwoCounterVectorAggregator(VectorObjectSelector objectSelector)
    {
      this(null, objectSelector, null, null);
    }

    AlwaysTwoCounterVectorAggregator(SingleValueDimensionVectorSelector dimSelector)
    {
      this(null, null, dimSelector, null);
    }

    AlwaysTwoCounterVectorAggregator(MultiValueDimensionVectorSelector dimSelector)
    {
      this(null, null, null, dimSelector);
    }

    AlwaysTwoCounterVectorAggregator(
        @Nullable VectorValueSelector valueSelector,
        @Nullable VectorObjectSelector objectSelector,
        @Nullable SingleValueDimensionVectorSelector singleValueDimensionSelector,
        @Nullable MultiValueDimensionVectorSelector multiValueDimensionSelector
    )
    {
      this.valueSelector = valueSelector;
      this.objectSelector = objectSelector;
      this.singleValueDimensionSelector = singleValueDimensionSelector;
      this.multiValueDimensionSelector = multiValueDimensionSelector;
    }

    @Override
    public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
    {
      if (valueSelector != null) {
        final long[] vector = valueSelector.getLongVector();

        long count = 0;
        for (int i = startRow; i < endRow; i++) {
          Assert.assertEquals(2L, vector[i]);
          count += 1;
        }

        buf.putLong(position, buf.getLong(position) + count);
        return;
      }
      if (objectSelector != null) {
        final Object[] vector = objectSelector.getObjectVector();

        long count = 0;
        for (int i = startRow; i < endRow; i++) {
          if (vector[i] instanceof List) {
            Assert.assertEquals(ImmutableList.of("2", "2"), vector[i]);
            count += 2;
          } else {
            Assert.assertEquals("2", vector[i]);
            count += 1;
          }
        }

        buf.putLong(position, buf.getLong(position) + count);
        return;
      }
      if (singleValueDimensionSelector != null) {
        final int[] rowVector = singleValueDimensionSelector.getRowVector();

        long count = 0;
        for (int i = startRow; i < endRow; i++) {
          Assert.assertEquals("2", singleValueDimensionSelector.lookupName(rowVector[i]));
          count += 1;
        }

        buf.putLong(position, buf.getLong(position) + count);
        return;
      }
      if (multiValueDimensionSelector != null) {
        final IndexedInts[] rowVector = multiValueDimensionSelector.getRowVector();

        long count = 0;
        for (int i = startRow; i < endRow; i++) {
          //noinspection SSBasedInspection
          for (int j = 0; j < rowVector[i].size(); j++) {
            Assert.assertEquals("2", multiValueDimensionSelector.lookupName(rowVector[i].get(j)));
            count += 1;
          }
        }

        buf.putLong(position, buf.getLong(position) + count);
        return;
      }
      throw new IllegalStateException("how did this happen");
    }

    @Override
    public void aggregate(
        ByteBuffer buf,
        int numRows,
        int[] positions,
        @Nullable int[] rows,
        int positionOffset
    )
    {
      if (valueSelector != null) {
        final long[] vector = valueSelector.getLongVector();

        for (int i = 0; i < numRows; i++) {
          final int position = positions[i] + positionOffset;
          Assert.assertEquals(2L, vector[i]);
          buf.putLong(position, buf.getLong(position) + 1);
        }
        return;
      }
      if (objectSelector != null) {
        final Object[] vector = objectSelector.getObjectVector();
        for (int i = 0; i < numRows; i++) {
          final int position = positions[i] + positionOffset;
          if (vector[i] instanceof List) {
            Assert.assertEquals(ImmutableList.of("2", "2"), vector[i]);
            buf.putLong(position, buf.getLong(position) + 2);
          } else {
            Assert.assertEquals("2", vector[i]);
            buf.putLong(position, buf.getLong(position) + 1);
          }
        }
        return;
      }
      if (singleValueDimensionSelector != null) {
        final int[] rowVector = singleValueDimensionSelector.getRowVector();
        for (int i = 0; i < numRows; i++) {
          final int position = positions[i] + positionOffset;
          Assert.assertEquals("2", singleValueDimensionSelector.lookupName(rowVector[i]));
          buf.putLong(position, buf.getLong(position) + 1);
        }
        return;
      }
      if (multiValueDimensionSelector != null) {
        final IndexedInts[] rowVector = multiValueDimensionSelector.getRowVector();
        for (int i = 0; i < numRows; i++) {
          final int position = positions[i] + positionOffset;
          //noinspection SSBasedInspection
          for (int j = 0; j < rowVector[i].size(); j++) {
            Assert.assertEquals("2", multiValueDimensionSelector.lookupName(rowVector[i].get(j)));
            buf.putLong(position, buf.getLong(position) + 1);
          }
        }
        return;
      }
      throw new IllegalStateException("how did this happen");
    }
  }
}
