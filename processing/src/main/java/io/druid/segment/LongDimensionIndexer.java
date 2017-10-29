/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.TimeAndDimsHolder;

import javax.annotation.Nullable;
import java.util.List;

public class LongDimensionIndexer implements DimensionIndexer<Long, Long, Long>
{
  @Override
  public ValueType getValueType()
  {
    return ValueType.LONG;
  }

  @Override
  public Long processRowValsToUnsortedEncodedKeyComponent(Object dimValues)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }

    return DimensionHandlerUtils.convertObjectToLong(dimValues);
  }

  @Override
  public Long getSortedEncodedValueFromUnsorted(Long unsortedIntermediateValue)
  {
    return unsortedIntermediateValue;
  }

  @Override
  public Long getUnsortedEncodedValueFromSorted(Long sortedIntermediateValue)
  {
    return sortedIntermediateValue;
  }

  @Override
  public Indexed<Long> getSortedIndexedValues()
  {
    throw new UnsupportedOperationException("Numeric columns do not support value dictionaries.");
  }

  @Override
  public Long getMinValue()
  {
    return Long.MIN_VALUE;
  }

  @Override
  public Long getMaxValue()
  {
    return Long.MAX_VALUE;
  }

  @Override
  public int getCardinality()
  {
    return DimensionSelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec,
      TimeAndDimsHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    return new LongWrappingDimensionSelector(makeColumnValueSelector(currEntry, desc), spec.getExtractionFn());
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      TimeAndDimsHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerLongColumnSelector implements LongColumnSelector
    {
      @Override
      public long getLong()
      {
        final Object[] dims = currEntry.getKey().getDims();

        if (dimIndex >= dims.length) {
          return 0L;
        }

        return (Long) dims[dimIndex];
      }

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Long getObject()
      {
        final Object[] dims = currEntry.getKey().getDims();

        if (dimIndex >= dims.length) {
          return null;
        }

        return (Long) dims[dimIndex];
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }

    return new IndexerLongColumnSelector();
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(@Nullable Long lhs, @Nullable Long rhs)
  {
    return DimensionHandlerUtils.nullToZero(lhs).compareTo(DimensionHandlerUtils.nullToZero(rhs));
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(@Nullable Long lhs, @Nullable Long rhs)
  {
    return DimensionHandlerUtils.nullToZero(lhs).equals(DimensionHandlerUtils.nullToZero(rhs));
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable Long key)
  {
    return DimensionHandlerUtils.nullToZero(key).hashCode();
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualArrayOrList(Long key, boolean asList)
  {
    return key;
  }

  @Override
  public Long convertUnsortedEncodedKeyComponentToSortedEncodedKeyComponent(Long key)
  {
    return key;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      Long key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Numeric columns do not support bitmaps.");
  }
}
