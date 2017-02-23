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

import com.google.common.collect.Lists;
import io.druid.collections.bitmap.BitmapFactory;
import io.druid.collections.bitmap.MutableBitmap;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;

import java.util.List;

public class FloatDimensionIndexer implements DimensionIndexer<Float, Float, Float>
{
  @Override
  public Float processRowValsToUnsortedEncodedKeyComponent(Object dimValues)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }

    return DimensionHandlerUtils.convertObjectToFloat(dimValues);
  }

  @Override
  public Float getSortedEncodedValueFromUnsorted(Float unsortedIntermediateValue)
  {
    return unsortedIntermediateValue;
  }

  @Override
  public Float getUnsortedEncodedValueFromSorted(Float sortedIntermediateValue)
  {
    return sortedIntermediateValue;
  }

  @Override
  public Indexed<Float> getSortedIndexedValues()
  {
    return null;
  }

  @Override
  public Float getMinValue()
  {
    return 0.0f;
  }

  @Override
  public Float getMaxValue()
  {
    return 0.0f;
  }

  @Override
  public int getCardinality()
  {
    return DimensionSelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(
      final DimensionSpec spec,
      final IncrementalIndexStorageAdapter.EntryHolder currEntry,
      final IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerFloatColumnSelector implements FloatColumnSelector
    {
      @Override
      public float get()
      {
        final Object[] dims = currEntry.getKey().getDims();

        if (dimIndex >= dims.length) {
          return 0L;
        }

        return (Float) dims[dimIndex];
      }
    }

    return new IndexerFloatColumnSelector();
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(Float lhs, Float rhs)
  {
    return lhs.compareTo(rhs);
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(Float lhs, Float rhs)
  {
    return lhs.equals(rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(Float key)
  {
    return key.hashCode();
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualArrayOrList(Float key, boolean asList)
  {
    return Lists.newArrayList(key);
  }

  @Override
  public Float convertUnsortedEncodedKeyComponentToSortedEncodedKeyComponent(Float key)
  {
    return key;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      Float key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {
    // floats don't have bitmaps
  }
}
