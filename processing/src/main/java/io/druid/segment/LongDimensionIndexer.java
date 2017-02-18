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

public class LongDimensionIndexer implements DimensionIndexer<Long, Long, Long>
{
  @Override
  public Long processRowValsToUnsortedEncodedArray(Object dimValues)
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
    return null;
  }

  @Override
  public Long getMinValue()
  {
    return 0L;
  }

  @Override
  public Long getMaxValue()
  {
    return 0L;
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
    class IndexerLongColumnSelector implements LongColumnSelector
    {
      @Override
      public long get()
      {
        final Object[] dims = currEntry.getKey().getDims();

        if (dimIndex >= dims.length) {
          return 0L;
        }

        return (Long) dims[dimIndex];
      }
    }

    return new IndexerLongColumnSelector();
  }

  @Override
  public int compareUnsortedEncodedArrays(Long lhs, Long rhs)
  {
    return lhs.compareTo(rhs);
  }

  @Override
  public boolean checkUnsortedEncodedArraysEqual(Long lhs, Long rhs)
  {
    return lhs.equals(rhs);
  }

  @Override
  public int getUnsortedEncodedArrayHashCode(Long key)
  {
    return key.hashCode();
  }

  @Override
  public Object convertUnsortedEncodedArrayToActualArrayOrList(Long key, boolean asList)
  {
    return Lists.newArrayList(key);
  }

  @Override
  public Long convertUnsortedEncodedArrayToSortedEncodedArray(Long key)
  {
    return key;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedArray(
      Long key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {
    // longs don't have bitmaps
  }
}
