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
import io.druid.common.config.NullHandling;
import io.druid.java.util.common.guava.Comparators;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexRowHolder;

import javax.annotation.Nullable;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class LongDimensionIndexer implements DimensionIndexer<Long, Long, Long>
{
  public static final Comparator LONG_COMPARATOR = Comparators.<Long>naturalNullsFirst();

  @Override
  public Long processRowValsToUnsortedEncodedKeyComponent(Object dimValues, boolean reportParseExceptions)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }

    Long ret = DimensionHandlerUtils.convertObjectToLong(dimValues, reportParseExceptions);
    // remove null -> zero conversion when https://github.com/druid-io/druid/pull/5278 series of patches is merged
    return ret == null ? DimensionHandlerUtils.ZERO_LONG : ret;
  }

  @Override
  public long estimateEncodedKeyComponentSize(Long key)
  {
    return Long.BYTES;
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
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    return new LongWrappingDimensionSelector(makeColumnValueSelector(currEntry, desc), spec.getExtractionFn());
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerLongColumnSelector implements LongColumnSelector
    {

      @Override
      public boolean isNull()
      {
        final Object[] dims = currEntry.get().getDims();
        return dimIndex >= dims.length || dims[dimIndex] == null;
      }

      @Override
      public long getLong()
      {
        final Object[] dims = currEntry.get().getDims();

        if (dimIndex >= dims.length || dims[dimIndex] == null) {
          assert NullHandling.replaceWithDefault();
          return 0;
        }

        return (Long) dims[dimIndex];
      }

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Long getObject()
      {
        final Object[] dims = currEntry.get().getDims();

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
    return LONG_COMPARATOR.compare(lhs, rhs);
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(@Nullable Long lhs, @Nullable Long rhs)
  {
    return Objects.equals(lhs, rhs);
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
  public ColumnValueSelector convertUnsortedValuesToSorted(ColumnValueSelector selectorWithUnsortedValues)
  {
    return selectorWithUnsortedValues;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      Long key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Numeric columns do not support bitmaps.");
  }
}
