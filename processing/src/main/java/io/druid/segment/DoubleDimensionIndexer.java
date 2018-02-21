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

public class DoubleDimensionIndexer implements DimensionIndexer<Double, Double, Double>
{
  public static final Comparator<Double> DOUBLE_COMPARATOR = Comparators.<Double>naturalNullsFirst();

  @Override
  public Double processRowValsToUnsortedEncodedKeyComponent(Object dimValues, boolean reportParseExceptions)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }
    Double ret = DimensionHandlerUtils.convertObjectToDouble(dimValues, reportParseExceptions);
    // remove null -> zero conversion when https://github.com/druid-io/druid/pull/5278 series of patches is merged
    return ret == null ? DimensionHandlerUtils.ZERO_DOUBLE : ret;
  }

  @Override
  public Double getUnsortedEncodedValueFromSorted(Double sortedIntermediateValue)
  {
    return sortedIntermediateValue;
  }

  @Override
  public Indexed<Double> getSortedIndexedValues()
  {
    throw new UnsupportedOperationException("Numeric columns do not support value dictionaries.");
  }

  @Override
  public Double getMinValue()
  {
    return Double.NEGATIVE_INFINITY;
  }

  @Override
  public Double getMaxValue()
  {
    return Double.POSITIVE_INFINITY;
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
    return new DoubleWrappingDimensionSelector(makeColumnValueSelector(currEntry, desc), spec.getExtractionFn());
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerDoubleColumnSelector implements DoubleColumnSelector
    {

      @Override
      public boolean isNull()
      {
        final Object[] dims = currEntry.get().getDims();
        return dimIndex >= dims.length || dims[dimIndex] == null;
      }

      @Override
      public double getDouble()
      {
        final Object[] dims = currEntry.get().getDims();

        if (dimIndex >= dims.length || dims[dimIndex] == null) {
          assert NullHandling.replaceWithDefault();
          return 0.0;
        }
        return (Double) dims[dimIndex];
      }

      @SuppressWarnings("deprecation")
      @Nullable
      @Override
      public Double getObject()
      {
        final Object[] dims = currEntry.get().getDims();

        if (dimIndex >= dims.length) {
          return null;
        }
        return (Double) dims[dimIndex];
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }

    return new IndexerDoubleColumnSelector();
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(@Nullable Double lhs, @Nullable Double rhs)
  {
    return DOUBLE_COMPARATOR.compare(lhs, rhs);
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(@Nullable Double lhs, @Nullable Double rhs)
  {
    return Objects.equals(lhs, rhs);
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable Double key)
  {
    return DimensionHandlerUtils.nullToZero(key).hashCode();
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualArrayOrList(Double key, boolean asList)
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
      Double key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Numeric columns do not support bitmaps.");
  }
}
