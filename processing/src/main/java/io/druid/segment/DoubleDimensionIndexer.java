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
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;

import javax.annotation.Nullable;
import java.util.List;

public class DoubleDimensionIndexer implements DimensionIndexer<Double, Double, Double>
{
  @Override
  public ValueType getValueType()
  {
    return ValueType.DOUBLE;
  }

  @Override
  public Double processRowValsToUnsortedEncodedKeyComponent(Object dimValues)
  {
    if (dimValues instanceof List) {
      throw new UnsupportedOperationException("Numeric columns do not support multivalue rows.");
    }
    return DimensionHandlerUtils.convertObjectToDouble(dimValues);
  }

  @Override
  public Double getSortedEncodedValueFromUnsorted(Double unsortedIntermediateValue)
  {
    return unsortedIntermediateValue;
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
      DimensionSpec spec, IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    return new DoubleWrappingDimensionSelector(
        makeDoubleColumnSelector(currEntry, desc),
        spec.getExtractionFn()
    );
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(
      IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
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

        double doubleValue = (Double) dims[dimIndex];
        return (long) doubleValue;
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
  public FloatColumnSelector makeFloatColumnSelector(
      IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
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
          return 0.0f;
        }

        double doubleValue = (Double) dims[dimIndex];
        return (float) doubleValue;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // nothing to inspect
      }
    }

    return new IndexerFloatColumnSelector();
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(
      DimensionSpec spec, IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerObjectColumnSelector implements ObjectColumnSelector
    {
      @Override
      public Class classOfObject()
      {
        return Double.class;
      }

      @Override
      public Object get()
      {
        final Object[] dims = currEntry.getKey().getDims();

        if (dimIndex >= dims.length) {
          return DimensionHandlerUtils.ZERO_DOUBLE;
        }

        return dims[dimIndex];
      }
    }

    return new IndexerObjectColumnSelector();
  }

  @Override
  public DoubleColumnSelector makeDoubleColumnSelector(
      IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerDoubleColumnSelector implements DoubleColumnSelector
    {
      @Override
      public double get()
      {
        final Object[] dims = currEntry.getKey().getDims();

        if (dimIndex >= dims.length) {
          return 0.0;
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
    return Double.compare(DimensionHandlerUtils.nullToZero(lhs), DimensionHandlerUtils.nullToZero(rhs));
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(@Nullable Double lhs, @Nullable Double rhs)
  {
    return DimensionHandlerUtils.nullToZero(lhs).equals(DimensionHandlerUtils.nullToZero(rhs));
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
  public Double convertUnsortedEncodedKeyComponentToSortedEncodedKeyComponent(Double key)
  {
    return key;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      Double key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Numeric columns do not support bitmaps.");
  }
}
