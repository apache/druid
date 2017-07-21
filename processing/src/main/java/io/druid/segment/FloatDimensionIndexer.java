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

public class FloatDimensionIndexer implements DimensionIndexer<Float, Float, Float>
{
  @Override
  public ValueType getValueType()
  {
    return ValueType.FLOAT;
  }

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
    throw new UnsupportedOperationException("Numeric columns do not support value dictionaries.");
  }

  @Override
  public Float getMinValue()
  {
    return Float.NEGATIVE_INFINITY;
  }

  @Override
  public Float getMaxValue()
  {
    return Float.POSITIVE_INFINITY;
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
    return new FloatWrappingDimensionSelector(
        makeFloatColumnSelector(currEntry, desc),
        spec.getExtractionFn()
    );
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(
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

        float floatVal = (Float) dims[dimIndex];
        return (long) floatVal;
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
          return 0.0f;
        }

        return (Float) dims[dimIndex];
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
      final DimensionSpec spec,
      final IncrementalIndexStorageAdapter.EntryHolder currEntry,
      final IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    class IndexerObjectColumnSelector implements ObjectColumnSelector
    {
      @Override
      public Class classOfObject()
      {
        return Float.class;
      }

      @Override
      public Object get()
      {
        final Object[] dims = currEntry.getKey().getDims();

        if (dimIndex >= dims.length) {
          return DimensionHandlerUtils.ZERO_FLOAT;
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
        float floatVal = (Float) dims[dimIndex];
        return (double) floatVal;
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
  public int compareUnsortedEncodedKeyComponents(@Nullable Float lhs, @Nullable Float rhs)
  {
    return DimensionHandlerUtils.nullToZero(lhs).compareTo(DimensionHandlerUtils.nullToZero(rhs));
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(@Nullable Float lhs, @Nullable Float rhs)
  {
    return DimensionHandlerUtils.nullToZero(lhs).equals(DimensionHandlerUtils.nullToZero(rhs));
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable Float key)
  {
    return DimensionHandlerUtils.nullToZero(key).hashCode();
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualArrayOrList(Float key, boolean asList)
  {
    return key;
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
    throw new UnsupportedOperationException("Numeric columns do not support bitmaps.");
  }
}
