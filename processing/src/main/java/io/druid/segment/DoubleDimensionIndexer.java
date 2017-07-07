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
import io.druid.segment.column.ValueType;
import io.druid.segment.data.Indexed;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexStorageAdapter;

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
    return null;
  }

  @Override
  public Double getSortedEncodedValueFromUnsorted(Double unsortedIntermediateValue)
  {
    return null;
  }

  @Override
  public Double getUnsortedEncodedValueFromSorted(Double sortedIntermediateValue)
  {
    return null;
  }

  @Override
  public Indexed<Double> getSortedIndexedValues()
  {
    return null;
  }

  @Override
  public Double getMinValue()
  {
    return null;
  }

  @Override
  public Double getMaxValue()
  {
    return null;
  }

  @Override
  public int getCardinality()
  {
    return 0;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec, IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    return null;
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(
      IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    return null;
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(
      IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    return null;
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(
      DimensionSpec spec, IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    return null;
  }

  @Override
  public DoubleColumnSelector makeDoubleColumnSelector(
      IncrementalIndexStorageAdapter.EntryHolder currEntry, IncrementalIndex.DimensionDesc desc
  )
  {
    return null;
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(Double lhs, Double rhs)
  {
    return 0;
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(Double lhs, Double rhs)
  {
    return false;
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(Double key)
  {
    return 0;
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualArrayOrList(Double key, boolean asList)
  {
    return null;
  }

  @Override
  public Double convertUnsortedEncodedKeyComponentToSortedEncodedKeyComponent(Double key)
  {
    return null;
  }

  @Override
  public void fillBitmapsFromUnsortedEncodedKeyComponent(
      Double key, int rowNum, MutableBitmap[] bitmapIndexes, BitmapFactory factory
  )
  {

  }
}
