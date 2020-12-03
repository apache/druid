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

package org.apache.druid.mapStringString;

import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.MutableBitmap;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRowHolder;

import javax.annotation.Nullable;
import java.util.Map;

public class MapStringStringDimensionIndexer implements DimensionIndexer<MapStringStringRow, MapStringStringRow, MapStringStringRow>
{
  @Override
  public MapStringStringRow processRowValsToUnsortedEncodedKeyComponent(
      @Nullable Object dimValues,
      boolean reportParseExceptions
  )
  {
    if (dimValues == null) {
      return MapStringStringRow.EMPTY_INSTANCE;
    } else if (dimValues instanceof MapStringStringRow) {
      MapStringStringRow row = (MapStringStringRow) dimValues;
      return row;
    } else if (dimValues instanceof Map) {
      MapStringStringRow row = MapStringStringRow.create((Map) dimValues);
      return row;
    } else {
      throw new IAE("Unsupported type");
    }
  }

  @Override
  public void setSparseIndexed()
  {
  }

  @Override
  public long estimateEncodedKeyComponentSize(MapStringStringRow key)
  {
    return key == null ? 0 : key.getEstimatedOnHeapSize();
  }

  @Override
  public MapStringStringRow getUnsortedEncodedValueFromSorted(MapStringStringRow sortedIntermediateValue)
  {
    return sortedIntermediateValue;
  }

  @Override
  public CloseableIndexed<MapStringStringRow> getSortedIndexedValues()
  {
    // no dictionary
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public MapStringStringRow getMinValue()
  {
    // This column is only used via VirtualColumn, so we should never reach here.
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public MapStringStringRow getMaxValue()
  {
    // This column is only used via VirtualColumn, so we should never reach here.
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public int getCardinality()
  {
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public ColumnCapabilities getColumnCapabilities()
  {
    return new ColumnCapabilitiesImpl().setType(ValueType.COMPLEX)
                                       .setComplexTypeName(MapStringStringDruidModule.TYPE_NAME);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec spec,
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    throw new UnsupportedOperationException("Not supported");
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      IncrementalIndexRowHolder currEntry,
      IncrementalIndex.DimensionDesc desc
  )
  {
    final int dimIndex = desc.getIndex();
    return new ObjectColumnSelector<MapStringStringRow>()
    {
      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Nullable
      @Override
      public MapStringStringRow getObject()
      {
        return (MapStringStringRow) currEntry.get().getDims()[dimIndex];
      }

      @Override
      public Class<MapStringStringRow> classOfObject()
      {
        return MapStringStringRow.class;
      }
    };
  }

  @Override
  public int compareUnsortedEncodedKeyComponents(
      @Nullable MapStringStringRow lhs,
      @Nullable MapStringStringRow rhs
  )
  {
    return MapStringStringRow.COMPARATOR.compare(lhs, rhs);
  }

  @Override
  public boolean checkUnsortedEncodedKeyComponentsEqual(
      @Nullable MapStringStringRow lhs,
      @Nullable MapStringStringRow rhs
  )
  {
    return MapStringStringRow.COMPARATOR.compare(lhs, rhs) == 0;
  }

  @Override
  public int getUnsortedEncodedKeyComponentHashCode(@Nullable MapStringStringRow key)
  {
    return key == null ? 0 : key.hashCode();
  }

  @Override
  public Object convertUnsortedEncodedKeyComponentToActualList(MapStringStringRow key)
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
      MapStringStringRow key,
      int rowNum, MutableBitmap[] bitmapIndexes,
      BitmapFactory factory
  )
  {
    throw new UnsupportedOperationException("Not supported");
  }
}
