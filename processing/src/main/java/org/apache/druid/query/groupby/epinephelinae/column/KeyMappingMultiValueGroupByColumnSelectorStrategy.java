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

package org.apache.druid.query.groupby.epinephelinae.column;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuildingUtils;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionDictionary;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.IntFunction;

/**
 * Like {@link KeyMappingGroupByColumnSelectorStrategy}, but for multi-value dimensions, i.e. strings. It can only handle
 * {@link DimensionSelector}
 */
public abstract class KeyMappingMultiValueGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{

  public static GroupByColumnSelectorStrategy create(
      ColumnCapabilities capabilities,
      DimensionSelector dimensionSelector
  )
  {
    if (dimensionSelector.getValueCardinality() >= 0 && dimensionSelector.nameLookupPossibleInAdvance()) {
      return new PrebuiltDictionary(capabilities, dimensionSelector::lookupName);
    }
    return new DictionaryBuilding();
  }

  @Override
  public int getGroupingKeySizeBytes()
  {
    return Integer.BYTES;
  }


  @Override
  public void initGroupingKeyColumnValue(
      int keyBufferPosition,
      int dimensionIndex,
      Object rowObj,
      ByteBuffer keyBuffer,
      int[] stack
  )
  {
    IndexedInts row = (IndexedInts) rowObj;
    int rowSize = row.size();

    keyBuffer.putInt(
        keyBufferPosition,
        rowSize == 0 ? GROUP_BY_MISSING_VALUE : row.get(0)
    );

    stack[dimensionIndex] = rowSize == 0 ? 0 : 1;
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition,
      Object rowObj,
      int rowValIdx,
      ByteBuffer keyBuffer
  )
  {
    IndexedInts row = (IndexedInts) rowObj;
    int rowSize = row.size();

    if (rowValIdx < rowSize) {
      keyBuffer.putInt(
          keyBufferPosition,
          row.get(rowValIdx)
      );
      return true;
    } else {
      return false;
    }
  }

  public static class PrebuiltDictionary extends KeyMappingMultiValueGroupByColumnSelectorStrategy
  {
    private final ColumnCapabilities capabilities;
    private final IntFunction<String> dictionaryLookup;

    public PrebuiltDictionary(
        ColumnCapabilities capabilities,
        IntFunction<String> dictionaryLookup
    )
    {
      this.capabilities = capabilities;
      this.dictionaryLookup = dictionaryLookup;
    }

    @Override
    public void processValueFromGroupingKey(
        GroupByColumnSelectorPlus selectorPlus,
        ByteBuffer key, ResultRow resultRow,
        int keyBufferPosition
    )
    {
      final int id = key.getInt(keyBufferPosition);
      if (id != GROUP_BY_MISSING_VALUE) {
        resultRow.set(
            selectorPlus.getResultRowPosition(),
            ((DimensionSelector) selectorPlus.getSelector()).lookupName(id)
        );
      } else {
        // Since this is used for String dimensions only, we can directly put the default string value here
        resultRow.set(selectorPlus.getResultRowPosition(), NullHandling.defaultStringValue());
      }
    }

    @Override
    public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
    {
      DimensionSelector dimSelector = (DimensionSelector) selector;
      IndexedInts row = dimSelector.getRow();
      valuess[columnIndex] = row;
      return 0;
    }

    @Override
    public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
    {
      final DimensionSelector dimSelector = (DimensionSelector) selector;
      final IndexedInts row = dimSelector.getRow();
      Preconditions.checkState(row.size() < 2, "Not supported for multi-value dimensions");
      final int dictId = row.size() == 1 ? row.get(0) : GROUP_BY_MISSING_VALUE;
      keyBuffer.putInt(keyBufferPosition, dictId);
      return 0;
    }

    @Override
    public Grouper.BufferComparator bufferComparator(
        int keyBufferPosition,
        @Nullable StringComparator stringComparator
    )
    {
      final boolean canCompareInts =
          capabilities != null &&
          capabilities.isDictionaryEncoded().and(
              capabilities.areDictionaryValuesSorted().and(
                  capabilities.areDictionaryValuesUnique()
              )
          ).isTrue();

      final StringComparator comparator = stringComparator == null ? StringComparators.LEXICOGRAPHIC : stringComparator;
      if (canCompareInts && StringComparators.LEXICOGRAPHIC.equals(comparator)) {
        return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Integer.compare(
            lhsBuffer.getInt(lhsPosition + keyBufferPosition),
            rhsBuffer.getInt(rhsPosition + keyBufferPosition)
        );
      } else {
        Preconditions.checkState(dictionaryLookup != null, "null dictionary lookup");
        return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
          String lhsStr = dictionaryLookup.apply(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
          String rhsStr = dictionaryLookup.apply(rhsBuffer.getInt(rhsPosition + keyBufferPosition));
          return comparator.compare(lhsStr, rhsStr);
        };
      }
    }

    @Override
    public void reset()
    {
      // Nothing to do.
    }
  }

  public static class DictionaryBuilding extends KeyMappingMultiValueGroupByColumnSelectorStrategy
  {

    private final List<String> dictionary = DictionaryBuildingUtils.createDictionary();
    private final Object2IntMap<String> reverseDictionary = DictionaryBuildingUtils.createReverseDictionary(ColumnType.STRING.getNullableStrategy());

    @Override
    public void processValueFromGroupingKey(
        GroupByColumnSelectorPlus selectorPlus,
        ByteBuffer key,
        ResultRow resultRow,
        int keyBufferPosition
    )
    {
      final int id = key.getInt(keyBufferPosition);

      // GROUP_BY_MISSING_VALUE is used to indicate empty rows, which are omitted from the result map.
      if (id != GROUP_BY_MISSING_VALUE) {
        final String value = dictionary.get(id);
        resultRow.set(selectorPlus.getResultRowPosition(), value);
      } else {
        resultRow.set(selectorPlus.getResultRowPosition(), NullHandling.defaultStringValue());
      }
    }

    @Override
    public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
    {
      final DimensionSelector dimSelector = (DimensionSelector) selector;
      final IndexedInts row = dimSelector.getRow();
      int stateFootprintIncrease = 0;
      ArrayBasedIndexedInts newRow = (ArrayBasedIndexedInts) valuess[columnIndex];
      if (newRow == null) {
        newRow = new ArrayBasedIndexedInts();
        valuess[columnIndex] = newRow;
      }
      int rowSize = row.size();
      newRow.ensureSize(rowSize);
      for (int i = 0; i < rowSize; i++) {
        final String value = dimSelector.lookupName(row.get(i));
        final int dictId = reverseDictionary.getInt(value);
        if (dictId < 0) {
          final int nextId = dictionary.size();
          dictionary.add(value);
          reverseDictionary.put(value, nextId);
          newRow.setValue(i, nextId);
          stateFootprintIncrease +=
              DictionaryBuildingUtils.estimateEntryFootprint((value == null ? 0 : value.length()) * Character.BYTES);
        } else {
          newRow.setValue(i, dictId);
        }
      }
      newRow.setSize(rowSize);
      return stateFootprintIncrease;
    }

    @Override
    public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
    {
      final DimensionSelector dimSelector = (DimensionSelector) selector;
      final IndexedInts row = dimSelector.getRow();

      Preconditions.checkState(row.size() < 2, "Not supported for multi-value dimensions");

      if (row.size() == 0) {
        keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
        return 0;
      }

      final String value = dimSelector.lookupName(row.get(0));
      final int dictId = reverseDictionary.getInt(value);
      if (dictId == DimensionDictionary.ABSENT_VALUE_ID) {
        final int nextId = dictionary.size();
        dictionary.add(value);
        reverseDictionary.put(value, nextId);
        keyBuffer.putInt(keyBufferPosition, nextId);
        return DictionaryBuildingUtils.estimateEntryFootprint((value == null ? 0 : value.length()) * Character.BYTES);
      } else {
        keyBuffer.putInt(keyBufferPosition, dictId);
        return 0;
      }
    }

    @Override
    public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
    {
      final StringComparator realComparator = stringComparator == null ?
                                              StringComparators.LEXICOGRAPHIC :
                                              stringComparator;
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        String lhsStr = dictionary.get(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
        String rhsStr = dictionary.get(rhsBuffer.getInt(rhsPosition + keyBufferPosition));
        return realComparator.compare(lhsStr, rhsStr);
      };
    }

    @Override
    public void reset()
    {
      dictionary.clear();
      reverseDictionary.clear();
    }
  }
}
