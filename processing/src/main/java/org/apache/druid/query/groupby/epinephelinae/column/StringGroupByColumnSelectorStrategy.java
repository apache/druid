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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.IntFunction;

public class StringGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{
  @Nullable
  private final ColumnCapabilities capabilities;

  @Nullable
  private final IntFunction<String> dictionaryLookup;

  public StringGroupByColumnSelectorStrategy(IntFunction<String> dictionaryLookup, ColumnCapabilities capabilities)
  {
    this.dictionaryLookup = dictionaryLookup;
    this.capabilities = capabilities;
  }

  @Override
  public int getGroupingKeySize()
  {
    return Integer.BYTES;
  }

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
      resultRow.set(
          selectorPlus.getResultRowPosition(),
          ((DimensionSelector) selectorPlus.getSelector()).lookupName(id)
      );
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), NullHandling.defaultStringValue());
    }
  }

  @Override
  public void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    DimensionSelector dimSelector = (DimensionSelector) selector;
    IndexedInts row = dimSelector.getRow();
    valuess[columnIndex] = row;
  }

  @Override
  public Object getOnlyValue(ColumnValueSelector selector)
  {
    final DimensionSelector dimSelector = (DimensionSelector) selector;
    final IndexedInts row = dimSelector.getRow();
    Preconditions.checkState(row.size() < 2, "Not supported for multi-value dimensions");
    return row.size() == 1 ? row.get(0) : GROUP_BY_MISSING_VALUE;
  }

  @Override
  public void writeToKeyBuffer(int keyBufferPosition, Object obj, ByteBuffer keyBuffer)
  {
    keyBuffer.putInt(keyBufferPosition, (int) obj);
  }

  @Override
  public void initGroupingKeyColumnValue(
      int keyBufferPosition,
      int columnIndex,
      Object rowObj,
      ByteBuffer keyBuffer,
      int[] stack
  )
  {
    IndexedInts row = (IndexedInts) rowObj;
    int rowSize = row.size();

    initializeGroupingKeyV2Dimension(row, rowSize, keyBuffer, keyBufferPosition);
    stack[columnIndex] = rowSize == 0 ? 0 : 1;
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

  private void initializeGroupingKeyV2Dimension(
      final IndexedInts values,
      final int rowSize,
      final ByteBuffer keyBuffer,
      final int keyBufferPosition
  )
  {
    if (rowSize == 0) {
      keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
    } else {
      keyBuffer.putInt(keyBufferPosition, values.get(0));
    }
  }

  @Override
  public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
  {
    final boolean canCompareInts =
        capabilities != null &&
        capabilities.hasBitmapIndexes() &&
        capabilities.areDictionaryValuesSorted().and(capabilities.areDictionaryValuesUnique()).isTrue();

    if (canCompareInts && (stringComparator == null || StringComparators.LEXICOGRAPHIC.equals(stringComparator))) {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Integer.compare(
          lhsBuffer.getInt(lhsPosition + keyBufferPosition),
          rhsBuffer.getInt(rhsPosition + keyBufferPosition)
      );
    } else {
      Preconditions.checkState(dictionaryLookup != null, "null dictionary lookup");
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        String lhsStr = dictionaryLookup.apply(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
        String rhsStr = dictionaryLookup.apply(rhsBuffer.getInt(rhsPosition + keyBufferPosition));
        return stringComparator.compare(lhsStr, rhsStr);
      };
    }
  }
}
