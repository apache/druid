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

package io.druid.query.groupby.epinephelinae.column;

import com.google.common.primitives.Ints;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.nio.ByteBuffer;
import java.util.Map;

public class StringGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{
  private static final int GROUP_BY_MISSING_VALUE = -1;

  @Override
  public int getGroupingKeySize()
  {
    return Ints.BYTES;
  }

  @Override
  public void processValueFromGroupingKey(GroupByColumnSelectorPlus selectorPlus, ByteBuffer key, Map<String, Object> resultMap)
  {
    final int id = key.getInt(selectorPlus.getKeyBufferPosition());

    // GROUP_BY_MISSING_VALUE is used to indicate empty rows, which are omitted from the result map.
    if (id != GROUP_BY_MISSING_VALUE) {
      resultMap.put(
          selectorPlus.getOutputName(),
          ((DimensionSelector) selectorPlus.getSelector()).lookupName(id)
      );
    } else {
      resultMap.put(selectorPlus.getOutputName(), "");
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
  public void initGroupingKeyColumnValue(int keyBufferPosition, int columnIndex, Object rowObj, ByteBuffer keyBuffer, int[] stack)
  {
    IndexedInts row = (IndexedInts) rowObj;
    int rowSize = row.size();

    initializeGroupingKeyV2Dimension(row, rowSize, keyBuffer, keyBufferPosition);
    stack[columnIndex] = rowSize == 0 ? 0 : 1;
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer)
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
}
