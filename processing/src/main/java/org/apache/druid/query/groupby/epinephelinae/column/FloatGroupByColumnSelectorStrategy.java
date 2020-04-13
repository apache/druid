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

import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.groupby.epinephelinae.GrouperBufferComparatorUtils;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

public class FloatGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{

  @Override
  public int getGroupingKeySize()
  {
    return Float.BYTES;
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      ResultRow resultRow,
      int keyBufferPosition
  )
  {
    final float val = key.getFloat(keyBufferPosition);
    resultRow.set(selectorPlus.getResultRowPosition(), val);
  }

  @Override
  public void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    valuess[columnIndex] = selector.getFloat();
  }

  @Override
  public Object getOnlyValue(ColumnValueSelector selector)
  {
    return selector.getFloat();
  }

  @Override
  public void writeToKeyBuffer(int keyBufferPosition, @Nullable Object obj, ByteBuffer keyBuffer)
  {
    keyBuffer.putFloat(keyBufferPosition, DimensionHandlerUtils.nullToZero((Float) obj));
  }

  @Override
  public Grouper.BufferComparator bufferComparator(
      int keyBufferPosition,
      @Nullable StringComparator stringComparator
  )
  {
    return GrouperBufferComparatorUtils.makeBufferComparatorForFloat(
        keyBufferPosition,
        true,
        stringComparator
    );
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
    writeToKeyBuffer(keyBufferPosition, rowObj, keyBuffer);
    stack[columnIndex] = 1;
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition,
      Object rowObj,
      int rowValIdx,
      ByteBuffer keyBuffer
  )
  {
    // rows from a float column always have a single value, multi-value is not currently supported
    // this method handles row values after the first in a multivalued row, so just return false
    return false;
  }
}
