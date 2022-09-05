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

public class LongGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{

  @Override
  public int getGroupingKeySize()
  {
    return Long.BYTES;
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      ResultRow resultRow,
      int keyBufferPosition
  )
  {
    final long val = key.getLong(keyBufferPosition);
    resultRow.set(selectorPlus.getResultRowPosition(), val);
  }

  @Override
  public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    valuess[columnIndex] = selector.getLong();
    return 0;
  }

  @Override
  public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
  {
    keyBuffer.putLong(keyBufferPosition, selector.getLong());
    return 0;
  }

  @Override
  public Grouper.BufferComparator bufferComparator(
      int keyBufferPosition,
      @Nullable StringComparator stringComparator
  )
  {
    return GrouperBufferComparatorUtils.makeBufferComparatorForLong(
        keyBufferPosition,
        true,
        stringComparator
    );
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
    writeToKeyBuffer(keyBufferPosition, DimensionHandlerUtils.nullToZero((Long) rowObj), keyBuffer);
    stack[dimensionIndex] = 1;
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition,
      Object rowObj,
      int rowValIdx,
      ByteBuffer keyBuffer
  )
  {
    // rows from a long column always have a single value, multi-value is not currently supported
    // this method handles row values after the first in a multivalued row, so just return false
    return false;
  }

  @Override
  public void reset()
  {
    // Nothing to do.
  }

  public void writeToKeyBuffer(int keyBufferPosition, long value, ByteBuffer keyBuffer)
  {
    keyBuffer.putLong(keyBufferPosition, value);
  }
}
