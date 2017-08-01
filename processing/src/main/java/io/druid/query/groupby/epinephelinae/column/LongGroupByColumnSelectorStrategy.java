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

import com.google.common.primitives.Longs;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.LongColumnSelector;

import java.nio.ByteBuffer;
import java.util.Map;

public class LongGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{

  @Override
  public int getGroupingKeySize()
  {
    return Longs.BYTES;
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus, ByteBuffer key, Map<String, Object> resultMap
  )
  {
    final long val = key.getLong(selectorPlus.getKeyBufferPosition());
    resultMap.put(selectorPlus.getOutputName(), val);
  }

  @Override
  public void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    valuess[columnIndex] = ((LongColumnSelector) selector).getLong();
  }

  @Override
  public void initGroupingKeyColumnValue(
      int keyBufferPosition, int columnIndex, Object rowObj, ByteBuffer keyBuffer, int[] stack
  )
  {
    keyBuffer.putLong(keyBufferPosition, (Long) rowObj);
    stack[columnIndex] = 1;
  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer
  )
  {
    // rows from a long column always have a single value, multi-value is not currently supported
    // this method handles row values after the first in a multivalued row, so just return false
    return false;
  }
}
