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

import io.druid.common.config.NullHandling;
import io.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;

public class LongGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{

  @Override
  public int getGroupingKeySize()
  {
    return Long.BYTES + Byte.BYTES;
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus, ByteBuffer key, Map<String, Object> resultMap
  )
  {
    if (key.get(selectorPlus.getKeyBufferPosition()) == (byte) 1) {
      resultMap.put(selectorPlus.getOutputName(), NullHandling.defaultLongValue());
    } else {
      final long val = key.getLong(selectorPlus.getKeyBufferPosition() + Byte.BYTES);
      resultMap.put(selectorPlus.getOutputName(), val);
    }
  }

  @Override
  public void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    if (NullHandling.sqlCompatible() && selector.isNull()) {
      valuess[columnIndex] = null;
    } else {
      valuess[columnIndex] = selector.getLong();
    }
  }

  @Override
  @Nullable
  public Object getOnlyValue(ColumnValueSelector selector)
  {
    if (NullHandling.sqlCompatible() && selector.isNull()) {
      return null;
    }
    return selector.getLong();
  }

  @Override
  public void writeToKeyBuffer(int keyBufferPosition, Object obj, ByteBuffer keyBuffer)
  {
    if (obj == null) {
      keyBuffer.put(keyBufferPosition, (byte) 1);
      keyBuffer.putLong(keyBufferPosition + Byte.BYTES, 0L);
    } else {
      keyBuffer.put(keyBufferPosition, (byte) 0);
      keyBuffer.putLong(keyBufferPosition + Byte.BYTES, (Long) obj);
    }
  }

  @Override
  public void initGroupingKeyColumnValue(
      int keyBufferPosition, int columnIndex, Object rowObj, ByteBuffer keyBuffer, int[] stack
  )
  {
    writeToKeyBuffer(keyBufferPosition, rowObj, keyBuffer);
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
