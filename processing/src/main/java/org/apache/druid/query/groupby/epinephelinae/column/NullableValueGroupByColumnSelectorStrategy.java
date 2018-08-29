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


import org.apache.druid.common.config.NullHandling;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Map;

public class NullableValueGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{
  private final GroupByColumnSelectorStrategy delegate;

  public NullableValueGroupByColumnSelectorStrategy(GroupByColumnSelectorStrategy delegate)
  {
    this.delegate = delegate;
  }

  @Override
  public int getGroupingKeySize()
  {
    return delegate.getGroupingKeySize() + Byte.BYTES;
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      Map<String, Object> resultMap,
      int keyBufferPosition
  )
  {
    if (key.get(keyBufferPosition) == NullHandling.IS_NULL_BYTE) {
      resultMap.put(selectorPlus.getOutputName(), null);
    } else {
      delegate.processValueFromGroupingKey(selectorPlus, key, resultMap, keyBufferPosition + Byte.BYTES);
    }
  }

  @Override
  public void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] values)
  {
    if (selector.isNull()) {
      values[columnIndex] = null;
    } else {
      delegate.initColumnValues(selector, columnIndex, values);
    }
  }

  @Override
  @Nullable
  public Object getOnlyValue(ColumnValueSelector selector)
  {
    if (selector.isNull()) {
      return null;
    }
    return delegate.getOnlyValue(selector);
  }

  @Override
  public void writeToKeyBuffer(int keyBufferPosition, @Nullable Object obj, ByteBuffer keyBuffer)
  {
    if (obj == null) {
      keyBuffer.put(keyBufferPosition, NullHandling.IS_NULL_BYTE);
    } else {
      keyBuffer.put(keyBufferPosition, NullHandling.IS_NOT_NULL_BYTE);
    }
    delegate.writeToKeyBuffer(keyBufferPosition + Byte.BYTES, obj, keyBuffer);
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
      int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer
  )
  {
    // rows from a nullable column always have a single value, multi-value is not currently supported
    // this method handles row values after the first in a multivalued row, so just return false
    return false;
  }
}
