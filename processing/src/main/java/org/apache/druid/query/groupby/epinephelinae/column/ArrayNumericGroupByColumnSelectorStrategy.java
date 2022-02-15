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

import com.google.common.annotations.VisibleForTesting;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.data.ComparableList;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public abstract class ArrayNumericGroupByColumnSelectorStrategy<T extends Comparable> implements GroupByColumnSelectorStrategy
{
  protected static final int GROUP_BY_MISSING_VALUE = -1;

  protected final List<List<T>> dictionary;
  protected final Object2IntOpenHashMap<List<T>> reverseDictionary;

  public ArrayNumericGroupByColumnSelectorStrategy()
  {
    dictionary = new ArrayList<>();
    reverseDictionary = new Object2IntOpenHashMap<>();
    reverseDictionary.defaultReturnValue(-1);
  }

  @VisibleForTesting
  ArrayNumericGroupByColumnSelectorStrategy(
      List<List<T>> dictionary,
      Object2IntOpenHashMap<List<T>> reverseDictionary
  )
  {
    this.dictionary = dictionary;
    this.reverseDictionary = reverseDictionary;
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
      final List<T> value = dictionary.get(id);
      resultRow.set(selectorPlus.getResultRowPosition(), new ComparableList(value));
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), null);
    }
  }

  @Override
  public void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    final int groupingKey = (int) getOnlyValue(selector);
    valuess[columnIndex] = groupingKey;
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
    final int groupingKey = (int) rowObj;
    writeToKeyBuffer(keyBufferPosition, groupingKey, keyBuffer);
    if (groupingKey == GROUP_BY_MISSING_VALUE) {
      stack[columnIndex] = 0;
    } else {
      stack[columnIndex] = 1;
    }

  }

  @Override
  public boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition,
      Object rowObj,
      int rowValIdx,
      ByteBuffer keyBuffer
  )
  {
    return false;
  }

  @Override
  public abstract Object getOnlyValue(ColumnValueSelector selector);


  @Override
  public void writeToKeyBuffer(int keyBufferPosition, Object obj, ByteBuffer keyBuffer)
  {
    keyBuffer.putInt(keyBufferPosition, (int) obj);
  }

  int addToIndexedDictionary(List<T> t)
  {
    final int dictId = reverseDictionary.getInt(t);
    if (dictId < 0) {
      final int size = dictionary.size();
      dictionary.add(t);
      reverseDictionary.put(t, size);
      return size;
    }
    return dictId;
  }

  @Override
  public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
  {
    StringComparator comparator = stringComparator == null ? StringComparators.NUMERIC : stringComparator;
    return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
      List<T> lhs = dictionary.get(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
      List<T> rhs = dictionary.get(rhsBuffer.getInt(rhsPosition + keyBufferPosition));

      int minLength = Math.min(lhs.size(), rhs.size());
      if (lhs == rhs) {
        return 0;
      } else {
        for (int i = 0; i < minLength; i++) {
          final T left = lhs.get(i);
          final T right = rhs.get(i);
          final int cmp;
          if (left == null && right == null) {
            cmp = 0;
          } else if (left == null) {
            cmp = -1;
          } else {
            cmp = comparator.compare(String.valueOf(lhs.get(i)), String.valueOf(rhs.get(i)));
          }
          if (cmp == 0) {
            continue;
          }
          return cmp;
        }
        if (lhs.size() == rhs.size()) {
          return 0;
        } else if (lhs.size() < rhs.size()) {
          return -1;
        }
        return 1;
      }
    };
  }
}
