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
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuilding;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class ArrayNumericGroupByColumnSelectorStrategy
    implements GroupByColumnSelectorStrategy
{
  protected static final int GROUP_BY_MISSING_VALUE = -1;

  private final List<Object[]> dictionary;
  private final Object2IntMap<Object[]> reverseDictionary;
  private long estimatedFootprint = 0L;
  private final int valueFootprint;

  public ArrayNumericGroupByColumnSelectorStrategy(final int valueFootprint, final ColumnType arrayType)
  {
    this.dictionary = DictionaryBuilding.createDictionary();
    this.reverseDictionary = DictionaryBuilding.createReverseDictionaryForPrimitiveArray(arrayType);
    this.valueFootprint = valueFootprint;
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
      final Object[] value = dictionary.get(id);
      resultRow.set(selectorPlus.getResultRowPosition(), value);
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), null);
    }
  }

  @Override
  public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    final long priorFootprint = estimatedFootprint;
    valuess[columnIndex] = computeDictionaryId(selector);
    return (int) (estimatedFootprint - priorFootprint);
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
    final int groupingKey = (int) rowObj;
    writeToKeyBuffer(keyBufferPosition, groupingKey, keyBuffer);
    if (groupingKey == GROUP_BY_MISSING_VALUE) {
      stack[dimensionIndex] = 0;
    } else {
      stack[dimensionIndex] = 1;
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

  protected abstract int computeDictionaryId(ColumnValueSelector selector);

  @Override
  public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
  {
    final long priorFootprint = estimatedFootprint;

    // computeDictionaryId updates estimatedFootprint
    keyBuffer.putInt(keyBufferPosition, computeDictionaryId(selector));

    return (int) (estimatedFootprint - priorFootprint);
  }

  protected int addToIndexedDictionary(Object[] t)
  {
    final int dictId = reverseDictionary.getInt(t);
    if (dictId < 0) {
      final int size = dictionary.size();
      dictionary.add(t);
      reverseDictionary.put(t, size);

      // Footprint estimate: one pointer, one value per list entry.
      estimatedFootprint += DictionaryBuilding.estimateEntryFootprint(t.length * (Long.BYTES + valueFootprint));
      return size;
    }
    return dictId;
  }

  @Override
  public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
  {
    StringComparator comparator = stringComparator == null ? StringComparators.NUMERIC : stringComparator;
    return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
      Object[] lhs = dictionary.get(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
      Object[] rhs = dictionary.get(rhsBuffer.getInt(rhsPosition + keyBufferPosition));

      int minLength = Math.min(lhs.length, rhs.length);
      //noinspection ArrayEquality
      if (lhs == rhs) {
        return 0;
      } else {
        for (int i = 0; i < minLength; i++) {
          final Object left = lhs[i];
          final Object right = rhs[i];
          final int cmp;
          if (left == null && right == null) {
            cmp = 0;
          } else if (left == null) {
            cmp = -1;
          } else {
            cmp = comparator.compare(String.valueOf(left), String.valueOf(right));
          }
          if (cmp == 0) {
            continue;
          }
          return cmp;
        }
        if (lhs.length == rhs.length) {
          return 0;
        } else if (lhs.length < rhs.length) {
          return -1;
        }
        return 1;
      }
    };
  }

  @Override
  public void reset()
  {
    dictionary.clear();
    reverseDictionary.clear();
    estimatedFootprint = 0;
  }

  @VisibleForTesting
  void writeToKeyBuffer(int keyBufferPosition, int groupingKey, ByteBuffer keyBuffer)
  {
    keyBuffer.putInt(keyBufferPosition, groupingKey);
  }
}
