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
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.DictionaryBuilding;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.ComparableIntArray;
import org.apache.druid.segment.data.ComparableStringArray;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

public class ArrayStringGroupByColumnSelectorStrategy implements GroupByColumnSelectorStrategy
{
  private static final int GROUP_BY_MISSING_VALUE = -1;


  // contains string <-> id for each element of the multi value grouping column
  // for eg : [a,b,c] is the col value. dictionaryToInt will contain { a <-> 1, b <-> 2, c <-> 3}
  private final BiMap<String, Integer> dictionaryToInt;

  // stores each row as a integer array where the int represents the value in dictionaryToInt
  // for eg : [a,b,c] would be converted to [1,2,3] and assigned a integer value 1.
  // [1,2,3] <-> 1
  private final BiMap<ComparableIntArray, Integer> intListToInt;

  private long estimatedFootprint = 0L;

  @Override
  public int getGroupingKeySize()
  {
    return Integer.BYTES;
  }

  public ArrayStringGroupByColumnSelectorStrategy()
  {
    dictionaryToInt = HashBiMap.create();
    intListToInt = HashBiMap.create();
  }

  @VisibleForTesting
  ArrayStringGroupByColumnSelectorStrategy(
      BiMap<String, Integer> dictionaryToInt,
      BiMap<ComparableIntArray, Integer> intArrayToInt
  )
  {
    this.dictionaryToInt = dictionaryToInt;
    this.intListToInt = intArrayToInt;
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

    // GROUP_BY_MISSING_VALUE is used to indicate empty rows
    if (id != GROUP_BY_MISSING_VALUE) {
      final int[] intRepresentation = intListToInt.inverse()
                                                  .get(id).getDelegate();
      final String[] stringRepresentaion = new String[intRepresentation.length];
      for (int i = 0; i < intRepresentation.length; i++) {
        stringRepresentaion[i] = dictionaryToInt.inverse().get(intRepresentation[i]);
      }
      resultRow.set(selectorPlus.getResultRowPosition(), ComparableStringArray.of(stringRepresentaion));
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), null);
    }

  }

  @Override
  public int initColumnValues(
      ColumnValueSelector selector,
      int columnIndex,
      Object[] valuess
  )
  {
    final long priorFootprint = estimatedFootprint;
    final int groupingKey = computeDictionaryId(selector);
    valuess[columnIndex] = groupingKey;
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

  /**
   * Compute dictionary ID for the given selector. Updates {@link #estimatedFootprint} as necessary.
   */
  @VisibleForTesting
  int computeDictionaryId(ColumnValueSelector selector)
  {
    final int[] intRepresentation;
    Object object = selector.getObject();
    if (object == null) {
      return GROUP_BY_MISSING_VALUE;
    } else if (object instanceof String) {
      intRepresentation = new int[1];
      intRepresentation[0] = addToIndexedDictionary((String) object);
    } else if (object instanceof List) {
      final int size = ((List<?>) object).size();
      intRepresentation = new int[size];
      for (int i = 0; i < size; i++) {
        intRepresentation[i] = addToIndexedDictionary((String) ((List<?>) object).get(i));
      }
    } else if (object instanceof String[]) {
      final int size = ((String[]) object).length;
      intRepresentation = new int[size];
      for (int i = 0; i < size; i++) {
        intRepresentation[i] = addToIndexedDictionary(((String[]) object)[i]);
      }
    } else if (object instanceof Object[]) {
      final int size = ((Object[]) object).length;
      intRepresentation = new int[size];
      for (int i = 0; i < size; i++) {
        intRepresentation[i] = addToIndexedDictionary((String) ((Object[]) object)[i]);
      }
    } else {
      throw new ISE("Found unexpected object type [%s] in %s array.", object.getClass().getName(), ValueType.STRING);
    }

    final ComparableIntArray comparableIntArray = ComparableIntArray.of(intRepresentation);
    final int dictId = intListToInt.getOrDefault(comparableIntArray, GROUP_BY_MISSING_VALUE);
    if (dictId == GROUP_BY_MISSING_VALUE) {
      final int nextId = intListToInt.keySet().size();
      intListToInt.put(comparableIntArray, nextId);

      // We're not using the dictionary and reverseDictionary from DictionaryBuilding, but the BiMap is close enough
      // that we expect this footprint calculation to still be useful. (It doesn't have to be exact.)
      estimatedFootprint +=
          DictionaryBuilding.estimateEntryFootprint(comparableIntArray.getDelegate().length * Integer.BYTES);

      return nextId;
    } else {
      return dictId;
    }
  }

  private int addToIndexedDictionary(String value)
  {
    final Integer dictId = dictionaryToInt.get(value);
    if (dictId == null) {
      final int nextId = dictionaryToInt.size();
      dictionaryToInt.put(value, nextId);

      // We're not using the dictionary and reverseDictionary from DictionaryBuilding, but the BiMap is close enough
      // that we expect this footprint calculation to still be useful. (It doesn't have to be exact.)
      estimatedFootprint +=
          DictionaryBuilding.estimateEntryFootprint((value == null ? 0 : value.length()) * Character.BYTES);

      return nextId;
    } else {
      return dictId;
    }
  }

  @Override
  public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
  {
    final long priorFootprint = estimatedFootprint;

    // computeDictionaryId updates estimatedFootprint
    keyBuffer.putInt(keyBufferPosition, computeDictionaryId(selector));

    return (int) (estimatedFootprint - priorFootprint);
  }

  @Override
  public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
  {
    final StringComparator comparator = stringComparator == null ? StringComparators.LEXICOGRAPHIC : stringComparator;
    return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
      int[] lhs = intListToInt.inverse().get(lhsBuffer.getInt(lhsPosition + keyBufferPosition)).getDelegate();
      int[] rhs = intListToInt.inverse().get(rhsBuffer.getInt(rhsPosition + keyBufferPosition)).getDelegate();

      int minLength = Math.min(lhs.length, rhs.length);
      //noinspection ArrayEquality
      if (lhs == rhs) {
        return 0;
      } else {
        for (int i = 0; i < minLength; i++) {
          final int cmp = comparator.compare(
              dictionaryToInt.inverse().get(lhs[i]),
              dictionaryToInt.inverse().get(rhs[i])
          );
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
    dictionaryToInt.clear();
    intListToInt.clear();
    estimatedFootprint = 0;
  }

  @VisibleForTesting
  void writeToKeyBuffer(int keyBufferPosition, int groupingKey, ByteBuffer keyBuffer)
  {
    keyBuffer.putInt(keyBufferPosition, groupingKey);
  }
}
