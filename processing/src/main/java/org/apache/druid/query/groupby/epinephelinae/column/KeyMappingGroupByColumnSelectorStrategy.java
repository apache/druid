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

import org.apache.druid.query.DimensionComparisonUtils;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;

/**
 * Strategy for grouping single value dimensions which can have variable-width objects. Materializing such objects on the buffer
 * require an additional step of mapping them to an integer index. The integer index can be materialized on the buffer within
 * a fixed width, and is often backed by a dictionary representing the actual dimension object. It is used for arrays,
 * and complex types.
 * <p>
 * The visibility of the class is limited, and the callers must use the following variant of the mapping strategy:
 * 2. {@link DictionaryBuildingGroupByColumnSelectorStrategy}
 * <p>
 * {@code null} can be represented by either -1 or the position of null in the dictionary it was stored when it was
 * encountered. This is fine, because most of the time, the dictionary id has no value of its own, and is converted back to
 * the value it represents, before doing further operations. The only place where it would matter would be when
 * {@link DimensionIdCodec#canCompareIds()} is true, and we compare directly on the dictionary ids for prebuilt
 * dictionaries (we can't compare ids for the dictionaries built on the fly in the grouping strategy). However, in that case,
 * it is guaranteed that the dictionaryId of null represented by the pre-built dictionary would be the lowest (most likely 0)
 * and therefore nulls (-1) would be adjacent to nulls (represented by the lowest non-negative dictionary id), and would get
 * grouped in the later merge stages.
 * <p>
 * It only handles single value dimensions, i.e. every type except for strings. Strings are handled by the implementations
 * of {@link KeyMappingMultiValueGroupByColumnSelectorStrategy}
 * <p>
 * It only handles non-primitive types, because numeric primitives are handled by the {@link FixedWidthGroupByColumnSelectorStrategy}
 * and the string primitives are handled by the {@link KeyMappingMultiValueGroupByColumnSelectorStrategy}
 *
 * @param <DimensionType>   Class of the dimension
 * @see DimensionIdCodec encoding decoding logic for converting value to dictionary
 */
@NotThreadSafe
class KeyMappingGroupByColumnSelectorStrategy<DimensionType> implements GroupByColumnSelectorStrategy
{
  /**
   * Converts the dimension to equivalent dictionaryId.
   */
  final DimensionIdCodec<DimensionType> dimensionIdCodec;

  /**
   * Type of the dimension on which the grouping strategy is used
   */
  final ColumnType columnType;

  /**
   * Nullable type strategy of the dimension
   */
  final NullableTypeStrategy<DimensionType> nullableTypeStrategy;

  /**
   * Default value of the dimension
   */
  final DimensionType defaultValue;

  KeyMappingGroupByColumnSelectorStrategy(
      final DimensionIdCodec<DimensionType> dimensionIdCodec,
      final ColumnType columnType,
      final NullableTypeStrategy<DimensionType> nullableTypeStrategy,
      final DimensionType defaultValue
  )
  {
    this.dimensionIdCodec = dimensionIdCodec;
    this.columnType = columnType;
    this.nullableTypeStrategy = nullableTypeStrategy;
    this.defaultValue = defaultValue;
  }

  /**
   * Strategy maps to integer dictionary ids
   */
  @Override
  public int getGroupingKeySizeBytes()
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
    if (id != GROUP_BY_MISSING_VALUE) {
      resultRow.set(selectorPlus.getResultRowPosition(), dimensionIdCodec.idToKey(id));
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), defaultValue);
    }
  }

  @Override
  public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    //noinspection unchecked
    final DimensionType value = (DimensionType) DimensionHandlerUtils.convertObjectToType(
        selector.getObject(),
        columnType
    );
    if (value == null) {
      valuess[columnIndex] = GROUP_BY_MISSING_VALUE;
      return 0;
    } else {
      MemoryFootprint<Integer> idAndMemoryFootprint = dimensionIdCodec.lookupId(value);
      valuess[columnIndex] = idAndMemoryFootprint.value();
      return idAndMemoryFootprint.memoryIncrease();
    }
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
    // It is always called with the dictionaryId that we'd have initialized
    //noinspection unchecked
    int dictId = (int) rowObj;
    keyBuffer.putInt(keyBufferPosition, dictId);
    if (dictId == GROUP_BY_MISSING_VALUE) {
      stack[dimensionIndex] = 0;
    } else {
      stack[dimensionIndex] = 1;
    }
  }

  // The method is only used for single value dimensions, therefore doesn't have any actual implementation of this
  // method, which is only called for multi-value dimensions
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
  public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
  {
    //noinspection unchecked
    final DimensionType value = (DimensionType) DimensionHandlerUtils.convertObjectToType(
        selector.getObject(),
        columnType
    );
    final int memoryIncrease;
    if (value == null) {
      keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
      return 0;
    } else {
      MemoryFootprint<Integer> idAndMemoryIncrease = dimensionIdCodec.lookupId(value);
      keyBuffer.putInt(keyBufferPosition, idAndMemoryIncrease.value());
      memoryIncrease = idAndMemoryIncrease.memoryIncrease();
    }
    return memoryIncrease;
  }

  @Override
  public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
  {
    boolean usesNaturalComparator =
        stringComparator == null
        || DimensionComparisonUtils.isNaturalComparator(columnType.getType(), stringComparator);
    if (dimensionIdCodec.canCompareIds() && usesNaturalComparator) {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Integer.compare(
          lhsBuffer.getInt(lhsPosition + keyBufferPosition),
          rhsBuffer.getInt(rhsPosition + keyBufferPosition)
      );
    } else {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        int lhsDictId = lhsBuffer.getInt(lhsPosition + keyBufferPosition);
        int rhsDictId = rhsBuffer.getInt(rhsPosition + keyBufferPosition);

        Object lhsObject = lhsDictId == GROUP_BY_MISSING_VALUE ? null : dimensionIdCodec.idToKey(lhsDictId);
        Object rhsObject = rhsDictId == GROUP_BY_MISSING_VALUE ? null : dimensionIdCodec.idToKey(rhsDictId);
        if (usesNaturalComparator) {
          return nullableTypeStrategy.compare(
              (DimensionType) DimensionHandlerUtils.convertObjectToType(lhsObject, columnType),
              (DimensionType) DimensionHandlerUtils.convertObjectToType(rhsObject, columnType)
          );
        } else {
          return stringComparator.compare(String.valueOf(lhsObject), String.valueOf(rhsObject));
        }
      };
    }
  }

  @Override
  public void reset()
  {
    dimensionIdCodec.reset();
  }
}
