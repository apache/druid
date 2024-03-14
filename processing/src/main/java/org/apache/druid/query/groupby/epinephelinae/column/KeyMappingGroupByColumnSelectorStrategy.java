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

import com.google.common.base.Preconditions;
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
 * Strategy for grouping dimensions which can have variable-width objects. Materializing such objects on the buffer
 * require an additional step of mapping them to an integer index. The integer index can be materialized on the buffer within
 * a fixed width, and is often backed by a dictionary representing the actual dimension object. It is used for arrays,
 * strings, and complex types.
 *
 * The visibility of the class is limited, and the callers must use one of the two variants of the mapping strategy:
 * 1. {@link PrebuiltDictionaryStringGroupByColumnSelectorStrategy}
 * 2. {@link DictionaryBuildingGroupByColumnSelectorStrategy}
 *
 * @param <DimensionType>> Class of the dimension
 * @param <DimensionHolderType> Class of the "dimension holder". For single-value dimensions, the holder's type and the
 *                             holder's object are equivalent to the dimension. For multi-value dimensions (only strings),
 *                             the holder's type and the object are different, where the type would be {@link org.apache.druid.segment.data.IndexedInts}
 *                             representing all the values in the multi-valued string, while the dimension type would be
 *                             String
 *
 * @see DimensionToIdConverter encoding logic for converting value to dictionary
 * @see IdToDimensionConverter decoding logic for converting back dictionary to value
 */
@NotThreadSafe
class KeyMappingGroupByColumnSelectorStrategy<DimensionType, DimensionHolderType>
    implements GroupByColumnSelectorStrategy
{
  /**
   * Converts the dimension to equivalent dictionaryId.
   */
  final DimensionToIdConverter<DimensionHolderType> dimensionToIdConverter;

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
  final IdToDimensionConverter<DimensionType> idToDimensionConverter;

  KeyMappingGroupByColumnSelectorStrategy(
      final DimensionToIdConverter<DimensionHolderType> dimensionToIdConverter,
      final ColumnType columnType,
      final NullableTypeStrategy<DimensionType> nullableTypeStrategy,
      final DimensionType defaultValue,
      final IdToDimensionConverter<DimensionType> idToDimensionConverter
  )
  {
    this.dimensionToIdConverter = dimensionToIdConverter;
    this.columnType = columnType;
    this.nullableTypeStrategy = nullableTypeStrategy;
    this.defaultValue = defaultValue;
    this.idToDimensionConverter = idToDimensionConverter;
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
      resultRow.set(selectorPlus.getResultRowPosition(), idToDimensionConverter.idToKey(id));
    } else {
      resultRow.set(selectorPlus.getResultRowPosition(), defaultValue);
    }
  }

  @Override
  public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    MemoryEstimate<DimensionHolderType> multiValueHolder = dimensionToIdConverter.getMultiValueHolder(selector, null);
    valuess[columnIndex] = multiValueHolder.value();
    return multiValueHolder.memoryIncrease();
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
    // It is always called with the DimensionHolderType, created
    //noinspection unchecked
    DimensionHolderType rowObjCasted = (DimensionHolderType) rowObj;
    int rowSize = dimensionToIdConverter.multiValueSize(rowObjCasted);
    if (rowSize == 0) {
      keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
      stack[dimensionIndex] = 0;
    } else {
      MemoryEstimate<Integer> dictionaryIdAndMemoryIncrease =
          dimensionToIdConverter.getIndividualValueDictId(rowObjCasted, 0);
      // We should have already accounted for the memory increase when we call initColumnValues(). Dictionary building for
      // all the values in the dimension (potentially multi-valued) should have happened there
      assert dictionaryIdAndMemoryIncrease.memoryIncrease() == 0;
      keyBuffer.putInt(keyBufferPosition, dictionaryIdAndMemoryIncrease.value());
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
    // Casting is fine, because while extracting the multiValueHolder, the implementations must ensure that the returned "multi-value"
    // type is what the callers here expect
    //noinspection unchecked
    DimensionHolderType rowObjCasted = (DimensionHolderType) rowObj;
    int rowSize = dimensionToIdConverter.multiValueSize(rowObjCasted);
    if (rowValIdx < rowSize) {
      MemoryEstimate<Integer> dictionaryIdAndMemoryIncrease =
          dimensionToIdConverter.getIndividualValueDictId(rowObjCasted, rowValIdx);
      // We should have already accounted for the memory increase when we call initColumnValues(). Dictionary building for
      // all the values in the dimension (potentially multi-valued) should have happened there
      assert dictionaryIdAndMemoryIncrease.memoryIncrease() == 0;
      keyBuffer.putInt(
          keyBufferPosition,
          dictionaryIdAndMemoryIncrease.value()
      );
      return true;
    } else {
      return false;
    }
  }

  @Override
  public int writeToKeyBuffer(int keyBufferPosition, ColumnValueSelector selector, ByteBuffer keyBuffer)
  {
    MemoryEstimate<DimensionHolderType> multiValueHolder = dimensionToIdConverter.getMultiValueHolder(selector, null);
    int multiValueSize = dimensionToIdConverter.multiValueSize(multiValueHolder.value());
    Preconditions.checkState(multiValueSize < 2, "Not supported for multi-value dimensions");
    MemoryEstimate<Integer> dictIdAndSizeIncrease = dimensionToIdConverter.getIndividualValueDictId(multiValueHolder.value(), 0);
    final int dictId = multiValueSize == 1 ? dictIdAndSizeIncrease.value() : GROUP_BY_MISSING_VALUE;
    keyBuffer.putInt(keyBufferPosition, dictId);

    // The implementations must return a non-nullable and non-negative size increase
    return multiValueHolder.memoryIncrease() + dictIdAndSizeIncrease.memoryIncrease();
  }

  @Override
  public Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator)
  {
    boolean usesNaturalComparator =
        stringComparator == null
        || DimensionComparisonUtils.isNaturalComparator(columnType.getType(), stringComparator);
    if (idToDimensionConverter.canCompareIds() && usesNaturalComparator) {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> Integer.compare(
          lhsBuffer.getInt(lhsPosition + keyBufferPosition),
          rhsBuffer.getInt(rhsPosition + keyBufferPosition)
      );
    } else {
      return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
        Object lhsObject = idToDimensionConverter.idToKey(lhsBuffer.getInt(lhsPosition + keyBufferPosition));
        Object rhsObject = idToDimensionConverter.idToKey(rhsBuffer.getInt(rhsPosition + keyBufferPosition));
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
    // Nothing to do here. Implementations which build dictionaries should clean them in the reset method.
  }
}
