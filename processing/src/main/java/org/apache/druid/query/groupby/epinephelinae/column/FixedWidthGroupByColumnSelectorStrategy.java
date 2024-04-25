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

import org.apache.druid.error.DruidException;
import org.apache.druid.query.DimensionComparisonUtils;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.NullableTypeStrategy;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.nio.ByteBuffer;
import java.util.function.Function;

/**
 * Strategy for grouping dimensions which have fixed-width objects. It is only used for numeric primitive types,
 * however complex types can reuse this strategy if they can hint the engine that they are always fixed width
 * (for e.g. IP types). Such types donot need to be backed by a dictionary, and hence are faster to group by.
 *
 * @param <T> Class of the dimension
 */
@NotThreadSafe
public class FixedWidthGroupByColumnSelectorStrategy<T> implements GroupByColumnSelectorStrategy
{
  /**
   * Size of the key when materialized as bytes
   */
  final int keySizeBytes;

  /**
   * Type of the dimension on which the grouping strategy is being used
   */
  final ColumnType columnType;

  /**
   * Nullable type strategy of the dimension
   */
  final NullableTypeStrategy<T> nullableTypeStrategy;

  final Function<ColumnValueSelector<?>, T> valueGetter;
  final Function<ColumnValueSelector<?>, Boolean> nullityGetter;

  public FixedWidthGroupByColumnSelectorStrategy(
      int keySizeBytes,
      ColumnType columnType,
      Function<ColumnValueSelector<?>, T> valueGetter,
      Function<ColumnValueSelector<?>, Boolean> nullityGetter
  )
  {
    this.keySizeBytes = keySizeBytes;
    this.columnType = columnType;
    this.nullableTypeStrategy = columnType.getNullableStrategy();
    this.valueGetter = valueGetter;
    this.nullityGetter = nullityGetter;
  }

  @Override
  public int getGroupingKeySizeBytes()
  {
    return keySizeBytes;
  }

  @Override
  public void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      ResultRow resultRow,
      int keyBufferPosition
  )
  {
    resultRow.set(
        selectorPlus.getResultRowPosition(),
        nullableTypeStrategy.read(key, keyBufferPosition)
    );
  }

  @Override
  public int initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess)
  {
    valuess[columnIndex] = getValue(selector);
    return 0;
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
    int written;
    if (rowObj == null) {
      written = nullableTypeStrategy.write(keyBuffer, keyBufferPosition, null, keySizeBytes);
      stack[dimensionIndex] = 0;
    } else {
      written = nullableTypeStrategy.write(keyBuffer, keyBufferPosition, (T) rowObj, keySizeBytes);
      stack[dimensionIndex] = 1;
    }
    // Since this is a fixed width strategy, the caller should already have allocated enough space to materialize the
    // key object, and the type strategy should always be able to write to the buffer
    if (written < 0) {
      throw DruidException.defensive("Unable to serialize the value [%s] to buffer", rowObj);
    }
  }

  /**
   * This is used for multi-valued dimensions, for values after the first one. None of the current types supported by
   * this strategy handle multi-valued dimensions, therefore this short circuits and returns false
   */
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
  public int writeToKeyBuffer(
      int keyBufferPosition,
      ColumnValueSelector selector,
      ByteBuffer keyBuffer
  )
  {
    T value = getValue(selector);
    int written = nullableTypeStrategy.write(keyBuffer, keyBufferPosition, value, keySizeBytes);
    if (written < 0) {
      throw DruidException.defensive("Unable to serialize the value [%s] to buffer", value);
    }
    // This strategy doesn't use dictionary building and doesn't hold any internal state, therefore size increase is nil.
    return 0;
  }

  @Override
  public Grouper.BufferComparator bufferComparator(
      int keyBufferPosition,
      @Nullable StringComparator stringComparator
  )
  {
    return (lhsBuffer, rhsBuffer, lhsPosition, rhsPosition) -> {
      T lhs = nullableTypeStrategy.read(lhsBuffer, lhsPosition + keyBufferPosition);
      T rhs = nullableTypeStrategy.read(rhsBuffer, rhsPosition + keyBufferPosition);
      if (stringComparator != null
          && !DimensionComparisonUtils.isNaturalComparator(columnType.getType(), stringComparator)) {
        return stringComparator.compare(String.valueOf(lhs), String.valueOf(rhs));
      }
      // Nulls are allowed while comparing
      //noinspection ConstantConditions
      return nullableTypeStrategy.compare(lhs, rhs);
    };
  }


  @Override
  public void reset()
  {
    // Nothing to reset
  }

  /**
   * Returns the value of the selector. It handles nullity of the value and casts it to the proper type so that the
   * upstream callers donot need to worry about handling incorrect types (for example, if a double column value selector
   * returns a long)
   */
  @Nullable
  private T getValue(ColumnValueSelector columnValueSelector)
  {
    if (nullityGetter.apply(columnValueSelector)) {
      return null;
    }
    // Convert the object to the desired type
    return valueGetter.apply(columnValueSelector);
  }
}
