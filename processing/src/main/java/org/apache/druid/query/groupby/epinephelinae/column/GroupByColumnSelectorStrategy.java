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

import org.apache.druid.query.dimension.ColumnSelectorStrategy;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.Grouper;
import org.apache.druid.query.ordering.StringComparator;
import org.apache.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Contains a collection of query processing methods for type-specific operations used exclusively by
 * GroupByQueryEngineV2.
 *
 * Each GroupByColumnSelectorStrategy is associated with a single dimension.
 */
public interface GroupByColumnSelectorStrategy extends ColumnSelectorStrategy
{
  int GROUP_BY_MISSING_VALUE = -1;

  /**
   * Return the size, in bytes, of this dimension's values in the grouping key.
   *
   * For example, a String implementation would return 4, the size of an int.
   *
   * @return size, in bytes, of this dimension's values in the grouping key.
   */
  int getGroupingKeySize();

  /**
   * Read a value from a grouping key and add it to the group by query result row, using the output name specified
   * in a DimensionSpec.
   *
   * An implementation may choose to not add anything to the result row
   * (e.g., as the String implementation does for empty rows)
   *
   * selectorPlus provides access to:
   * - the keyBufferPosition offset from which to read the value
   * - the dimension value selector
   * - the DimensionSpec for this dimension from the query
   *
   * @param selectorPlus      dimension info containing the key offset, value selector, and dimension spec
   * @param resultRow         result row for the group by query being served
   * @param key               grouping key
   * @param keyBufferPosition buffer position for the grouping key, added to support chaining multiple {@link ColumnSelectorStrategy}
   */
  void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      ResultRow resultRow,
      int keyBufferPosition
  );

  /**
   * Retrieve a row object from the {@link ColumnValueSelector} and put it in valuess at columnIndex.
   *
   * @param selector    Value selector for a column.
   * @param columnIndex Index of the column within the row values array
   * @param valuess     Row values array, one index per column
   */
  void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess);

  /**
   * Read the first value within a row values object (e. g. {@link org.apache.druid.segment.data.IndexedInts}, as the value in
   * a dictionary-encoded string column) and write that value to the keyBuffer at keyBufferPosition. If the row size is
   * 0 (e. g. {@link org.apache.druid.segment.data.IndexedInts#size}), write {@link #GROUP_BY_MISSING_VALUE} instead.
   *
   * If the size of the row is > 0, write 1 to stack[] at columnIndex, otherwise write 0.
   *
   * @param keyBufferPosition Starting offset for this column's value within the grouping key.
   * @param columnIndex       Index of the column within the row values array
   * @param rowObj            Row value object for this column
   * @param keyBuffer         grouping key
   * @param stack             array containing the current within-row value index for each column
   */
  void initGroupingKeyColumnValue(
      int keyBufferPosition,
      int columnIndex,
      Object rowObj,
      ByteBuffer keyBuffer,
      int[] stack
  );

  /**
   * If rowValIdx is less than the size of rowObj (haven't handled all of the row values):
   * First, read the value at rowValIdx from a rowObj and write that value to the keyBuffer at keyBufferPosition.
   * Then return true
   *
   * Otherwise, return false.
   *
   * @param keyBufferPosition Starting offset for this column's value within the grouping key.
   * @param rowObj            Row value object for this column (e.g., IndexedInts)
   * @param rowValIdx         Index of the current value being grouped on within the row
   * @param keyBuffer         grouping key
   *
   * @return true if rowValIdx < size of rowObj, false otherwise
   */
  boolean checkRowIndexAndAddValueToGroupingKey(
      int keyBufferPosition,
      Object rowObj,
      int rowValIdx,
      ByteBuffer keyBuffer
  );

  /**
   * Retrieve a single object using the {@link ColumnValueSelector}.  The reading column must have a single value.
   *
   * @param selector Value selector for a column
   *
   * @return an object retrieved from the column
   */
  Object getOnlyValue(ColumnValueSelector selector);

  /**
   * Write a given object to the keyBuffer at keyBufferPosition.
   *
   * @param keyBufferPosition starting offset for this column's value within the grouping key
   * @param obj               row value object retrieved from {@link #getOnlyValue(ColumnValueSelector)}
   * @param keyBuffer         grouping key
   */
  void writeToKeyBuffer(int keyBufferPosition, Object obj, ByteBuffer keyBuffer);

  /**
   * Return BufferComparator for values written using this strategy when limit is pushed down to segment scan.
   * @param keyBufferPosition starting offset for this column's value within the grouping key
   * @param stringComparator stringComparator from LimitSpec for this column. If this is null, implementations
   *                         will use the {@link org.apache.druid.query.ordering.StringComparators#LEXICOGRAPHIC}
   *                         comparator.
   * @return BufferComparator for comparing values written
   */
  Grouper.BufferComparator bufferComparator(int keyBufferPosition, @Nullable StringComparator stringComparator);
}
