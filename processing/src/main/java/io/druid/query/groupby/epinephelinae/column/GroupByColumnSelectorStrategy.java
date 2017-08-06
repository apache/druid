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

import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.segment.ColumnValueSelector;

import java.nio.ByteBuffer;
import java.util.Map;

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
   * Read a value from a grouping key and add it to the group by query result map, using the output name specified
   * in a DimensionSpec.
   *
   * An implementation may choose to not add anything to the result map
   * (e.g., as the String implementation does for empty rows)
   *
   * selectorPlus provides access to:
   * - the keyBufferPosition offset from which to read the value
   * - the dimension value selector
   * - the DimensionSpec for this dimension from the query
   *
   * @param selectorPlus dimension info containing the key offset, value selector, and dimension spec
   * @param resultMap result map for the group by query being served
   * @param key grouping key
   */
  void processValueFromGroupingKey(
      GroupByColumnSelectorPlus selectorPlus,
      ByteBuffer key,
      Map<String, Object> resultMap
  );

  /**
   * Retrieve a row object from the {@link ColumnValueSelector} and put it in valuess at columnIndex.
   *
   * @param selector Value selector for a column.
   * @param columnIndex Index of the column within the row values array
   * @param valuess Row values array, one index per column
   */
  void initColumnValues(ColumnValueSelector selector, int columnIndex, Object[] valuess);

  /**
   * Read the first value within a row values object (IndexedInts, IndexedLongs, etc.) and write that value
   * to the keyBuffer at keyBufferPosition. If rowSize is 0, write GROUP_BY_MISSING_VALUE instead.
   *
   * If the size of the row is > 0, write 1 to stack[] at columnIndex, otherwise write 0.
   *
   * @param keyBufferPosition Starting offset for this column's value within the grouping key.
   * @param columnIndex Index of the column within the row values array
   * @param rowObj Row value object for this column (e.g., IndexedInts)
   * @param keyBuffer grouping key
   * @param stack array containing the current within-row value index for each column
   */
  void initGroupingKeyColumnValue(int keyBufferPosition, int columnIndex, Object rowObj, ByteBuffer keyBuffer, int[] stack);

  /**
   * If rowValIdx is less than the size of rowObj (haven't handled all of the row values):
   * First, read the value at rowValIdx from a rowObj and write that value to the keyBuffer at keyBufferPosition.
   * Then return true
   *
   * Otherwise, return false.
   *
   * @param keyBufferPosition Starting offset for this column's value within the grouping key.
   * @param rowObj Row value object for this column (e.g., IndexedInts)
   * @param rowValIdx Index of the current value being grouped on within the row
   * @param keyBuffer grouping key
   * @return true if rowValIdx < size of rowObj, false otherwise
   */
  boolean checkRowIndexAndAddValueToGroupingKey(int keyBufferPosition, Object rowObj, int rowValIdx, ByteBuffer keyBuffer);

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
}
