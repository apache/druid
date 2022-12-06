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

package org.apache.druid.query.rowsandcols.column;

import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

/**
 * Allows for accessing a column, provides methods to enable cell-by-cell access.
 */
public interface ColumnAccessor
{
  /**
   * Get the type of the Column
   *
   * @return the type of the Column
   */
  ColumnType getType();

  /**
   * Get the number of cells
   *
   * @return the number of cells
   */
  int numRows();

  /**
   * Get whether the value of a cell is null
   *
   * @param rowNum the cell id, 0-indexed
   * @return true if the value is null
   */
  boolean isNull(int rowNum);

  /**
   * Get the {@link Object} representation of the cell.
   *
   * @param rowNum the cell id, 0-indexed
   * @return the {@link Object} representation of the cell.  Returns {@code null} If {@link #isNull} is true.
   */
  @Nullable
  Object getObject(int rowNum);

  /**
   * Get the primitive {@code double} representation of the cell.
   *
   * @param rowNum the cell id, 0-indexed
   * @return the primitive {@code double} representation of the cell.  Returns {@code 0D} If {@link #isNull} is true.
   */
  double getDouble(int rowNum);

  /**
   * Get the primitive {@code float} representation of the cell.
   *
   * @param rowNum the cell id, 0-indexed
   * @return the primitive {@code float} representation of the cell.  Returns {@code 0F} If {@link #isNull} is true.
   */
  float getFloat(int rowNum);

  /**
   * Get the primitive {@code long} representation of the cell.
   *
   * @param rowNum the cell id, 0-indexed
   * @return the primitive {@code long} representation of the cell.  Returns {@code 0L} If {@link #isNull} is true.
   */
  long getLong(int rowNum);

  /**
   * Get the primitive {@code int} representation of the cell.
   *
   * @param rowNum the cell id, 0-indexed
   * @return the primitive {@code int} representation of the cell.  Returns {@code 0} If {@link #isNull} is true.
   */
  int getInt(int rowNum);

  /**
   * Compares two cells using a comparison that follows the same semantics as {@link java.util.Comparator#compare}
   * <p>
   * This is not comparing the cell Ids, but the values referred to by the cell ids.
   *
   * @param lhsRowNum the cell id of the left-hand-side of the comparison
   * @param rhsRowNum the cell id of the right-hand-side of the comparison
   * @return the result of the comparison of the two cells
   */
  int compareCells(int lhsRowNum, int rhsRowNum);
}
