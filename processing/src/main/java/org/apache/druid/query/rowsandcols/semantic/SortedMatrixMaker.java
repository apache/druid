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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.util.FindResult;

import javax.annotation.Nullable;
import java.util.List;

/**
 * A thing that makes Matrixs in an assumed sorted-fashion.
 *
 * This SemanticInterface was created in support of the SortedInnerJoinOperator, which was created as an attempt
 * to ensure that the pausable Operators actually enable us to merge independent streams of data.  As such, thought
 * has gone into it to the extent that it was necessary to make something that works, it's entirely possible that
 * it's a pointless interface and should be re-evaluated when/if the join implementation is dusted back off
 */
public interface SortedMatrixMaker
{
  static SortedMatrixMaker fromRAC(RowsAndColumns rac)
  {
    SortedMatrixMaker retVal = rac.as(SortedMatrixMaker.class);
    if (retVal == null) {
      return new DefaultSortedMatrixMaker(rac);
    }
    return retVal;
  }

  SortedMatrix make(List<String> columns);

  /**
   * A matrix thingie
   */
  interface SortedMatrix
  {
    int numRows();

    MatrixRow getRow(int rowId);

    /**
     * @param startRowIndex the rowIndex to start the search from.  The return value should never be less than this
     *                      value
     * @param row           the row to compare against
     * @return a FindResult null if exists beyond the bounds of the current Matrix
     */
    FindResult findRow(int startRowIndex, MatrixRow row);

    interface MatrixRow
    {
      int length();

      boolean isNull(int columnId);

      @Nullable
      Object getObject(int columnId);

      double getDouble(int columnId);

      float getFloat(int columnId);

      long getLong(int columnId);
    }
  }

}
