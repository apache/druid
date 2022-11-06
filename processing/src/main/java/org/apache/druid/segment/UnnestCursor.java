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

package org.apache.druid.segment;

/**
 * UnnestCursor is an abstraction over cursors for unnest.
 * There are two types of cursors :
 * -- DimensionUnnestCursor which is called if the column is disctionary encoded
 * -- ColumnarValueCursor otherwise
 */
public abstract class UnnestCursor implements Cursor
{

  /**
   * This initializes the unnest cursor and creates daa=ta structures
   * to start iterating over the values to be unnested.
   * This would also create a bitset for dictonary encoded columns to
   * check for matching values specified in allowedList of UnnestDataSource.
   */
  abstract void initialize();

  /**
   * This advances the cursor to move to the next element to be unnested.
   * When the last element in a row is unnested, it is also responsible
   * to move the base cursor to the next row for unnesting and repopulates
   * the data structures, created during initialize(), to point to the new row
   */
  abstract void advanceAndUpdate();

  /**
   * This advances the unnest cursor in cases where an allowList is specified
   * and the current value at the unnest cursor is not in the allowList.
   * The cursor in such cases is moved till the next match is found.
   *
   * @return a boolean to indicate whether to stay or move cursor
   */
  abstract boolean matchAndProceed();

}
