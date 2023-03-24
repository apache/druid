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

/**
 * A semantic interface for use with {@link Column} objects.
 *
 * This is used to swap values inside of a column.  Note that this interface fundamentally mutates the underlying
 * column.  If a column cannot support mutation, it should not return return an implementation of this interface.
 */
public interface ColumnValueSwapper
{
  /**
   * Swaps the values at the two row ids.  There is no significant to "right" and "left", it's just easier to name
   * the parameters that way.
   *
   * @param lhs the left-hand-side rowId
   * @param rhs the right-hand-side rowId
   */
  void swapValues(int lhs, int rhs);
}
