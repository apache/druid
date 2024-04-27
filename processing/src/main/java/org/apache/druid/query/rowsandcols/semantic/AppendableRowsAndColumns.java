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
import org.apache.druid.query.rowsandcols.column.Column;

/**
 * A RowsAndColumns that supports appending columns.  This interface is particularly useful because even if there is
 * some composition of code that works with RowsAndColumns, we would like to add the columns to a singular base object
 * instead of build up a complex object graph.
 */
public interface AppendableRowsAndColumns extends RowsAndColumns
{
  /**
   * Mutates the RowsAndColumns by appending the requested Column.
   *
   * @param name   the name of the new column
   * @param column the Column object representing the new column
   */
  void addColumn(String name, Column column);
}
