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

package org.apache.druid.query.scan;

import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.segment.column.ColumnHolder;

import java.util.List;

class ScanQueries
{
  /**
   * Report whether the sort can be pushed into the Cursor, or must be done as a
   * separate sort step.
   */
  public static boolean canPushSort(ScanQuery scanQuery)
  {
    List<ScanQuery.OrderBy> orderBys = scanQuery.getOrderBys();
    DataSource dataSource = scanQuery.getDataSource();
    // Can push non-existent sort.
    if (orderBys.size() == 0) {
      return true;
    }
    // Cursor can sort by only one column.
    if (orderBys.size() > 1) {
      return false;
    }
    // Inline datasources can't sort
    if (dataSource instanceof InlineDataSource) {
      return false;
    }
    // Cursor can sort by the __time column
    return ColumnHolder.TIME_COLUMN_NAME.equals(orderBys.get(0).getColumnName());
  }

}
