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

import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.operator.ColumnWithDirection;
import org.apache.druid.query.rowsandcols.DecoratedRowsAndColumns;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.util.List;

public interface DecoratableRowsAndColumns extends RowsAndColumns
{
  static DecoratableRowsAndColumns fromRAC(RowsAndColumns rac)
  {
    if (rac instanceof DecoratableRowsAndColumns) {
      return (DecoratableRowsAndColumns) rac;
    }

    final DecoratableRowsAndColumns retVal = rac.as(DecoratableRowsAndColumns.class);
    if (retVal == null) {
      return new DecoratedRowsAndColumns(rac);
    }
    return retVal;
  }

  void limitTimeRange(Interval interval);

  void addFilter(Filter filter);

  void addVirtualColumns(VirtualColumns virtualColumn);

  void setLimit(int numRows);

  void restrictColumns(List<String> columns);

  void setOrdering(List<ColumnWithDirection> ordering);
}
