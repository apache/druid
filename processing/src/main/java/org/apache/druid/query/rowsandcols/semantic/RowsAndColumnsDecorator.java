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
import org.apache.druid.query.operator.OffsetLimit;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import java.util.List;

/**
 * An interface for "decorating" a rowsAndColumns.  This basically takes extra metadata that impacts the shape of the
 * RowsAndColumns.
 *
 * Generally speaking, all of the void methods on this interface should cause the RowsAndColumns object to act
 * as if it has been mutated with that decoration already.  That is, whether an implementation is lazy or not should
 * not impact what is visible/not visible.
 *
 * After all decoration methods have been called, either {@link #restrictColumns(List)} or {@link #toRowsAndColumns()}
 * can be called to generate a RowsAndColumns with the decorations applied.  Note, that it is generally expected that
 * implementations will choose to lazily apply the decorations and not actually materialize them until the last
 * possible moment, but this is an implementation detail left up to the specific implementation.
 */
public interface RowsAndColumnsDecorator
{
  static RowsAndColumnsDecorator fromRAC(RowsAndColumns rac)
  {
    if (rac instanceof RowsAndColumnsDecorator) {
      return (RowsAndColumnsDecorator) rac;
    }

    final RowsAndColumnsDecorator retVal = rac.as(RowsAndColumnsDecorator.class);
    if (retVal == null) {
      return new DefaultRowsAndColumnsDecorator(rac);
    }
    return retVal;
  }

  void limitTimeRange(Interval interval);

  void addFilter(Filter filter);

  void addVirtualColumns(VirtualColumns virtualColumn);

  void setOffsetLimit(OffsetLimit offsetLimit);

  void setOrdering(List<ColumnWithDirection> ordering);

  RowsAndColumns restrictColumns(List<String> columns);

  RowsAndColumns toRowsAndColumns();
}
