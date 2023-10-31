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

package org.apache.druid.query.operator;

import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.RowsAndColumnsDecorator;
import org.apache.druid.segment.VirtualColumns;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;

/**
 * A scan operator is really just a way to push down various things that can be lazily applied when data needs to
 * actually be read.  This ScanOperator works by converting the RowsAndColumns to a DecoratableRowsAndColumns
 * and then just decorates it with metadata, in this way, the RowsAndColumns should be able to leverage the extra
 * metadata whenever it is asked to do something else with a different Semantic interface by the next Operator.
 * <p>
 * If it is important to materialize the data after this metadata is applied, the Projectable semantic interface
 * can be used to materialize a new RowsAndColumns.
 */
public class ScanOperator implements Operator
{
  private final Operator subOperator;
  private final Interval timeRange;
  private final Filter filter;
  private final OffsetLimit offsetLimit;
  private final List<String> projectedColumns;
  private final VirtualColumns virtualColumns;
  private final List<ColumnWithDirection> ordering;

  public ScanOperator(
      Operator subOperator,
      List<String> projectedColumns,
      VirtualColumns virtualColumns,
      Interval timeRange,
      Filter filter,
      List<ColumnWithDirection> ordering,
      OffsetLimit offsetLimit
  )
  {
    this.subOperator = subOperator;
    this.projectedColumns = projectedColumns;
    this.virtualColumns = virtualColumns;
    this.timeRange = timeRange;
    this.filter = filter;
    this.ordering = ordering;
    this.offsetLimit = offsetLimit == null ? OffsetLimit.NONE : offsetLimit;
  }

  @Nullable
  @Override
  public Closeable goOrContinue(
      Closeable continuationObject,
      Receiver receiver
  )
  {
    return subOperator.goOrContinue(continuationObject, new Receiver()
    {
      @Override
      public Signal push(RowsAndColumns rac)
      {
        final RowsAndColumnsDecorator decor = RowsAndColumnsDecorator.fromRAC(rac);

        if (filter != null) {
          decor.addFilter(filter);
        }

        if (virtualColumns != null) {
          decor.addVirtualColumns(virtualColumns);
        }

        if (timeRange != null) {
          decor.limitTimeRange(timeRange);
        }

        if (offsetLimit.isPresent()) {
          decor.setOffsetLimit(offsetLimit);
        }

        if (!(ordering == null || ordering.isEmpty())) {
          decor.setOrdering(ordering);
        }

        if (!(projectedColumns == null || projectedColumns.isEmpty())) {
          return receiver.push(decor.restrictColumns(projectedColumns));
        } else {
          return receiver.push(decor.toRowsAndColumns());
        }
      }

      @Override
      public void completed()
      {
        receiver.completed();
      }
    });
  }
}
