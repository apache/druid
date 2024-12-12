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

import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.projections.QueryableProjection;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorOffset;

import javax.annotation.Nullable;
import java.util.LinkedHashSet;
import java.util.List;

public class QueryableIndexCursorFactory implements CursorFactory
{
  private final QueryableIndex index;
  private final TimeBoundaryInspector timeBoundaryInspector;

  /**
   * Constructor that accepts a {@link QueryableIndexTimeBoundaryInspector} that is in use elsewhere, promoting
   * efficient re-use.
   */
  public QueryableIndexCursorFactory(QueryableIndex index, TimeBoundaryInspector timeBoundaryInspector)
  {
    this.index = index;
    this.timeBoundaryInspector = timeBoundaryInspector;
  }

  /**
   * Constructor that creates a new {@link QueryableIndexTimeBoundaryInspector}.
   */
  public QueryableIndexCursorFactory(QueryableIndex index)
  {
    this(index, QueryableIndexTimeBoundaryInspector.create(index));
  }

  @Override
  public CursorHolder makeCursorHolder(CursorBuildSpec spec)
  {
    QueryableProjection<QueryableIndex> projection = index.getProjection(spec);
    if (projection != null) {
      return new QueryableIndexCursorHolder(
          projection.getRowSelector(),
          projection.getCursorBuildSpec(),
          timeBoundaryInspector
      )
      {
        @Override
        protected ColumnSelectorFactory makeColumnSelectorFactoryForOffset(
            ColumnCache columnCache,
            Offset baseOffset
        )
        {
          return projection.wrapColumnSelectorFactory(
              super.makeColumnSelectorFactoryForOffset(columnCache, baseOffset)
          );
        }

        @Override
        protected VectorColumnSelectorFactory makeVectorColumnSelectorFactoryForOffset(
            ColumnCache columnCache,
            VectorOffset baseOffset
        )
        {
          return projection.wrapVectorColumnSelectorFactory(
              super.makeVectorColumnSelectorFactoryForOffset(columnCache, baseOffset)
          );
        }

        @Override
        public boolean isPreAggregated()
        {
          return true;
        }

        @Nullable
        @Override
        public List<AggregatorFactory> getAggregatorsForPreAggregated()
        {
          return projection.getCursorBuildSpec().getAggregators();
        }
      };
    }
    return new QueryableIndexCursorHolder(index, CursorBuildSpec.builder(spec).build(), timeBoundaryInspector);
  }

  @Override
  public RowSignature getRowSignature()
  {
    final LinkedHashSet<String> columns = new LinkedHashSet<>();

    for (final OrderBy orderBy : index.getOrdering()) {
      columns.add(orderBy.getColumnName());
    }

    // Add __time after the defined ordering, if __time wasn't part of it.
    columns.add(ColumnHolder.TIME_COLUMN_NAME);
    columns.addAll(index.getColumnNames());

    final RowSignature.Builder builder = RowSignature.builder();
    for (final String column : columns) {
      final ColumnType columnType = ColumnType.fromCapabilities(index.getColumnCapabilities(column));

      // index.getOrdering() may include columns that don't exist, such as if they were omitted due to
      // being 100% nulls. Don't add those to the row signature.
      if (columnType != null) {
        builder.add(column, columnType);
      }
    }

    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return index.getColumnCapabilities(column);
  }
}
