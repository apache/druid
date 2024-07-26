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

package org.apache.druid.frame;

import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexStorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
 * Storage adapter around {@link QueryableIndex} that transforms all multi-value strings columns into string arrays.
 */
public class TestArrayStorageAdapter extends QueryableIndexStorageAdapter
{
  public TestArrayStorageAdapter(QueryableIndex index)
  {
    super(index);
  }

  @Override
  public boolean canVectorize(
      @Nullable Filter filter,
      VirtualColumns virtualColumns,
      boolean descending
  )
  {
    return false;
  }

  @Override
  public Sequence<Cursor> makeCursors(
      @Nullable final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final boolean descending,
      @Nullable final QueryMetrics<?> queryMetrics
  )
  {
    return super.makeCursors(filter, interval, virtualColumns, gran, descending, queryMetrics)
                .map(DecoratedCursor::new);
  }

  @Override
  public RowSignature getRowSignature()
  {
    final RowSignature.Builder builder = RowSignature.builder();
    builder.addTimeColumn();

    for (final String column : Iterables.concat(super.getAvailableDimensions(), super.getAvailableMetrics())) {
      ColumnCapabilities columnCapabilities = super.getColumnCapabilities(column);
      ColumnType columnType = columnCapabilities == null ? null : columnCapabilities.toColumnType();
      //change MV strings columns to Array<String>
      if (columnType != null
          && columnType.equals(ColumnType.STRING)
          && columnCapabilities.hasMultipleValues().isMaybeTrue()) {
        columnType = ColumnType.STRING_ARRAY;
      }
      builder.add(column, columnType);
    }

    return builder.build();
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    final ColumnCapabilities ourType = getRowSignature().getColumnCapabilities(column);
    if (ourType != null) {
      return ColumnCapabilitiesImpl.copyOf(super.getColumnCapabilities(column)).setType(ourType.toColumnType());
    } else {
      return super.getColumnCapabilities(column);
    }
  }

  private class DecoratedCursor implements Cursor
  {
    private final Cursor cursor;

    public DecoratedCursor(Cursor cursor)
    {
      this.cursor = cursor;
    }

    @Override
    public ColumnSelectorFactory getColumnSelectorFactory()
    {
      final ColumnSelectorFactory columnSelectorFactory = cursor.getColumnSelectorFactory();
      return new ColumnSelectorFactory()
      {
        @Override
        public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
        {
          if (!(dimensionSpec instanceof DefaultDimensionSpec)) {
            // No tests need this case, don't bother to implement
            throw new UnsupportedOperationException();
          }

          final ColumnCapabilities capabilities = getColumnCapabilities(dimensionSpec.getDimension());
          if (capabilities == null || capabilities.is(ValueType.ARRAY)) {
            throw new UnsupportedOperationException("Must not call makeDimensionSelector on ARRAY");
          }

          return columnSelectorFactory.makeDimensionSelector(dimensionSpec);
        }

        @Override
        public ColumnValueSelector makeColumnValueSelector(String columnName)
        {
          final ColumnCapabilities capabilities = getColumnCapabilities(columnName);
          if (capabilities != null && capabilities.toColumnType().equals(ColumnType.STRING_ARRAY)) {
            final DimensionSelector delegate =
                columnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
            return new ObjectColumnSelector<Object[]>()
            {
              @Override
              public Object[] getObject()
              {
                final IndexedInts row = delegate.getRow();
                final int sz = row.size();
                final Object[] retVal = new Object[sz];
                for (int i = 0; i < sz; i++) {
                  retVal[i] = delegate.lookupName(row.get(i));
                }
                return retVal;
              }

              @Override
              public Class<Object[]> classOfObject()
              {
                return Object[].class;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                // No
              }
            };
          } else {
            return columnSelectorFactory.makeColumnValueSelector(columnName);
          }
        }

        @Nullable
        @Override
        public ColumnCapabilities getColumnCapabilities(String column)
        {
          return TestArrayStorageAdapter.this.getColumnCapabilities(column);
        }
      };
    }

    @Override
    public DateTime getTime()
    {
      return cursor.getTime();
    }

    @Override
    public void advance()
    {
      cursor.advance();
    }

    @Override
    public void advanceUninterruptibly()
    {
      cursor.advanceUninterruptibly();
    }

    @Override
    public boolean isDone()
    {
      return cursor.isDone();
    }

    @Override
    public boolean isDoneOrInterrupted()
    {
      return cursor.isDoneOrInterrupted();
    }

    @Override
    public void reset()
    {
      cursor.reset();
    }
  }
}
