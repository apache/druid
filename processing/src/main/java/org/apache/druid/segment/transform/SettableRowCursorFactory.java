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

package org.apache.druid.segment.transform;

import org.apache.druid.data.input.InputRow;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.CursorBuildSpec;
import org.apache.druid.segment.CursorFactory;
import org.apache.druid.segment.CursorHolder;
import org.apache.druid.segment.RowAdapters;
import org.apache.druid.segment.RowBasedColumnSelectorFactory;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.filter.ValueMatchers;

import javax.annotation.Nullable;

/**
 * A {@link CursorFactory} backed by a mutable {@link InputRow} holder. Each call to
 * {@link #makeCursorHolder(CursorBuildSpec)} returns a cursor that applies the spec's virtual columns
 * on top of the current row. The underlying row holder is shared — call {@link #set(InputRow)} to
 * swap the current row, then {@link Cursor#reset()} on the returned cursor.
 */
class SettableRowCursorFactory implements CursorFactory
{
  private final RowSignature rowSignature;
  private final ColumnSelectorFactory baseSelectorFactory;
  private InputRow currentRow;
  private long rowId = RowIdSupplier.INIT;

  SettableRowCursorFactory(final RowSignature rowSignature)
  {
    this.rowSignature = rowSignature;
    this.baseSelectorFactory = new RowBasedColumnSelectorFactory<>(
        this::getCurrentRow,
        this::getRowId,
        RowAdapters.standardRow(),
        rowSignature,
        false
    );
  }

  void set(final InputRow row)
  {
    this.currentRow = row;
    this.rowId++;
  }

  @Override
  public CursorHolder makeCursorHolder(final CursorBuildSpec spec)
  {
    final ColumnSelectorFactory selectorFactory = spec.getVirtualColumns().wrap(baseSelectorFactory);
    final Filter filter = spec.getFilter();
    final ValueMatcher filterMatcher = filter == null
                                       ? ValueMatchers.allTrue()
                                       : filter.makeMatcher(selectorFactory);

    return new CursorHolder()
    {
      @Override
      public Cursor asCursor()
      {
        return new Cursor()
        {
          private boolean done = currentRow == null || !filterMatcher.matches(false);

          @Override
          public ColumnSelectorFactory getColumnSelectorFactory()
          {
            return selectorFactory;
          }

          @Override
          public void advance()
          {
            done = true;
          }

          @Override
          public void advanceUninterruptibly()
          {
            done = true;
          }

          @Override
          public boolean isDone()
          {
            return done;
          }

          @Override
          public boolean isDoneOrInterrupted()
          {
            return done || Thread.currentThread().isInterrupted();
          }

          @Override
          public void reset()
          {
            done = currentRow == null || !filterMatcher.matches(false);
          }
        };
      }

      @Override
      public void close()
      {
      }
    };
  }

  private InputRow getCurrentRow()
  {
    return currentRow;
  }

  private long getRowId()
  {
    return rowId;
  }

  @Override
  public RowSignature getRowSignature()
  {
    return rowSignature;
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(final String column)
  {
    return rowSignature.getColumnCapabilities(column);
  }
}
