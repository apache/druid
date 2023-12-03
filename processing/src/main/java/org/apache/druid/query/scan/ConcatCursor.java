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

import com.google.common.math.IntMath;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Combines multiple cursors and iterates over them. It skips over the empty cursors
 * The {@link DimensionSelector} and {@link ColumnValueSelector} it generates hold the reference to the original object
 * because the cursor might be advanced independently after extracting out the {@link ColumnSelectorFactory} like in
 * {@link org.apache.druid.frame.segment.FrameCursorUtils#cursorToFrames}. This ensures that the selectors always return
 * the value pointed by the {@link #currentCursor}.
 */
public class ConcatCursor implements Cursor
{

  private final List<Cursor> cursors;
  private int currentCursor;

  public ConcatCursor(
      List<Cursor> cursors
  )
  {
    this.cursors = cursors;
    currentCursor = 0;
    skipEmptyCursors();
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        return new DimensionSelector()
        {
          @Override
          public IndexedInts getRow()
          {
            return cursors.get(currentCursor).getColumnSelectorFactory().makeDimensionSelector(dimensionSpec).getRow();
          }

          @Override
          public ValueMatcher makeValueMatcher(@Nullable String value)
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .makeValueMatcher(value);
          }

          @Override
          public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .makeValueMatcher(predicateFactory);
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            cursors.get(currentCursor)
                   .getColumnSelectorFactory()
                   .makeDimensionSelector(dimensionSpec)
                   .inspectRuntimeShape(inspector);
          }

          @Nullable
          @Override
          public Object getObject()
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .getObject();
          }

          @Override
          public Class<?> classOfObject()
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .classOfObject();
          }

          @Override
          public int getValueCardinality()
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .getValueCardinality();
          }

          @Nullable
          @Override
          public String lookupName(int id)
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .lookupName(id);
          }

          @Override
          public boolean nameLookupPossibleInAdvance()
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .nameLookupPossibleInAdvance();
          }

          @Nullable
          @Override
          public IdLookup idLookup()
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .idLookup();
          }
        };
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        return new ColumnValueSelector()
        {
          @Override
          public double getDouble()
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeColumnValueSelector(columnName)
                          .getDouble();
          }

          @Override
          public float getFloat()
          {
            return cursors.get(currentCursor).getColumnSelectorFactory().makeColumnValueSelector(columnName).getFloat();
          }

          @Override
          public long getLong()
          {
            return cursors.get(currentCursor).getColumnSelectorFactory().makeColumnValueSelector(columnName).getLong();
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            cursors.get(currentCursor)
                   .getColumnSelectorFactory()
                   .makeColumnValueSelector(columnName)
                   .inspectRuntimeShape(inspector);
          }

          @Override
          public boolean isNull()
          {
            return cursors.get(currentCursor).getColumnSelectorFactory().makeColumnValueSelector(columnName).isNull();
          }

          @Nullable
          @Override
          public Object getObject()
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeColumnValueSelector(columnName)
                          .getObject();
          }

          @Override
          public Class classOfObject()
          {
            return cursors.get(currentCursor)
                          .getColumnSelectorFactory()
                          .makeColumnValueSelector(columnName)
                          .classOfObject();
          }
        };
      }

      @Override
      public ColumnCapabilities getColumnCapabilitiesWithDefault(String column, ColumnCapabilities defaultCapabilites)
      {
        return cursors.get(currentCursor)
                      .getColumnSelectorFactory()
                      .getColumnCapabilitiesWithDefault(column, defaultCapabilites);
      }

      @Nullable
      @Override
      public ExpressionType getType(String name)
      {
        return cursors.get(currentCursor).getColumnSelectorFactory().getType(name);
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return cursors.get(currentCursor).getColumnSelectorFactory().getColumnCapabilities(column);
      }

      @Nullable
      @Override
      public RowIdSupplier getRowIdSupplier()
      {
        return cursors.get(currentCursor).getColumnSelectorFactory().getRowIdSupplier();
      }
    };
  }

  @Override
  public DateTime getTime()
  {
    return cursors.get(currentCursor).getTime();
  }

  @Override
  public void advance()
  {
    if (currentCursor < cursors.size()) {
      cursors.get(currentCursor).advance();
      advanceCursor();
    }
  }

  @Override
  public void advanceUninterruptibly()
  {
    if (currentCursor < cursors.size()) {
      cursors.get(currentCursor).advanceUninterruptibly();
      advanceCursor();
    }
  }

  @Override
  public boolean isDone()
  {
    return currentCursor == cursors.size();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    while (currentCursor >= 0) {
      if (currentCursor < cursors.size()) {
        cursors.get(currentCursor).reset();
      }
      currentCursor = IntMath.checkedSubtract(currentCursor, 1);
    }
    currentCursor = 0;
    skipEmptyCursors();
  }

  /**
   * This method should be called whenever the currentCursor gets updated. It skips over the empty cursors so that the
   * current pointer is pointing to a valid cursor
   */
  private void skipEmptyCursors()
  {
    while (currentCursor < cursors.size() && cursors.get(currentCursor).isDone()) {
      currentCursor = IntMath.checkedAdd(currentCursor, 1);
    }
  }

  /**
   * This method updates the current cursor. This is used to update the current cursor under question.
   */
  private void advanceCursor()
  {
    if (cursors.get(currentCursor).isDone()) {
      currentCursor = IntMath.checkedAdd(currentCursor, 1);
      skipEmptyCursors();
    }
  }
}
