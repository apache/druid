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

import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public class ColumnarValueUnnestCursor implements Cursor
{
  private final Cursor baseCursor;
  private final ColumnSelectorFactory baseColumSelectorFactory;
  private final ColumnValueSelector columnValueSelector;
  private final String columnName;
  private final String outputName;
  private final LinkedHashSet<String> allowSet;
  private int index;
  private Object currentVal;
  private List<Object> unnestListForCurrentRow;
  private boolean needInitialization;

  public ColumnarValueUnnestCursor(
      Cursor cursor,
      ColumnSelectorFactory baseColumSelectorFactory,
      String columnName,
      String outputColumnName,
      LinkedHashSet<String> allowSet
  )
  {
    this.baseCursor = cursor;
    this.baseColumSelectorFactory = baseColumSelectorFactory;
    this.columnValueSelector = this.baseColumSelectorFactory.makeColumnValueSelector(columnName);
    this.columnName = columnName;
    this.index = 0;
    this.outputName = outputColumnName;
    this.needInitialization = true;
    this.allowSet = allowSet;
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        if (!outputName.equals(dimensionSpec.getDimension())) {
          return baseColumSelectorFactory.makeDimensionSelector(dimensionSpec);
        }
        return baseColumSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        if (!outputName.equals(columnName)) {
          return baseColumSelectorFactory.makeColumnValueSelector(columnName);
        }
        return new ColumnValueSelector()
        {
          @Override
          public double getDouble()
          {
            Object value = getObject();
            if (value == null) {
              return 0;
            }
            return Double.valueOf((String) value);
          }

          @Override
          public float getFloat()
          {
            Object value = getObject();
            if (value == null) {
              return 0;
            }
            return Float.valueOf((String) value);
          }

          @Override
          public long getLong()
          {
            Object value = getObject();
            if (value == null) {
              return 0;
            }
            return Long.valueOf((String) value);
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            columnValueSelector.inspectRuntimeShape(inspector);
          }

          @Override
          public boolean isNull()
          {
            return getObject() == null;
          }

          @Nullable
          @Override
          public Object getObject()
          {
            if (!unnestListForCurrentRow.isEmpty()) {
              if (allowSet == null || allowSet.isEmpty()) {
                return unnestListForCurrentRow.get(index);
              } else if (allowSet.contains((String) unnestListForCurrentRow.get(index))) {
                return unnestListForCurrentRow.get(index);
              }
            }
            return null;
          }

          @Override
          public Class<?> classOfObject()
          {
            return Object.class;
          }
        };
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        if (!outputName.equals(columnName)) {
          baseColumSelectorFactory.getColumnCapabilities(column);
        }
        return baseColumSelectorFactory.getColumnCapabilities(columnName);
      }
    };
  }

  @Override
  public DateTime getTime()
  {
    return baseCursor.getTime();
  }

  @Override
  public void advance()
  {
    advanceUninterruptibly();
    BaseQuery.checkInterrupted();
  }

  @Override
  public void advanceUninterruptibly()
  {
    do {
      advanceAndUpdate();
    } while (matchAndProceed());
  }

  @Override
  public boolean isDone()
  {
    if (needInitialization && !baseCursor.isDone()) {
      initialize();
    }
    return baseCursor.isDone();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    if (needInitialization && !baseCursor.isDoneOrInterrupted()) {
      initialize();
    }
    return baseCursor.isDoneOrInterrupted();
  }

  @Override
  public void reset()
  {
    index = 0;
    needInitialization = true;
    baseCursor.reset();
  }

  private void getNextRow(boolean firstRun)
  {
    currentVal = this.columnValueSelector.getObject();
    if (currentVal == null) {
      if (!firstRun) {
        unnestListForCurrentRow = new ArrayList<>();
      }
      unnestListForCurrentRow.add(null);
    } else {
      if (currentVal instanceof List) {
        unnestListForCurrentRow = (List<Object>) currentVal;
      } else if (currentVal.getClass().equals(String.class)) {
        if (!firstRun) {
          unnestListForCurrentRow = new ArrayList<>();
        }
        unnestListForCurrentRow.add(currentVal);
      }
    }
  }

  /**
   * This initializes the unnest cursor and creates data structures
   * to start iterating over the values to be unnested.
   * This would also create a bitset for dictonary encoded columns to
   * check for matching values specified in allowedList of UnnestDataSource.
   */
  public void initialize()
  {
    this.unnestListForCurrentRow = new ArrayList<>();
    getNextRow(needInitialization);
    if (allowSet != null) {
      if (!allowSet.isEmpty()) {
        if (!allowSet.contains((String) unnestListForCurrentRow.get(index))) {
          advance();
        }
      }
    }
    needInitialization = false;
  }

  /**
   * This advances the cursor to move to the next element to be unnested.
   * When the last element in a row is unnested, it is also responsible
   * to move the base cursor to the next row for unnesting and repopulates
   * the data structures, created during initialize(), to point to the new row
   */
  public void advanceAndUpdate()
  {
    if (unnestListForCurrentRow.isEmpty() || index >= unnestListForCurrentRow.size() - 1) {
      index = 0;
      baseCursor.advance();
      if (!baseCursor.isDone()) {
        getNextRow(needInitialization);
      }
    } else {
      index++;
    }
  }

  /**
   * This advances the unnest cursor in cases where an allowList is specified
   * and the current value at the unnest cursor is not in the allowList.
   * The cursor in such cases is moved till the next match is found.
   *
   * @return a boolean to indicate whether to stay or move cursor
   */
  public boolean matchAndProceed()
  {
    boolean matchStatus;
    if (allowSet == null || allowSet.isEmpty()) {
      matchStatus = true;
    } else {
      matchStatus = allowSet.contains((String) unnestListForCurrentRow.get(index));
    }
    return !baseCursor.isDone() && !matchStatus;
  }
}
