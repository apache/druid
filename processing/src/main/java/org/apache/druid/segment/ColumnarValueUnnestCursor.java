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
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;

public class ColumnarValueUnnestCursor implements UnnestCursor
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
      String columnName,
      String outputColumnName,
      LinkedHashSet<String> allowSet
  )
  {
    this.baseCursor = cursor;
    this.baseColumSelectorFactory = cursor.getColumnSelectorFactory();
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
        return baseColumSelectorFactory.makeDimensionSelector(dimensionSpec);
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
            baseColumSelectorFactory.makeColumnValueSelector(columnName).inspectRuntimeShape(inspector);
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
          public Class classOfObject()
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
    if (needInitialization && baseCursor.isDone() == false) {
      initialize();
    }
    return baseCursor.isDone();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    if (needInitialization && baseCursor.isDoneOrInterrupted() == false) {
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

  @Override
  public void initialize()
  {
    if (columnValueSelector != null) {
      this.currentVal = this.columnValueSelector.getObject();
      this.unnestListForCurrentRow = new ArrayList<>();
      if (currentVal == null) {
        unnestListForCurrentRow = new ArrayList<>();
        unnestListForCurrentRow.add(null);
      } else {
        if (currentVal instanceof List) {
          unnestListForCurrentRow = (List<Object>) currentVal;
        } else if (currentVal.getClass().equals(String.class)) {
          unnestListForCurrentRow.add(currentVal);
        }
      }
      if (allowSet != null) {
        if (!allowSet.isEmpty()) {
          if (!allowSet.contains((String) unnestListForCurrentRow.get(index))) {
            advance();
          }
        }
      }
    }
    needInitialization = false;
  }

  @Override
  public void advanceAndUpdate()
  {
    if (unnestListForCurrentRow.isEmpty() || index >= unnestListForCurrentRow.size() - 1) {
      index = 0;
      baseCursor.advance();
      // get the next row
      if (!baseCursor.isDone()) {
        currentVal = columnValueSelector.getObject();
        if (currentVal == null) {
          unnestListForCurrentRow = new ArrayList<>();
          unnestListForCurrentRow.add(null);
        } else {
          if (currentVal instanceof List) {
            //convert array into array list
            unnestListForCurrentRow = (List<Object>) currentVal;
          } else if (currentVal instanceof String) {
            unnestListForCurrentRow = new ArrayList<>();
            unnestListForCurrentRow.add(currentVal);
          }
        }
      }
    } else {
      index++;
    }
  }

  @Override
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
