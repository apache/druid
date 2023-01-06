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

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * The cursor to help unnest MVDs without dictionary encoding and ARRAY type selectors.
 * <p>
 * Consider a segment has 2 rows
 * ['a', 'b', 'c']
 * ['d', 'e']
 * <p>
 * The baseCursor points to the row ['a', 'b', 'c']
 * while the unnestCursor with each call of advance() moves over individual elements.
 * <p>
 * unnestCursor.advance() -> 'a'
 * unnestCursor.advance() -> 'b'
 * unnestCursor.advance() -> 'c'
 * unnestCursor.advance() -> 'd' (advances base cursor first)
 * unnestCursor.advance() -> 'e'
 * <p>
 * <p>
 * The allowSet if available helps skip over elements which are not in the allowList by moving the cursor to
 * the next available match.
 * <p>
 * The index reference points to the index of each row that the unnest cursor is accessing through currentVal
 * The index ranges from 0 to the size of the list in each row which is held in the unnestListForCurrentRow
 * <p>
 * The needInitialization flag sets up the initial values of unnestListForCurrentRow at the beginning of the segment
 */
public class UnnestColumnValueSelectorCursor implements Cursor
{
  private final Cursor baseCursor;
  private final ColumnSelectorFactory baseColumnSelectorFactory;
  private final ColumnValueSelector columnValueSelector;
  private final String columnName;
  private final String outputName;
  private final LinkedHashSet<String> allowSet;
  private int index;
  private Object currentVal;
  private List<Object> unnestListForCurrentRow;
  private boolean needInitialization;

  public UnnestColumnValueSelectorCursor(
      Cursor cursor,
      ColumnSelectorFactory baseColumSelectorFactory,
      String columnName,
      String outputColumnName,
      LinkedHashSet<String> allowSet
  )
  {
    this.baseCursor = cursor;
    this.baseColumnSelectorFactory = baseColumSelectorFactory;
    this.columnValueSelector = this.baseColumnSelectorFactory.makeColumnValueSelector(columnName);
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
          return baseColumnSelectorFactory.makeDimensionSelector(dimensionSpec);
        }
        throw new UOE("Unsupported dimension selector while using column value selector for column [%s]", outputName);
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        if (!outputName.equals(columnName)) {
          return baseColumnSelectorFactory.makeColumnValueSelector(columnName);
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
            if (value instanceof Number) {
              return ((Number) value).doubleValue();
            }
            throw new UOE("Cannot convert object to double");
          }

          @Override
          public float getFloat()
          {
            Object value = getObject();
            if (value == null) {
              return 0;
            }
            if (value instanceof Number) {
              return ((Number) value).floatValue();
            }
            throw new UOE("Cannot convert object to float");
          }

          @Override
          public long getLong()
          {
            Object value = getObject();
            if (value == null) {
              return 0;
            }
            if (value instanceof Number) {
              return ((Number) value).longValue();
            }
            throw new UOE("Cannot convert object to long");
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
        if (!outputName.equals(column)) {
          return baseColumnSelectorFactory.getColumnCapabilities(column);
        }
        final ColumnCapabilities capabilities = baseColumnSelectorFactory.getColumnCapabilities(columnName);
        if (capabilities.isArray()) {
          return ColumnCapabilitiesImpl.copyOf(capabilities).setType(capabilities.getElementType());
        }
        if (capabilities.hasMultipleValues().isTrue()) {
          return ColumnCapabilitiesImpl.copyOf(capabilities).setHasMultipleValues(false);
        }
        return baseColumnSelectorFactory.getColumnCapabilities(columnName);
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

  /**
   * This method populates the objects when the base cursor moves to the next row
   *
   * @param firstRun flag to populate one time object references to hold values for unnest cursor
   */
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
      } else if (currentVal instanceof Object[]) {
        unnestListForCurrentRow = Arrays.asList((Object[]) currentVal);
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
  private void initialize()
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
  private void advanceAndUpdate()
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
  private boolean matchAndProceed()
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
