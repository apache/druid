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
import java.util.Arrays;
import java.util.Collections;
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
  private final VirtualColumn unnestColumn;
  private final String outputName;
  private int index;
  private Object currentVal;
  private List<Object> unnestListForCurrentRow;
  private boolean needInitialization;


  public UnnestColumnValueSelectorCursor(
      Cursor cursor,
      ColumnSelectorFactory baseColumnSelectorFactory,
      VirtualColumn unnestColumn,
      String outputColumnName
  )
  {
    this.baseCursor = cursor;
    this.baseColumnSelectorFactory = baseColumnSelectorFactory;
    this.columnValueSelector = unnestColumn.makeColumnValueSelector(
        unnestColumn.getOutputName(),
        this.baseColumnSelectorFactory
    );
    this.unnestColumn = unnestColumn;
    this.index = 0;
    this.outputName = outputColumnName;
    this.needInitialization = true;
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
        // this is done to support virtual columns
        // In future a developer should move towards making sure that
        // for all dictionary encoded cases we only get the dimension selector
        return new BaseSingleValueDimensionSelector()
        {
          final ColumnValueSelector colSelector = makeColumnValueSelector(dimensionSpec.getDimension());

          @Nullable
          @Override
          protected String getValue()
          {
            final Object returnedObj = colSelector.getObject();
            if (returnedObj == null) {
              return null;
            } else {
              return String.valueOf(returnedObj);
            }
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            colSelector.inspectRuntimeShape(inspector);
          }
        };
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
            return unnestListForCurrentRow.get(index);
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

        final ColumnCapabilities capabilities = unnestColumn.capabilities(
            baseColumnSelectorFactory,
            unnestColumn.getOutputName()
        );

        if (capabilities == null) {
          return null;
        } else if (capabilities.isArray()) {
          return ColumnCapabilitiesImpl.copyOf(capabilities).setType(capabilities.getElementType());
        } else if (capabilities.hasMultipleValues().isTrue()) {
          return ColumnCapabilitiesImpl.copyOf(capabilities).setHasMultipleValues(false);
        } else {
          return capabilities;
        }
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
    advanceAndUpdate();
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
   */
  private void getNextRow()
  {
    currentVal = this.columnValueSelector.getObject();
    if (currentVal == null) {
      unnestListForCurrentRow = Collections.emptyList();
    } else if (currentVal instanceof List) {
      unnestListForCurrentRow = (List<Object>) currentVal;
    } else if (currentVal instanceof Object[]) {
      unnestListForCurrentRow = Arrays.asList((Object[]) currentVal);
    } else {
      unnestListForCurrentRow = Collections.singletonList(currentVal);
    }
  }

  /**
   * This initializes the unnest cursor and creates data structures
   * to start iterating over the values to be unnested.
   */
  private void initialize()
  {
    getNextRow();
    if (unnestListForCurrentRow.isEmpty()) {
      moveToNextNonEmptyRow();
    }
    needInitialization = false;
  }

  private void moveToNextNonEmptyRow()
  {
    index = 0;
    do {
      baseCursor.advance();
      if (!baseCursor.isDone()) {
        getNextRow();
      } else {
        return;
      }
    } while (unnestListForCurrentRow.isEmpty());
  }

  /**
   * This advances the cursor to move to the next element to be unnested.
   * When the last element in a row is unnested, it is also responsible
   * to move the base cursor to the next row for unnesting and repopulates
   * the data structures, created during initialize(), to point to the new row
   */
  private void advanceAndUpdate()
  {
    if (++index >= unnestListForCurrentRow.size()) {
      moveToNextNonEmptyRow();
    }
  }
}
