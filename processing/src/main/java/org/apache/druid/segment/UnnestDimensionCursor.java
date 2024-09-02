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

import com.google.common.base.Preconditions;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

import javax.annotation.Nullable;

/**
 * The cursor to help unnest MVDs with dictionary encoding.
 * Consider a segment has 2 rows
 * ['a', 'b', 'c']
 * ['d', 'c']
 * <p>
 * Considering dictionary encoding, these are represented as
 * <p>
 * 'a' -> 0
 * 'b' -> 1
 * 'c' -> 2
 * 'd' -> 3
 * <p>
 * The baseCursor points to the row of IndexedInts [0, 1, 2]
 * while the unnestCursor with each call of advance() moves over individual elements.
 * <p>
 * advance() -> 0 -> 'a'
 * advance() -> 1 -> 'b'
 * advance() -> 2 -> 'c'
 * advance() -> 3 -> 'd' (advances base cursor first)
 * advance() -> 2 -> 'c'
 * <p>
 * Total 5 advance calls above
 * <p>
 * <p>
 * The index reference points to the index of each row that the unnest cursor is accessing
 * The indexedInts for each row are held in the indexedIntsForCurrentRow object
 * <p>
 * The needInitialization flag sets up the initial values of indexedIntsForCurrentRow at the beginning of the segment
 */
public class UnnestDimensionCursor implements Cursor
{
  private final Cursor baseCursor;
  private final DimensionSelector dimSelector;
  private final VirtualColumn unnestColumn;
  private final String outputName;
  private final ColumnSelectorFactory baseColumnSelectorFactory;
  private int index;
  @MonotonicNonNull
  private IndexedInts indexedIntsForCurrentRow;
  private boolean needInitialization;
  @MonotonicNonNull
  private SingleIndexInts indexIntsForRow;
  private final int nullId;
  private final int idOffset;

  public UnnestDimensionCursor(
      Cursor cursor,
      ColumnSelectorFactory baseColumnSelectorFactory,
      VirtualColumn unnestColumn,
      String outputColumnName
  )
  {
    this.baseCursor = cursor;
    this.baseColumnSelectorFactory = baseColumnSelectorFactory;
    this.dimSelector = unnestColumn.makeDimensionSelector(
        DefaultDimensionSpec.of(unnestColumn.getOutputName()),
        this.baseColumnSelectorFactory
    );
    this.unnestColumn = unnestColumn;
    this.index = 0;
    this.outputName = outputColumnName;
    this.needInitialization = true;
    // this shouldn't happen, but just in case...
    final IdLookup lookup = Preconditions.checkNotNull(dimSelector.idLookup());
    final int nullId = lookup.lookupId(null);
    if (nullId < 0) {
      this.idOffset = 1;
      this.nullId = 0;
    } else {
      this.idOffset = 0;
      this.nullId = nullId;
    }
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

        return new DimensionSelector()
        {
          @Override
          public IndexedInts getRow()
          {
            return indexIntsForRow;
          }

          @Override
          public ValueMatcher makeValueMatcher(@Nullable String value)
          {
            final int idForLookup = dimSelector.idLookup().lookupId(value);
            if (idForLookup < 0) {
              return new ValueMatcher()
              {
                @Override
                public boolean matches(boolean includeUnknown)
                {
                  // don't match anything, except for null values when includeUnknown is true
                  if (includeUnknown) {
                    if (indexedIntsForCurrentRow == null || indexedIntsForCurrentRow.size() <= 0) {
                      return true;
                    }
                    final int rowId = indexedIntsForCurrentRow.get(index);
                    return dimSelector.lookupName(rowId) == null;
                  }
                  return false;
                }

                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                {
                  inspector.visit("selector", dimSelector);
                }
              };
            }

            return new ValueMatcher()
            {
              @Override
              public boolean matches(boolean includeUnknown)
              {
                if (indexedIntsForCurrentRow == null) {
                  return includeUnknown;
                }
                if (indexedIntsForCurrentRow.size() <= 0) {
                  return includeUnknown;
                }
                final int rowId = indexedIntsForCurrentRow.get(index);
                return (includeUnknown && dimSelector.lookupName(rowId) == null) || idForLookup == rowId;
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                inspector.visit("selector", dimSelector);
              }
            };
          }

          @Override
          public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
          {
            return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicateFactory);
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            dimSelector.inspectRuntimeShape(inspector);
          }

          @Nullable
          @Override
          public Object getObject()
          {
            if (indexedIntsForCurrentRow.size() == 0) {
              return null;
            }
            return dimSelector.lookupName(indexedIntsForCurrentRow.get(index));
          }

          @Override
          public Class<?> classOfObject()
          {
            return Object.class;
          }

          @Override
          public int getValueCardinality()
          {
            return dimSelector.getValueCardinality() + idOffset;
          }

          @Nullable
          @Override
          public String lookupName(int id)
          {
            return dimSelector.lookupName(id - idOffset);
          }

          @Override
          public boolean nameLookupPossibleInAdvance()
          {
            return dimSelector.nameLookupPossibleInAdvance();
          }

          @Nullable
          @Override
          public IdLookup idLookup()
          {
            return name -> name == null ? nullId : dimSelector.idLookup().lookupId(name) + idOffset;
          }
        };
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        if (outputName.equals(columnName)) {
          return makeDimensionSelector(DefaultDimensionSpec.of(columnName));
        }

        return baseColumnSelectorFactory.makeColumnValueSelector(columnName);
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        if (outputName.equals(column)) {
          return UnnestCursorFactory.computeOutputColumnCapabilities(baseColumnSelectorFactory, unnestColumn);
        }

        return baseColumnSelectorFactory.getColumnCapabilities(column);
      }
    };
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
   * This initializes the unnest cursor and creates data structures
   * to start iterating over the values to be unnested.
   * This would also create a bitset for dictonary encoded columns to
   * check for matching values specified in allowedList of UnnestDataSource.
   */
  @Nullable
  private void initialize()
  {
    index = 0;
    this.indexIntsForRow = new SingleIndexInts();
    this.indexedIntsForCurrentRow = dimSelector.getRow();
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
    if (index >= indexedIntsForCurrentRow.size() - 1) {
      if (!baseCursor.isDone()) {
        baseCursor.advanceUninterruptibly();
      }
      if (!baseCursor.isDone()) {
        indexedIntsForCurrentRow = dimSelector.getRow();
      }
      index = 0;
    } else {
      ++index;
    }
  }

  // Helper class to help in returning
  // getRow from the dimensionSelector
  // This is set in the initialize method
  private class SingleIndexInts implements IndexedInts
  {

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      //nothing to inspect
    }

    @Override
    public int size()
    {
      // After unnest each row will have a single element
      return 1;
    }

    @Override
    public int get(int idx)
    {
      // everything that calls get also checks size
      if (indexedIntsForCurrentRow.size() == 0) {
        return nullId;
      }
      return idOffset + indexedIntsForCurrentRow.get(index);
    }
  }
}
