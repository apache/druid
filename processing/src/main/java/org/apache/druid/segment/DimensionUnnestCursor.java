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

import com.google.common.base.Predicate;
import org.apache.druid.query.BaseQuery;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.BitSet;
import java.util.LinkedHashSet;

public class DimensionUnnestCursor implements Cursor
{
  private final Cursor baseCursor;
  private final DimensionSelector dimSelector;
  private final String columnName;
  private final String outputName;
  private final LinkedHashSet<String> allowSet;
  private final BitSet allowedBitSet;
  private final ColumnSelectorFactory baseColumnSelectorFactory;
  private int index;
  private IndexedInts indexedIntsForCurrentRow;
  private boolean needInitialization;
  private SingleIndexInts indexIntsForRow;

  public DimensionUnnestCursor(
      Cursor cursor,
      ColumnSelectorFactory baseColumnSelectorFactory,
      String columnName,
      String outputColumnName,
      LinkedHashSet<String> allowSet
  )
  {
    this.baseCursor = cursor;
    this.baseColumnSelectorFactory = baseColumnSelectorFactory;
    this.dimSelector = this.baseColumnSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
    this.columnName = columnName;
    this.index = 0;
    this.outputName = outputColumnName;
    this.needInitialization = true;
    this.allowSet = allowSet;
    this.allowedBitSet = new BitSet();
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

        final DimensionSpec actualDimensionSpec = dimensionSpec.withDimension(columnName);
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
            final int idForLookup = idLookup().lookupId(value);
            if (idForLookup < 0) {
              return new ValueMatcher()
              {
                @Override
                public boolean matches()
                {
                  return false;
                }

                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector inspector)
                {

                }
              };
            }

            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                return idForLookup == indexedIntsForCurrentRow.get(index);
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                baseColumnSelectorFactory.makeDimensionSelector(actualDimensionSpec).inspectRuntimeShape(inspector);
              }
            };
          }

          @Override
          public ValueMatcher makeValueMatcher(Predicate<String> predicate)
          {
            return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
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
            if (allowedBitSet.isEmpty()) {
              if (allowSet == null || allowSet.isEmpty()) {
                return lookupName(indexedIntsForCurrentRow.get(index));
              }
            } else if (allowedBitSet.get(indexedIntsForCurrentRow.get(index))) {
              return lookupName(indexedIntsForCurrentRow.get(index));
            }
            return null;
          }

          @Override
          public Class<?> classOfObject()
          {
            return Object.class;
          }

          @Override
          public int getValueCardinality()
          {
            if (!allowedBitSet.isEmpty()) {
              return allowedBitSet.cardinality();
            }
            return dimSelector.getValueCardinality();
          }

          @Nullable
          @Override
          public String lookupName(int id)
          {
            return dimSelector.lookupName(id);
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
            return dimSelector.idLookup();
          }
        };
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        if (!outputName.equals(columnName)) {
          return baseColumnSelectorFactory.makeColumnValueSelector(columnName);
        }
        return makeDimensionSelector(DefaultDimensionSpec.of(columnName));
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        if (!outputName.equals(columnName)) {
          baseColumnSelectorFactory.getColumnCapabilities(column);
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


  public void initialize()
  {
    IdLookup idLookup = dimSelector.idLookup();
    this.indexIntsForRow = new SingleIndexInts();
    if (allowSet != null && !allowSet.isEmpty() && idLookup != null) {
      for (String s : allowSet) {
        if (idLookup.lookupId(s) >= 0) {
          allowedBitSet.set(idLookup.lookupId(s));
        }
      }
    }
    if (dimSelector.getObject() != null) {
      this.indexedIntsForCurrentRow = dimSelector.getRow();
    }
    if (!allowedBitSet.isEmpty()) {
      if (!allowedBitSet.get(indexedIntsForCurrentRow.get(index))) {
        advance();
      }
    }
    needInitialization = false;
  }


  public void advanceAndUpdate()
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


  public boolean matchAndProceed()
  {
    boolean matchStatus;
    if ((allowSet == null || allowSet.isEmpty()) && allowedBitSet.isEmpty()) {
      matchStatus = true;
    } else {
      matchStatus = allowedBitSet.get(indexedIntsForCurrentRow.get(index));
    }
    return !baseCursor.isDone() && !matchStatus;
  }

  private class SingleIndexInts implements IndexedInts
  {

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {

    }

    @Override
    public int size()
    {
      return 1;
    }

    @Override
    public int get(int idx)
    {
      return indexedIntsForCurrentRow.get(index);
    }
  }
}
