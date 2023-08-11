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
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;


/**
 * A Cursor that iterates over a user created list.
 * This is used to test the base cursor of an UnnestCursor.
 * Usages can be found in tests of {@link UnnestColumnValueSelectorCursor} in {@link UnnestColumnValueSelectorCursorTest}
 * However this cannot help with {@link UnnestDimensionCursor}.
 * Tests for {@link UnnestDimensionCursor} are done alongside tests for {@link UnnestStorageAdapterTest}
 */
public class ListCursor implements Cursor
{
  private final List<Object> baseList;
  private int index;

  public ListCursor(List<Object> inputList)
  {
    this.baseList = inputList;
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
            throw new UnsupportedOperationException();
          }

          @Override
          public ValueMatcher makeValueMatcher(@Nullable String value)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public ValueMatcher makeValueMatcher(Predicate<String> predicate)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {

          }

          @Nullable
          @Override
          public Object getObject()
          {
            if (index < baseList.size()) {
              return baseList.get(index);
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
            return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
          }

          @Nullable
          @Override
          public String lookupName(int id)
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public boolean nameLookupPossibleInAdvance()
          {
            return false;
          }

          @Nullable
          @Override
          public IdLookup idLookup()
          {
            return null;
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
            return 0;
          }

          @Override
          public float getFloat()
          {
            return 0;
          }

          @Override
          public long getLong()
          {
            return 0;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {

          }

          @Override
          public boolean isNull()
          {
            return false;
          }

          @Nullable
          @Override
          public Object getObject()
          {
            if (index < baseList.size()) {
              return baseList.get(index);
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
        return null;
      }
    };
  }

  @Override
  public DateTime getTime()
  {
    throw new UnsupportedOperationException();
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
    index++;
  }

  @Override
  public boolean isDone()
  {
    return index > baseList.size() - 1;
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return false;
  }

  @Override
  public void reset()
  {
    index = 0;
  }
}
