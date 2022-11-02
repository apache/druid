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
import java.util.ArrayList;
import java.util.BitSet;
import java.util.LinkedHashSet;
import java.util.List;


public class UnnestCursor implements Cursor
{
  private final Cursor baseCursor;
  private final ColumnSelectorFactory baseColumSelectorFactory;
  private final DimensionSelector dimSelector;
  private final ColumnValueSelector columnValueSelector;
  private final String columnName;
  private final String outputName;
  private final LinkedHashSet<String> allowSet;
  private final BitSet allowedBitSet;
  private int index;
  private Object currentVal;
  private IndexedInts pos;
  private List<Object> unnestList;
  private boolean needInitialization;

  public UnnestCursor(Cursor cursor, String columnName, String outputColumnName, LinkedHashSet<String> allowSet)
  {
    this.baseCursor = cursor;
    this.baseColumSelectorFactory = cursor.getColumnSelectorFactory();
    this.dimSelector = this.baseColumSelectorFactory.makeDimensionSelector(DefaultDimensionSpec.of(columnName));
    this.columnValueSelector = this.baseColumSelectorFactory.makeColumnValueSelector(columnName);
    this.columnName = columnName;
    this.index = 0;
    this.outputName = outputColumnName;
    this.needInitialization = true;
    this.allowSet = allowSet;
    allowedBitSet = new BitSet();
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

        final DimensionSpec actualDimensionSpec = dimensionSpec.withDimension(columnName);
        return new DimensionSelector()
        {
          @Override
          public IndexedInts getRow()
          {
            return dimSelector.getRow();
          }

          @Override
          public ValueMatcher makeValueMatcher(@Nullable String value)
          {
            return new ValueMatcher()
            {
              @Override
              public boolean matches()
              {
                return lookupName(pos.get(index)).equals(value);
              }

              @Override
              public void inspectRuntimeShape(RuntimeShapeInspector inspector)
              {
                baseColumSelectorFactory.makeDimensionSelector(actualDimensionSpec).inspectRuntimeShape(inspector);
              }
            };
          }

          @Override
          public ValueMatcher makeValueMatcher(Predicate<String> predicate)
          {
            return null;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            baseCursor.getColumnSelectorFactory()
                      .makeDimensionSelector(actualDimensionSpec)
                      .inspectRuntimeShape(inspector);
          }

          @Nullable
          @Override
          public Object getObject()
          {
            if (pos != null) {
              if (allowedBitSet.isEmpty()) {
                if (allowSet == null || allowSet.isEmpty()) {
                  return lookupName(pos.get(index));
                }
              } else if (allowedBitSet.get(pos.get(index))) {
                return lookupName(pos.get(index));
              }
            } else {
              return null;
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
            return 0;
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
        if (!outputName.equals(columnName)) {
          return baseColumSelectorFactory.makeColumnValueSelector(columnName);
        }

        return new ColumnValueSelector()
        {
          //Implement these
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
            if (pos != null) {
              if (allowedBitSet.isEmpty()) {
                // when bitset is empty for a segment
                // but the allowSet is non-empty
                // the entire segment can be skipped
                if (allowSet == null || allowSet.isEmpty()) {
                  return dimSelector.lookupName(pos.get(index));
                }
              } else {
                if (allowedBitSet.get(pos.get(index))) {
                  return dimSelector.lookupName(pos.get(index));
                }
              }
            } else {
              if (!unnestList.isEmpty()) {
                if (allowSet == null || allowSet.isEmpty()) {
                  return unnestList.get(index);
                } else if (allowSet.contains((String) unnestList.get(index))) {
                  return unnestList.get(index);
                }
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
        return baseColumSelectorFactory.getColumnCapabilities(column);
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
    // check if dimension selector
    boolean status = checkIfDimensionSelectorAndAdvance();
    // if status is false check if column selector
    if (!status) {
      checkIfColumnSelectorAndAdvance();
    }
  }

  private void checkIfColumnSelectorAndAdvance()
  {
    if (unnestList.isEmpty() || index >= unnestList.size() - 1) {
      index = 0;
      baseCursor.advance();
      // get the next row
      if (!baseCursor.isDone()) {
        currentVal = columnValueSelector.getObject();
        if (currentVal == null) {
          unnestList = new ArrayList<>();
          unnestList.add(null);
        } else {
          if (currentVal instanceof List) {
            //convert array into array list
            unnestList = (List<Object>) currentVal;
          } else if (currentVal instanceof String) {
            unnestList = new ArrayList<>();
            unnestList.add(currentVal);
          }
        }
      }
    } else {
      index++;
    }
    //index has been decided before this point
    //decide whether to stay at it or increase it

  }

  private boolean checkIfDimensionSelectorAndAdvance()
  {
    if (this.dimSelector != null && pos != null) {
      if (index >= pos.size() - 1) {
        if (!baseCursor.isDone()) {
          baseCursor.advanceUninterruptibly();
        }
        if (!baseCursor.isDone()) {
          pos = dimSelector.getRow();
        }
        index = 0;
      } else {
        ++index;
      }
      return true;
    }
    return false;
  }

  private void initialize()
  {
    if (dimSelector != null) {
      IdLookup idLookup = dimSelector.idLookup();
      if (allowSet != null && !allowSet.isEmpty() && idLookup != null) {
        for (String s : allowSet) {
          if (idLookup.lookupId(s) >= 0) {
            allowedBitSet.set(idLookup.lookupId(s));
          }
        }
      }
      if (dimSelector.getObject() instanceof List) {
        this.pos = dimSelector.getRow();
      }
    }
    if (columnValueSelector != null) {
      this.currentVal = this.columnValueSelector.getObject();
      this.unnestList = new ArrayList<>();
      if (currentVal == null) {
        unnestList = new ArrayList<>();
        unnestList.add(null);
      } else {
        if (currentVal instanceof List) {
          unnestList = (List<Object>) currentVal;
        } else if (currentVal.getClass().equals(String.class)) {
          unnestList.add(currentVal);
        }
      }
    }
    needInitialization = false;
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
    return baseCursor.isDoneOrInterrupted();
  }

  @Override
  public void reset()
  {
    index = 0;
    needInitialization = true;
    baseCursor.reset();
  }

}

