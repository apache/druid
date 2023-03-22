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

package org.apache.druid.query;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DoubleColumnSelector;
import org.apache.druid.segment.FloatColumnSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.ObjectColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An implementation of {@link Cursor} used to iterate over the fields of a list of rows. This is used by the
 * to iterate over the results of an inline subquery to serialize it into a frame
 */
public class IterableRowsCursor implements Cursor
{

  private final RowSignature rowSignature;
  private final Iterable<Object[]> results;
  private final AtomicReference<Object[]> curRow = new AtomicReference<>(null);
  private Iterator<Object[]> iterator;

  /**
   * @param results      results for the cursor to iterate over
   * @param rowSignature row signature of the result row
   */
  public IterableRowsCursor(Iterable<Object[]> results, RowSignature rowSignature)
  {
    this.results = Preconditions.checkNotNull(results, "'results' cannot be null");
    this.rowSignature = rowSignature;
    this.iterator = results.iterator();
    if (this.iterator.hasNext()) {
      curRow.set(this.iterator.next());
    }
  }

  @Nonnull
  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return new ColumnSelectorFactory()
    {
      @Override
      public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
      {
        return makeDimensionSelectorFor(dimensionSpec);
      }

      @Override
      public ColumnValueSelector makeColumnValueSelector(String columnName)
      {
        return makeColumnValueSelectorFor(columnName);
      }

      @Nullable
      @Override
      public ColumnCapabilities getColumnCapabilities(String column)
      {
        return rowSignature.getColumnCapabilities(column);
      }
    };
  }

  @Override
  public DateTime getTime()
  {
    return DateTimes.MIN;
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
    if (iterator.hasNext()) {
      curRow.set(iterator.next());
    } else {
      curRow.set(null);
    }
  }

  @Override
  public boolean isDone()
  {
    return curRow.get() == null;
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    this.iterator = results.iterator();
    if (this.iterator.hasNext()) {
      curRow.set(this.iterator.next());
    } else {
      curRow.set(null);
    }
  }

  private DimensionSelector makeDimensionSelectorFor(DimensionSpec dimensionSpec)
  {
    return dimensionSpec.decorate(makeUndecoratedDimensionSelectorFor(dimensionSpec.getDimension()));
  }

  private DimensionSelector makeUndecoratedDimensionSelectorFor(String dimensionSpec)
  {
    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        Object stringValueObject = curRow.get()[rowSignature.indexOf(dimensionSpec)];
        RangeIndexedInts rangeIndexedInts = new RangeIndexedInts();
        if (stringValueObject instanceof String) {
          rangeIndexedInts.setSize(1);
        } else if (stringValueObject instanceof List) {
          rangeIndexedInts.setSize(((List<?>) stringValueObject).size());
        }
        return rangeIndexedInts;
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
        return curRow.get()[rowSignature.indexOf(dimensionSpec)];
      }

      @Override
      public Class<?> classOfObject()
      {
        Object stringValueObject = curRow.get()[rowSignature.indexOf(dimensionSpec)];
        if (stringValueObject instanceof String) {
          return String.class;
        } else if (stringValueObject instanceof List) {
          return List.class;
        } else {
          return Object.class;
        }
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
        Object stringValueObject = curRow.get()[rowSignature.indexOf(dimensionSpec)];
        if (stringValueObject instanceof String) {
          if (id == 0) {
            return (String) stringValueObject;
          } else {
            throw new IndexOutOfBoundsException();
          }
        } else if (stringValueObject instanceof List) {
          List<?> stringValueList = (List<?>) stringValueObject;
          if (id < stringValueList.size()) {
            return (String) stringValueList.get(id);
          } else {
            throw new IndexOutOfBoundsException();
          }
        } else {
          throw new UnsupportedOperationException();
        }
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

  private ColumnValueSelector makeColumnValueSelectorFor(String columnName)
  {
    int columnNumber = rowSignature.indexOf(columnName);
    Optional<ColumnType> columnTypeOptional = rowSignature.getColumnType(columnName);

    // If the type information is not present, then return the column value selector for null;
    if (!columnTypeOptional.isPresent()) {
      if (curRow.get()[columnNumber] == null) {
        return makeColumnValueSelectorForLong(columnNumber);
      } else {
        throw new ISE("Cannot make colunValueSelectorFor");
      }
    }

    ColumnType columnType = columnTypeOptional.get();
    ColumnValueSelector columnValueSelector;
    switch (columnType.getType()) {
      case LONG:
        columnValueSelector = makeColumnValueSelectorForLong(columnNumber);
        break;
      case DOUBLE:
        columnValueSelector = makeColumnValueSelectorForDouble(columnNumber);
        break;
      case FLOAT:
        columnValueSelector = makeColumnValueSelectorForFloat(columnNumber);
        break;
      case ARRAY:
        switch (columnType.getElementType().getType()) {
          case STRING:
            columnValueSelector = makeColumnValueSelectorForStringArray(columnNumber);
            break;
          default:
            throw new UnsupportedColumnTypeException(columnName, columnType);
        }
        break;
      case COMPLEX:
        columnValueSelector = makeColumnValueSelectorForObject(columnNumber, columnType);
        break;
      default:
        throw new UnsupportedColumnTypeException(columnName, columnType);
    }
    return columnValueSelector;
  }

  private ColumnValueSelector makeColumnValueSelectorForLong(int columnNumber)
  {
    return new LongColumnSelector()
    {

      @Override
      public long getLong()
      {
        Object val = curRow.get()[columnNumber];
        if (val == null) {
          return 0;
        }
        return ((Number) val).longValue();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public boolean isNull()
      {
        Object val = curRow.get()[columnNumber];
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForDouble(int columnNumber)
  {
    return new DoubleColumnSelector()
    {
      @Override
      public double getDouble()
      {
        Object val = curRow.get()[columnNumber];
        if (val == null) {
          return 0;
        }
        return (Double) val;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public boolean isNull()
      {
        Object val = curRow.get()[columnNumber];
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForFloat(int columnNumber)
  {
    return new FloatColumnSelector()
    {
      @Override
      public float getFloat()
      {
        Object val = curRow.get()[columnNumber];
        if (val == null) {
          return 0;
        }
        return (Float) val;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public boolean isNull()
      {
        Object val = curRow.get()[columnNumber];
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForObject(
      int columnNumber,
      ColumnType columnType
  )
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        return curRow.get()[columnNumber];
      }

      @Override
      public Class<?> classOfObject()
      {
        return ComplexMetrics.getSerdeForType(columnType.getComplexTypeName()).getExtractor().extractedClass();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForStringArray(int columnNumber)
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        return curRow.get()[columnNumber];
      }

      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    };
  }
}
