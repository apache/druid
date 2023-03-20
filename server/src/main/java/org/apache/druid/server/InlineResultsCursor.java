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

package org.apache.druid.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import org.apache.druid.frame.write.UnsupportedColumnTypeException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.BaseQuery;
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
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

public class InlineResultsCursor implements Cursor
{

  private final SimpleSettableOffset offset;
  private final RowSignature rowSignature;
  private final List<Object[]> results;


  public InlineResultsCursor(List<Object[]> results, RowSignature rowSignature)
  {
    this.results = Preconditions.checkNotNull(results, "'results' cannot be null");
    this.rowSignature = rowSignature;
    this.offset = new SimpleAscendingOffset(results.size());
  }

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
        int index = offset.getOffset();
        return makeColumnValueSelectorFor(columnName, index);
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
    offset.increment();
    BaseQuery.checkInterrupted();
  }

  @Override
  public void advanceUninterruptibly()
  {
    offset.increment();
  }

  @Override
  public boolean isDone()
  {
    return !offset.withinBounds();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return !offset.withinBounds() || Thread.currentThread().isInterrupted();
  }

  @Override
  public void reset()
  {
    offset.reset();
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
        Object stringValueObject = results.get(offset.getOffset())[rowSignature.indexOf(dimensionSpec)];
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
        Object stringValueObject = results.get(offset.getOffset())[rowSignature.indexOf(dimensionSpec)];
        return stringValueObject;
      }

      @Override
      public Class<?> classOfObject()
      {
        Object stringValueObject = results.get(offset.getOffset())[rowSignature.indexOf(dimensionSpec)];
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
        Object stringValueObject = results.get(offset.getOffset())[rowSignature.indexOf(dimensionSpec)];
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

  private ColumnValueSelector makeColumnValueSelectorFor(String columnName, int index)
  {
    Optional<ColumnType> columnTypeOptional = rowSignature.getColumnType(columnName);
    if (!columnTypeOptional.isPresent()) {
      throw new ISE("Cannot find type for column [%s]", columnName);
    }
    ColumnType columnType = columnTypeOptional.get();
    ColumnValueSelector columnValueSelector;
    int columnNumber = rowSignature.indexOf(columnName);
    switch (columnType.getType()) {
      case LONG:
        columnValueSelector = makeColumnValueSelectorForLong(columnNumber, offset, results);
        break;
      case DOUBLE:
        columnValueSelector = makeColumnValueSelectorForDouble(columnNumber, offset, results);
        break;
      case FLOAT:
        columnValueSelector = makeColumnValueSelectorForFloat(columnNumber, offset, results);
        break;
      case ARRAY:
        switch (columnType.getElementType().getType()) {
          case STRING:
            columnValueSelector = makeColumnValueSelectorForStringArray(columnNumber, offset, results);
            break;
          default:
            throw new UnsupportedColumnTypeException(columnName, columnType);
        }
        break;
      case COMPLEX:
        columnValueSelector = makeColumnValueSelectorForObject(columnNumber, columnType, offset, results);
        break;
      default:
        throw new UnsupportedColumnTypeException(columnName, columnType);
    }
    return columnValueSelector;
  }

  private ColumnValueSelector makeColumnValueSelectorForLong(int columnNumber, Offset offset, List<Object[]> results)
  {
    return new LongColumnSelector()
    {

      @Override
      public long getLong()
      {
        Object val = results.get(offset.getOffset())[columnNumber];
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
        Object val = results.get(offset.getOffset())[columnNumber];
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForDouble(int columnNumber, Offset offset, List<Object[]> results)
  {
    return new DoubleColumnSelector()
    {
      @Override
      public double getDouble()
      {
        Object val = results.get(offset.getOffset())[columnNumber];
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
        Object val = results.get(offset.getOffset())[columnNumber];
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForFloat(int columnNumber, Offset offset, List<Object[]> results)
  {
    return new FloatColumnSelector()
    {
      @Override
      public float getFloat()
      {
        Object val = results.get(offset.getOffset())[columnNumber];
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
        Object val = results.get(offset.getOffset())[columnNumber];
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForObject(
      int columnNumber,
      ColumnType columnType,
      Offset offset,
      List<Object[]> results
  )
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        Object val = results.get(offset.getOffset())[columnNumber];
        return val;
      }

      @Override
      public Class classOfObject()
      {
        return ComplexMetrics.getSerdeForType(columnType.getComplexTypeName()).getExtractor().extractedClass();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForStringArray(
      int columnNumber,
      Offset offset,
      List<Object[]> results
  )
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
        Object val = results.get(offset.getOffset())[columnNumber];
        return val;
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
