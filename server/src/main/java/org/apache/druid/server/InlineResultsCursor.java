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


  public InlineResultsCursor(List<Object[]> results, RowSignature rowSignature) {
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
        return null;
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
    return offset.withinBounds();
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return offset.withinBounds() || Thread.currentThread().isInterrupted();
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
    Object string
    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        return null;
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
        return null;
      }

      @Override
      public Class<?> classOfObject()
      {
        return null;
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
        return null;
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
    Optional<ColumnType> columnTypeOptional = rowSignature.getColumnType(columnName);
    if (!columnTypeOptional.isPresent()) {
      throw new ISE("Cannot find type for column [%s]", columnName);
    }
    ColumnType columnType = columnTypeOptional.get();
    ColumnValueSelector columnValueSelector;
    Object columnValue = results.get(offset.getOffset())[rowSignature.indexOf(columnName)];
    switch (columnType.getType()) {
      case LONG:
        columnValueSelector = makeColumnValueSelectorForLong((Long) columnValue);
        break;
      case DOUBLE:
        columnValueSelector = makeColumnValueSelectorForDouble((Double) columnValue);
        break;
      case FLOAT:
        columnValueSelector = makeColumnValueSelectorForFloat((Float) columnValue);
        break;
      case ARRAY:
        switch (columnType.getElementType().getType()) {
          case STRING:
             columnValueSelector = makeColumnValueSelectorForStringArray(columnValue);
             break;
          default:
            throw new UnsupportedColumnTypeException(columnName, columnType);
        }
        break;
      case COMPLEX:
        columnValueSelector = makeColumnValueSelectorForObject(columnValue, columnType);
        break;
      default:
        throw new UnsupportedColumnTypeException(columnName, columnType);
    }
    return columnValueSelector;
  }

  private ColumnValueSelector makeColumnValueSelectorForLong(Long val)
  {
    return new LongColumnSelector()
    {
      @Override
      public long getLong()
      {
        if (val == null) {
          return 0;
        }
        return val;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public boolean isNull()
      {
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForDouble(Double val)
  {
    return new DoubleColumnSelector()
    {
      @Override
      public double getDouble()
      {
        if (val == null) {
          return 0;
        }
        return val;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public boolean isNull()
      {
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForFloat(Float val)
  {
    return new FloatColumnSelector()
    {
      @Override
      public float getFloat()
      {
        if (val == null) {
          return 0;
        }
        return val;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }

      @Override
      public boolean isNull()
      {
        return val == null;
      }
    };
  }

  private ColumnValueSelector makeColumnValueSelectorForObject(Object val, ColumnType columnType)
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
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

  private ColumnValueSelector makeColumnValueSelectorForStringArray(Object val)
  {
    return new ObjectColumnSelector()
    {
      @Nullable
      @Override
      public Object getObject()
      {
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
