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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class DefaultColumnSelectorFactoryMaker implements ColumnSelectorFactoryMaker
{
  private final RowsAndColumns rac;

  public DefaultColumnSelectorFactoryMaker(RowsAndColumns rac)
  {
    this.rac = rac;
  }

  @Override
  public ColumnSelectorFactory make(AtomicInteger rowIdProvider)
  {
    return new ColumnAccessorBasedColumnSelectorFactory(rowIdProvider, rac);
  }

  public static class ColumnAccessorBasedColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final Map<String, ColumnAccessor> accessorCache = new HashMap<>();

    private final AtomicInteger cellIdSupplier;
    private final RowsAndColumns rac;

    public ColumnAccessorBasedColumnSelectorFactory(
        AtomicInteger cellIdSupplier,
        RowsAndColumns rac
    )
    {
      this.cellIdSupplier = cellIdSupplier;
      this.rac = rac;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return withColumnAccessor(dimensionSpec.getDimension(), columnAccessor -> {
        if (columnAccessor == null) {
          return DimensionSelector.nilSelector();
        } else {
          boolean maybeSupportsUtf8 = columnAccessor.getType().is(ValueType.STRING);
          int rowCounter = 0;
          int rowCount = columnAccessor.numRows();
          while (maybeSupportsUtf8 && rowCounter < rowCount && columnAccessor.isNull(rowCounter)) {
            ++rowCounter;
          }

          if (rowCounter == rowCount) {
            // We iterated through all of the things and got only nulls, might as well specialize to a null
            return DimensionSelector.nilSelector();
          }

          final boolean supportsUtf8 = maybeSupportsUtf8 && columnAccessor.getObject(rowCounter) instanceof ByteBuffer;

          return new BaseSingleValueDimensionSelector()
          {
            @Nullable
            @Override
            protected String getValue()
            {
              final Object retVal = columnAccessor.getObject(cellIdSupplier.get());
              if (retVal == null) {
                return null;
              }
              if (retVal instanceof ByteBuffer) {
                return StringUtils.fromUtf8(((ByteBuffer) retVal).asReadOnlyBuffer());
              }
              return String.valueOf(retVal);
            }

            @Nullable
            @Override
            public ByteBuffer lookupNameUtf8(int id)
            {
              return (ByteBuffer) columnAccessor.getObject(cellIdSupplier.get());
            }

            @Override
            public boolean supportsLookupNameUtf8()
            {
              return supportsUtf8;
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {

            }
          };
        }
      });
    }

    @SuppressWarnings("rawtypes")
    @Override
    public ColumnValueSelector makeColumnValueSelector(@Nonnull String columnName)
    {
      return withColumnAccessor(columnName, columnAccessor -> {
        if (columnAccessor == null) {
          return DimensionSelector.nilSelector();
        } else {
          final ColumnType type = columnAccessor.getType();
          switch (type.getType()) {
            case STRING:
              return new StringColumnValueSelector(columnAccessor);
            case COMPLEX:
              return new ComplexColumnValueSelector(columnAccessor);
            default:
              return new PassThroughColumnValueSelector(columnAccessor);
          }
        }
      });
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return withColumnAccessor(column, columnAccessor -> {
        if (columnAccessor == null) {
          return null;
        } else {
          return new ColumnCapabilitiesImpl()
              .setType(columnAccessor.getType())
              .setHasMultipleValues(false)
              .setDictionaryEncoded(false)
              .setHasBitmapIndexes(false);
        }
      });
    }

    private <T> T withColumnAccessor(String column, Function<ColumnAccessor, T> fn)
    {
      @Nullable
      ColumnAccessor retVal = accessorCache.get(column);
      if (retVal == null) {
        Column racColumn = rac.findColumn(column);
        retVal = racColumn == null ? null : racColumn.toAccessor();
        accessorCache.put(column, retVal);
      }
      return fn.apply(retVal);
    }

    private class PassThroughColumnValueSelector implements ColumnValueSelector
    {
      private final Class myClazz;
      private final ColumnAccessor columnAccessor;

      public PassThroughColumnValueSelector(
          ColumnAccessor columnAccessor
      )
      {
        this.columnAccessor = columnAccessor;
        switch (columnAccessor.getType().getType()) {
          case LONG:
            myClazz = long.class;
            break;
          case DOUBLE:
            myClazz = double.class;
            break;
          case FLOAT:
            myClazz = float.class;
            break;
          case ARRAY:
            myClazz = List.class;
          default:
            throw DruidException.defensive("this class cannot handle type [%s]", columnAccessor.getType());
        }
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return columnAccessor.getObject(cellIdSupplier.get());
      }

      @SuppressWarnings("rawtypes")
      @Override
      public Class classOfObject()
      {
        return myClazz;
      }

      @Override
      public boolean isNull()
      {
        return columnAccessor.isNull(cellIdSupplier.get());
      }

      @Override
      public long getLong()
      {
        return columnAccessor.getLong(cellIdSupplier.get());
      }

      @Override
      public float getFloat()
      {
        return columnAccessor.getFloat(cellIdSupplier.get());
      }

      @Override
      public double getDouble()
      {
        return columnAccessor.getDouble(cellIdSupplier.get());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    }

    private class StringColumnValueSelector implements ColumnValueSelector
    {
      private final ColumnAccessor columnAccessor;

      public StringColumnValueSelector(
          ColumnAccessor columnAccessor
      )
      {
        this.columnAccessor = columnAccessor;
      }

      @Nullable
      @Override
      public Object getObject()
      {
        // We want our String columns to be ByteBuffers, but users of this ColumnValueSelector interface
        // would generally expect String objects instead of UTF8 ByteBuffers, so we have to convert here
        // if we get a ByteBuffer.

        final Object retVal = columnAccessor.getObject(cellIdSupplier.get());
        if (retVal instanceof ByteBuffer) {
          return StringUtils.fromUtf8(((ByteBuffer) retVal).asReadOnlyBuffer());
        }
        return retVal;
      }

      @SuppressWarnings("rawtypes")
      @Override
      public Class classOfObject()
      {
        return String.class;
      }

      @Override
      public boolean isNull()
      {
        return columnAccessor.isNull(cellIdSupplier.get());
      }

      @Override
      public long getLong()
      {
        return columnAccessor.getLong(cellIdSupplier.get());
      }

      @Override
      public float getFloat()
      {
        return columnAccessor.getFloat(cellIdSupplier.get());
      }

      @Override
      public double getDouble()
      {
        return columnAccessor.getDouble(cellIdSupplier.get());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    }

    private class ComplexColumnValueSelector implements ColumnValueSelector
    {
      private final AtomicReference<Class> myClazz;
      private final ColumnAccessor columnAccessor;

      public ComplexColumnValueSelector(ColumnAccessor columnAccessor)
      {
        this.columnAccessor = columnAccessor;
        myClazz = new AtomicReference<>(null);
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return columnAccessor.getObject(cellIdSupplier.get());
      }

      @SuppressWarnings("rawtypes")
      @Override
      public Class classOfObject()
      {
        Class retVal = myClazz.get();
        if (retVal == null) {
          retVal = findClazz();
          myClazz.set(retVal);
        }
        return retVal;
      }

      private Class findClazz()
      {
        final ColumnType type = columnAccessor.getType();
        if (type.getType() == ValueType.COMPLEX) {
          final ComplexMetricSerde serdeForType = ComplexMetrics.getSerdeForType(type.getComplexTypeName());
          if (serdeForType != null && serdeForType.getObjectStrategy() != null) {
            return serdeForType.getObjectStrategy().getClazz();
          }

          for (int i = 0; i < columnAccessor.numRows(); ++i) {
            Object obj = columnAccessor.getObject(i);
            if (obj != null) {
              return obj.getClass();
            }
          }
          return Object.class;
        }
        throw DruidException.defensive("this class cannot handle type [%s]", type);
      }

      @Override
      public boolean isNull()
      {
        return columnAccessor.isNull(cellIdSupplier.get());
      }

      @Override
      public long getLong()
      {
        return columnAccessor.getLong(cellIdSupplier.get());
      }

      @Override
      public float getFloat()
      {
        return columnAccessor.getFloat(cellIdSupplier.get());
      }

      @Override
      public double getDouble()
      {
        return columnAccessor.getDouble(cellIdSupplier.get());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    }
  }
}
