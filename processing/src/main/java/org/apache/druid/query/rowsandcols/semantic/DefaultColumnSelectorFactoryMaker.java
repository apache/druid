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

import org.apache.druid.java.util.common.ISE;
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
import org.apache.druid.segment.serde.ComplexMetricSerde;
import org.apache.druid.segment.serde.ComplexMetrics;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
          return DimensionSelector.constant(null);
        } else {
          return new BaseSingleValueDimensionSelector()
          {
            @Nullable
            @Override
            protected String getValue()
            {
              return String.valueOf(columnAccessor.getObject(cellIdSupplier.get()));
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
          return DimensionSelector.constant(null);
        } else {
          return new ColumnValueSelector()
          {
            private final AtomicReference<Class> myClazz = new AtomicReference<>(null);

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
              switch (type.getType()) {
                case LONG:
                  return long.class;
                case DOUBLE:
                  return double.class;
                case FLOAT:
                  return float.class;
                case STRING:
                  return String.class;
                case ARRAY:
                  return List.class;
                case COMPLEX:
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
                default:
                  throw new ISE("Unknown type[%s]", type.getType());
              }
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
          };
        }
      });
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return withColumnAccessor(column, columnAccessor ->
          new ColumnCapabilitiesImpl()
              .setType(columnAccessor.getType())
              .setDictionaryEncoded(false)
              .setHasBitmapIndexes(false));
    }

    private <T> T withColumnAccessor(String column, Function<ColumnAccessor, T> fn)
    {
      ColumnAccessor retVal = accessorCache.get(column);
      if (retVal == null) {
        Column racColumn = rac.findColumn(column);
        retVal = racColumn == null ? null : racColumn.toAccessor();
        accessorCache.put(column, retVal);
      }
      return fn.apply(retVal);
    }
  }
}
