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

package org.apache.druid.query.scan;

import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.function.Supplier;

/**
 * Expected to work correctly if the individual cursors have the same corresponding columns, columnTypes,
 * and column capabilities since the usual pattern is to cache these methods and assume that the signature is
 * fixed throughout
 */
@NotThreadSafe
public class SettableCursorColumnSelectorFactory implements ColumnSelectorFactory
{

  // supplier should never return null
  private final Supplier<Cursor> cursorSupplier;
  private final RowSignature rowSignature;

  public SettableCursorColumnSelectorFactory(final Supplier<Cursor> cursorSupplier, final RowSignature rowSignature)
  {
    this.cursorSupplier = cursorSupplier;
    this.rowSignature = rowSignature;
  }

  @Nullable
  @Override
  public ExpressionType getType(String name)
  {
    return cursorSupplier.get().getColumnSelectorFactory().getType(name);
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        return cursorSupplier.get().getColumnSelectorFactory().makeDimensionSelector(dimensionSpec).getRow();
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        return new ValueMatcher()
        {
          @Override
          public boolean matches(boolean includeUnknown)
          {
            return cursorSupplier.get()
                                 .getColumnSelectorFactory()
                                 .makeDimensionSelector(dimensionSpec)
                                 .makeValueMatcher(value)
                                 .matches(includeUnknown);
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            cursorSupplier.get()
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .makeValueMatcher(value)
                          .inspectRuntimeShape(inspector);
          }
        };
      }

      @Override
      public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
      {
        return new ValueMatcher()
        {
          @Override
          public boolean matches(boolean includeUnknown)
          {
            return cursorSupplier.get()
                                 .getColumnSelectorFactory()
                                 .makeDimensionSelector(dimensionSpec)
                                 .makeValueMatcher(predicateFactory)
                                 .matches(includeUnknown);
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            cursorSupplier.get()
                          .getColumnSelectorFactory()
                          .makeDimensionSelector(dimensionSpec)
                          .makeValueMatcher(predicateFactory)
                          .inspectRuntimeShape(inspector);
          }
        };
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        cursorSupplier.get()
                      .getColumnSelectorFactory()
                      .makeDimensionSelector(dimensionSpec)
                      .inspectRuntimeShape(inspector);

      }

      @Nullable
      @Override
      public Object getObject()
      {
        return cursorSupplier.get()
                             .getColumnSelectorFactory()
                             .makeDimensionSelector(dimensionSpec)
                             .getObject();
      }

      @Override
      public Class<?> classOfObject()
      {
        return cursorSupplier.get()
                             .getColumnSelectorFactory()
                             .makeDimensionSelector(dimensionSpec)
                             .classOfObject();

      }

      @Override
      public int getValueCardinality()
      {
        return CARDINALITY_UNKNOWN;
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return cursorSupplier.get()
                             .getColumnSelectorFactory()
                             .makeDimensionSelector(dimensionSpec)
                             .lookupName(id);
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
        return cursorSupplier
            .get()
            .getColumnSelectorFactory()
            .makeColumnValueSelector(columnName)
            .getDouble();
      }

      @Override
      public float getFloat()
      {
        return cursorSupplier.get().getColumnSelectorFactory().makeColumnValueSelector(columnName).getFloat();
      }

      @Override
      public long getLong()
      {
        return cursorSupplier.get().getColumnSelectorFactory().makeColumnValueSelector(columnName).getLong();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        cursorSupplier.get()
                      .getColumnSelectorFactory()
                      .makeColumnValueSelector(columnName)
                      .inspectRuntimeShape(inspector);

      }

      @Override
      public boolean isNull()
      {
        return cursorSupplier.get().getColumnSelectorFactory().makeColumnValueSelector(columnName).isNull();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return cursorSupplier.get()
                             .getColumnSelectorFactory()
                             .makeColumnValueSelector(columnName)
                             .getObject();

      }

      @Override
      public Class classOfObject()
      {
        return cursorSupplier.get()
                             .getColumnSelectorFactory()
                             .makeColumnValueSelector(columnName)
                             .classOfObject();

      }
    };
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    // Assume only the type of the capabilities
    return rowSignature.getColumnCapabilities(column);
  }

  // Row Id cannot be unique
  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return null;
  }
}
