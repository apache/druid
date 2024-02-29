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
 * A column selector factory, that represents the column values from multiple underlying cursors. It essentially
 * wraps over the multiple cursors and can be passed to methods that expect column value selector from a single cursor.
 * It is the duty of the caller to know when to switch from one cursor to another
 *
 * It is expected to work correctly if the individual cursors have the same corresponding columns, columnTypes,
 * and column capabilities since the usual pattern throughout the code is to cache these values and
 * assume they are fixed throughout.
 */
@NotThreadSafe
public class SettableCursorColumnSelectorFactory implements ColumnSelectorFactory
{
  /**
   * Cursor supplier whose supplied value will determine what cursor and the values to expose to the caller
   */
  private final Supplier<Cursor> cursorSupplier;

  /**
   * Overarching row signature of the values returned by the cursor
   */
  private final RowSignature rowSignature;

  public SettableCursorColumnSelectorFactory(final Supplier<Cursor> cursorSupplier, final RowSignature rowSignature)
  {
    this.cursorSupplier = cursorSupplier;
    this.rowSignature = rowSignature;
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

      /**
       * Cardinality of the concatenation of these selectors would be unknown
       */
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

      /**
       * Name lookup is not possible in advance, because not all may support name lookups in the first place and they
       * will have their own name lookups that will conflict with one another
       */
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

  /**
   * Create {@link ColumnValueSelector} for the give column name
   */
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

  /**
   * Return column capabilities from the signature. This returns the capabilities with the minimal assumptions.
   * For example, one cursor can have capabilities with multivalues set to FALSE, while other can have it set to TRUE
   * Creating the capabilites from the signature would leave the state undefined. If the caller has more knowledge
   * about all the cursors that will get created, the caller can subclass and override the function with that information
   */
  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    // Assume only the type of the capabilities
    return rowSignature.getColumnCapabilities(column);
  }

  /**
   * Since we don't know all the cursors beforehand, we can't reconcile their row id suppliers to provide
   * one here
   */
  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return null;
  }
}
