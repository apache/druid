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

package org.apache.druid.segment.virtual;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.StringUtf8DictionaryEncodedColumn;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ReadableOffset;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

/**
 * A String column like VirtualColumn to test drive VirtualColumn interface.
 */
public class DummyStringVirtualColumn implements VirtualColumn
{
  private final String baseColumnName;
  private final String outputName;

  private final boolean enableRowBasedMethods;
  private final boolean enableColumnBasedMethods;
  private final boolean enableBitmaps;
  private final boolean disableValueMatchers;

  public DummyStringVirtualColumn(
      String baseColumnName,
      String outputName,
      boolean enableRowBasedMethods,
      boolean enableColumnBasedMethods,
      boolean enableBitmaps,
      boolean disableValueMatchers
  )
  {
    this.baseColumnName = baseColumnName;
    this.outputName = outputName;
    this.enableRowBasedMethods = enableRowBasedMethods;
    this.enableColumnBasedMethods = enableColumnBasedMethods;
    this.enableBitmaps = enableBitmaps;
    this.disableValueMatchers = disableValueMatchers;
  }

  @Override
  public String getOutputName()
  {
    return this.outputName;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    if (enableColumnBasedMethods) {
      ColumnHolder holder = columnSelector.getColumnHolder(baseColumnName);
      if (holder == null) {
        return DimensionSelector.constant(null);
      }

      StringUtf8DictionaryEncodedColumn stringCol = toStringDictionaryEncodedColumn(holder.getColumn());

      DimensionSelector baseDimensionSelector = stringCol.makeDimensionSelector(
          offset,
          dimensionSpec.getExtractionFn()
      );
      if (disableValueMatchers) {
        baseDimensionSelector = disableValueMatchers(baseDimensionSelector);
      }
      return dimensionSpec.decorate(baseDimensionSelector);
    } else {
      return null;
    }
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory factory
  )
  {
    if (enableRowBasedMethods) {
      DimensionSelector baseDimensionSelector = factory.makeDimensionSelector(new DefaultDimensionSpec(
          baseColumnName,
          baseColumnName,
          null
      ));

      if (disableValueMatchers) {
        baseDimensionSelector = disableValueMatchers(baseDimensionSelector);
      }
      return dimensionSpec.decorate(baseDimensionSelector);
    } else {
      throw new UnsupportedOperationException("not supported");
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    if (enableColumnBasedMethods) {
      ColumnHolder holder = columnSelector.getColumnHolder(baseColumnName);
      if (holder == null) {
        return NilColumnValueSelector.instance();
      }

      StringUtf8DictionaryEncodedColumn stringCol = toStringDictionaryEncodedColumn(holder.getColumn());
      return stringCol.makeColumnValueSelector(offset);
    } else {
      return null;
    }
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory factory
  )
  {
    if (enableRowBasedMethods) {
      return factory.makeColumnValueSelector(baseColumnName);
    } else {
      throw new UnsupportedOperationException("not supported");
    }
  }

  @Nullable
  @Override
  public ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnIndexSelector indexSelector
  )
  {
    return new ColumnIndexSupplier()
    {

      @Nullable
      @Override
      public <T> T as(Class<T> clazz)
      {
        if (enableBitmaps) {
          ColumnIndexSupplier supplier = indexSelector.getIndexSupplier(baseColumnName);
          if (supplier == null) {
            return null;
          }
          return supplier.as(clazz);
        } else {
          return null;
        }
      }
    };
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    ColumnCapabilitiesImpl capabilities = new ColumnCapabilitiesImpl().setType(ColumnType.STRING)
                                                                      .setDictionaryEncoded(true);
    if (enableBitmaps) {
      capabilities.setHasBitmapIndexes(true);
    }
    return capabilities;
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.singletonList(baseColumnName);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new byte[0];
  }

  private StringUtf8DictionaryEncodedColumn toStringDictionaryEncodedColumn(BaseColumn column)
  {
    if (!(column instanceof StringUtf8DictionaryEncodedColumn)) {
      throw new IAE("I can only work with StringDictionaryEncodedColumn");
    }

    return (StringUtf8DictionaryEncodedColumn) column;
  }

  private DimensionSelector disableValueMatchers(DimensionSelector base)
  {
    return new DimensionSelector()
    {
      @Override
      public IndexedInts getRow()
      {
        return base.getRow();
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        throw new UnsupportedOperationException("not supported");
      }

      @Override
      public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
      {
        throw new UnsupportedOperationException("not supported");
      }

      @Override
      public int getValueCardinality()
      {
        return base.getValueCardinality();
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return base.lookupName(id);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return base.nameLookupPossibleInAdvance();
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return base.idLookup();
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        base.inspectRuntimeShape(inspector);
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return base.getObject();
      }

      @Override
      public Class<?> classOfObject()
      {
        return base.classOfObject();
      }
    };
  }
}
