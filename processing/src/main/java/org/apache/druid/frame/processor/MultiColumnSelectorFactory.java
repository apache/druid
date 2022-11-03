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

package org.apache.druid.frame.processor;

import com.google.common.base.Predicate;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.function.Supplier;

/**
 * A {@link ColumnSelectorFactory} that wraps multiple {@link ColumnSelectorFactory} and delegates to one of
 * them at any given time. The identity of the delegated-to factory is changed by calling {@link #setCurrentFactory}.
 */
public class MultiColumnSelectorFactory implements ColumnSelectorFactory
{
  private final List<Supplier<ColumnSelectorFactory>> factorySuppliers;
  private final ColumnInspector columnInspector;

  private int currentFactory = 0;

  public MultiColumnSelectorFactory(
      final List<Supplier<ColumnSelectorFactory>> factorySuppliers,
      final ColumnInspector columnInspector
  )
  {
    this.factorySuppliers = factorySuppliers;
    this.columnInspector = columnInspector;
  }

  public void setCurrentFactory(final int currentFactory)
  {
    this.currentFactory = currentFactory;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return new DimensionSelector()
    {
      private final ColumnSelectorFactory[] delegateFactories = new ColumnSelectorFactory[factorySuppliers.size()];
      private final DimensionSelector[] delegateSelectors = new DimensionSelector[factorySuppliers.size()];

      @Override
      public IndexedInts getRow()
      {
        return populateDelegate().getRow();
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
      }

      @Override
      public ValueMatcher makeValueMatcher(Predicate<String> predicate)
      {
        return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return populateDelegate().getObject();
      }

      @Override
      public Class<?> classOfObject()
      {
        return populateDelegate().classOfObject();
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
        return populateDelegate().lookupName(id);
      }

      @Nullable
      @Override
      public ByteBuffer lookupNameUtf8(int id)
      {
        return populateDelegate().lookupNameUtf8(id);
      }

      @Override
      public boolean supportsLookupNameUtf8()
      {
        return populateDelegate().supportsLookupNameUtf8();
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

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Do nothing.
      }

      private DimensionSelector populateDelegate()
      {
        final ColumnSelectorFactory factory = factorySuppliers.get(currentFactory).get();

        //noinspection ObjectEquality: checking reference equality intentionally
        if (factory != delegateFactories[currentFactory]) {
          delegateSelectors[currentFactory] = factory.makeDimensionSelector(dimensionSpec);
          delegateFactories[currentFactory] = factory;
        }

        return delegateSelectors[currentFactory];
      }
    };
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(final String columnName)
  {

    return new ColumnValueSelector()
    {
      private final ColumnSelectorFactory[] delegateFactories = new ColumnSelectorFactory[factorySuppliers.size()];

      @SuppressWarnings("rawtypes")
      private final ColumnValueSelector[] delegateSelectors = new ColumnValueSelector[factorySuppliers.size()];

      @Override
      public double getDouble()
      {
        return populateDelegate().getDouble();
      }

      @Override
      public float getFloat()
      {
        return populateDelegate().getFloat();
      }

      @Override
      public long getLong()
      {
        return populateDelegate().getLong();
      }

      @Override
      public boolean isNull()
      {
        return populateDelegate().isNull();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return populateDelegate().getObject();
      }

      @Override
      public Class classOfObject()
      {
        // Assumes all delegate factories have the same class of object.
        return populateDelegate().classOfObject();
      }

      private ColumnValueSelector<?> populateDelegate()
      {
        final ColumnSelectorFactory factory = factorySuppliers.get(currentFactory).get();

        //noinspection ObjectEquality: checking reference equality intentionally
        if (factory != delegateFactories[currentFactory]) {
          delegateSelectors[currentFactory] = factory.makeColumnValueSelector(columnName);
          delegateFactories[currentFactory] = factory;
        }

        return delegateSelectors[currentFactory];
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Do nothing.
      }
    };
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return columnInspector.getColumnCapabilities(column);
  }
}
