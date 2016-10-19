/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.groupby.epinephelinae;

import io.druid.data.input.Row;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.NumericColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;

public class TestColumnSelectorFactory implements ColumnSelectorFactory
{
  private ThreadLocal<Row> row = new ThreadLocal<>();

  public void setRow(Row row)
  {
    this.row.set(row);
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(final String columnName)
  {
    return new FloatColumnSelector()
    {
      @Override
      public float get()
      {
        return row.get().getFloatMetric(columnName);
      }
    };
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(final String columnName)
  {
    return new LongColumnSelector()
    {
      @Override
      public long get()
      {
        return row.get().getLongMetric(columnName);
      }
    };
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(final String columnName)
  {
    return new ObjectColumnSelector()
    {
      @Override
      public Class classOfObject()
      {
        return Object.class;
      }

      @Override
      public Object get()
      {
        return row.get().getRaw(columnName);
      }
    };
  }

  @Override
  public NumericColumnSelector makeMathExpressionSelector(String expression)
  {
    throw new UnsupportedOperationException("expression is not supported in current context");
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return null;
  }
}
