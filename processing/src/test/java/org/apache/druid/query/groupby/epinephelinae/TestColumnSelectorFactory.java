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

package org.apache.druid.query.groupby.epinephelinae;

import org.apache.druid.data.input.Row;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;

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
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    return new ColumnValueSelector<Object>()
    {
      @Override
      public double getDouble()
      {
        return row.get().getMetric(columnName).doubleValue();
      }

      @Override
      public float getFloat()
      {
        return row.get().getMetric(columnName).floatValue();
      }

      @Override
      public long getLong()
      {
        return row.get().getMetric(columnName).longValue();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        return row.get().getRaw(columnName);
      }

      @Override
      public Class<Object> classOfObject()
      {
        return Object.class;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // don't inspect in tests
      }

      @Override
      public boolean isNull()
      {
        return row.get().getMetric(columnName) == null;
      }


    };
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    return null;
  }
}
