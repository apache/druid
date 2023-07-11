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

package org.apache.druid.frame.util;

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnType;

import java.util.Collections;
import java.util.List;

/**
 * Virtual column that returns a changeable value, via {@link #setValue(long)}. Useful for injecting synthetic values
 * into a cursor, such as row numbers.
 */
public class SettableLongVirtualColumn implements VirtualColumn
{
  private final String columnName;
  private long theValue = 0;

  public SettableLongVirtualColumn(final String columnName)
  {
    this.columnName = columnName;
  }

  @Override
  public String getOutputName()
  {
    return columnName;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
  {
    return new LongColumnSelector()
    {
      @Override
      public long getLong()
      {
        return theValue;
      }

      @Override
      public boolean isNull()
      {
        return false;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        // Do nothing.
      }
    };
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG).setHasNulls(false);
  }

  @Override
  public List<String> requiredColumns()
  {
    return Collections.emptyList();
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    throw new UnsupportedOperationException();
  }

  public long getValue()
  {
    return theValue;
  }

  public void setValue(long theValue)
  {
    this.theValue = theValue;
  }
}
