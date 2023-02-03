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

package org.apache.druid.segment;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestColumnSelectorFactory implements ColumnSelectorFactory
{
  private final Map<String, DimensionSelector> dimSelectors = new LinkedHashMap<>();
  private final Map<String, ColumnValueSelector<?>> columnSelectors = new LinkedHashMap<>();
  private final Map<String, ColumnCapabilities> capabilitiesMap = new LinkedHashMap<>();

  public TestColumnSelectorFactory addDimSelector(String name, @Nullable DimensionSelector selector)
  {
    dimSelectors.put(name, selector);
    return this;
  }

  public <T> TestColumnSelectorFactory addColumnSelector(String name, @Nullable ColumnValueSelector<T> selector)
  {
    columnSelectors.put(name, selector);
    return this;
  }

  public TestColumnSelectorFactory addCapabilities(String name, @Nullable ColumnCapabilities capabilities)
  {
    capabilitiesMap.put(name, capabilities);
    return this;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return getFromMap(dimSelectors, dimensionSpec.getDimension(), "dimension");
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    return getFromMap(columnSelectors, columnName, "column");
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return getFromMap(capabilitiesMap, column, "capability");
  }

  private <T> T getFromMap(Map<String, T> map, String key, String name)
  {
    if (!map.containsKey(key)) {
      throw new UOE("%s[%s] wasn't registered, but was asked for, register first (null is okay)", name, key);
    }
    return map.get(key);
  }
}
