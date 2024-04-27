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

package org.apache.druid.segment.vector;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestVectorColumnSelectorFactory implements VectorColumnSelectorFactory
{
  private ReadableVectorInspector inspector = null;

  private final Map<String, SingleValueDimensionVectorSelector> singleValDimSelectors = new LinkedHashMap<>();
  private final Map<String, MultiValueDimensionVectorSelector> multiValDimSelectors = new LinkedHashMap<>();
  private final Map<String, VectorValueSelector> vectorValueSelectors = new LinkedHashMap<>();
  private final Map<String, VectorObjectSelector> vectorObjectSelectors = new LinkedHashMap<>();
  private final Map<String, ColumnCapabilities> capabilitiesMap = new LinkedHashMap<>();

  public TestVectorColumnSelectorFactory addSVDVS(String col, SingleValueDimensionVectorSelector selector)
  {
    singleValDimSelectors.put(col, selector);
    return this;
  }

  public TestVectorColumnSelectorFactory addMVDVS(String col, MultiValueDimensionVectorSelector selector)
  {
    multiValDimSelectors.put(col, selector);
    return this;
  }

  public TestVectorColumnSelectorFactory addVVS(String col, VectorValueSelector selector)
  {
    vectorValueSelectors.put(col, selector);
    return this;
  }

  public TestVectorColumnSelectorFactory addVOS(String col, VectorObjectSelector selector)
  {
    vectorObjectSelectors.put(col, selector);
    return this;
  }

  public TestVectorColumnSelectorFactory addCapabilities(String col, ColumnCapabilities capabilities)
  {
    capabilitiesMap.put(col, capabilities);
    return this;
  }

  @Override
  public ReadableVectorInspector getReadableVectorInspector()
  {
    return inspector;
  }

  @Override
  public SingleValueDimensionVectorSelector makeSingleValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    return getFromMap(singleValDimSelectors, dimensionSpec.getDimension(), "dimension");
  }

  @Override
  public MultiValueDimensionVectorSelector makeMultiValueDimensionSelector(DimensionSpec dimensionSpec)
  {
    return getFromMap(multiValDimSelectors, dimensionSpec.getDimension(), "dimension");
  }

  @Override
  public VectorValueSelector makeValueSelector(String column)
  {
    return getFromMap(vectorValueSelectors, column, "column");
  }

  @Override
  public VectorObjectSelector makeObjectSelector(String column)
  {
    return getFromMap(vectorObjectSelectors, column, "column");
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
