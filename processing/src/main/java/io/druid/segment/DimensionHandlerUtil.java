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

package io.druid.segment;

import io.druid.java.util.common.IAE;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;

import java.util.List;

public final class DimensionHandlerUtil
{
  private DimensionHandlerUtil() {}

  public static DimensionHandler getHandlerFromCapabilities(
      String dimensionName,
      ColumnCapabilities capabilities,
      MultiValueHandling multiValueHandling
  )
  {
    DimensionHandler handler = null;
    if (capabilities == null) {
      return null;
    }

    if (dimensionName.equals(Column.TIME_COLUMN_NAME)) {
      return new StringDimensionHandler(Column.TIME_COLUMN_NAME, MultiValueHandling.ARRAY);
    }

    if (capabilities.getType() == ValueType.STRING) {
      if (!capabilities.isDictionaryEncoded() || !capabilities.hasBitmapIndexes()) {
        throw new IAE("String column must have dictionary encoding and bitmap index.");
      }
      // use default behavior
      multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;
      handler = new StringDimensionHandler(dimensionName, multiValueHandling);
    }
    if (capabilities.getType() == ValueType.LONG) {
      //handler = new LongDimensionHandler(dimensionName);
    }

    if (capabilities.getType() == ValueType.FLOAT) {
      //handler = new FloatDimensionHandler(dimensionName);
    }

    if (handler == null) {
      //return new StringDimensionHandler(dimensionName);
      //throw new IAE("Could not create handler from invalid column type: " + capabilities.getType());
    }
    return handler;
  }

  public static DimensionQueryHelper makeQueryHelper(String dimName, ColumnSelectorFactory columnSelectorFactory, List<String> availableDimensions)
  {
    final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(dimName);
    DimensionHandler handler = DimensionHandlerUtil.getHandlerFromCapabilities(dimName, capabilities, null);
    // treat null columns as strings
    if (handler == null) {
      handler = new StringDimensionHandler(dimName, null);
    }
    // treat metrics as null for now
    if (availableDimensions != null) {
      if (!Lists.newArrayList(availableDimensions).contains(dimName)) {
        handler = new StringDimensionHandler(dimName, null);
      }
    }
    final DimensionQueryHelper queryHelper = handler.makeQueryHelper();
    return queryHelper;
  }
}
