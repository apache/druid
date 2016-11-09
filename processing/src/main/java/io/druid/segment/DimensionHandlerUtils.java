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
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;

import java.util.List;

public final class DimensionHandlerUtils
{
  private DimensionHandlerUtils() {}

  public static DimensionHandler getHandlerFromCapabilities(
      String dimensionName,
      ColumnCapabilities capabilities,
      MultiValueHandling multiValueHandling
  )
  {
    if (capabilities == null) {
      return new StringDimensionHandler(dimensionName, multiValueHandling);
    }

    if (dimensionName.equals(Column.TIME_COLUMN_NAME)) {
      return new StringDimensionHandler(Column.TIME_COLUMN_NAME, MultiValueHandling.ARRAY);
    }

    multiValueHandling = multiValueHandling == null ? MultiValueHandling.ofDefault() : multiValueHandling;

    if (capabilities.getType() == ValueType.STRING) {
      if (!capabilities.isDictionaryEncoded() || !capabilities.hasBitmapIndexes()) {
        throw new IAE("String column must have dictionary encoding and bitmap index.");
      }
      return new StringDimensionHandler(dimensionName, multiValueHandling);
    }

    // Return a StringDimensionHandler by default (null columns will be treated as String typed)
    return new StringDimensionHandler(dimensionName, multiValueHandling);
  }

  public static DimensionQueryHelper makeQueryHelper(String dimName, ColumnSelectorFactory columnSelectorFactory, List<String> availableDimensions)
  {
    final ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(dimName);
    DimensionHandler handler = DimensionHandlerUtils.getHandlerFromCapabilities(dimName, capabilities, null);

    // treat metrics as null for now
    if (availableDimensions != null) {
      if (availableDimensions.contains(dimName)) {
        handler = new StringDimensionHandler(dimName, null);
      }
    }
    return handler.makeQueryHelper();
  }
}
