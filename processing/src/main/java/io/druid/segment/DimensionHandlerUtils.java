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

import com.google.common.collect.Lists;
import io.druid.java.util.common.IAE;
import io.druid.data.input.impl.DimensionSchema.MultiValueHandling;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;

import java.util.List;

public final class DimensionHandlerUtils
{
  private DimensionHandlerUtils() {}

  public final static ColumnCapabilities DEFAULT_STRING_CAPABILITIES =
      new ColumnCapabilitiesImpl().setType(ValueType.STRING)
                                  .setDictionaryEncoded(true)
                                  .setHasBitmapIndexes(true);

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

  public static <ColumnSelectorStrategyClass extends ColumnSelectorStrategy> ColumnSelectorStrategyClass makeStrategy(
      ColumnSelectorStrategyFactory<ColumnSelectorStrategyClass> strategyFactory,
      String dimName,
      ColumnCapabilities capabilities,
      List<String> availableDimensions
  )
  {
    capabilities = getEffectiveCapabilities(dimName, capabilities, availableDimensions);
    return strategyFactory.makeColumnSelectorStrategy(dimName, capabilities);
  }

  public static <ColumnSelectorStrategyClass extends ColumnSelectorStrategy> ColumnSelectorPlus<ColumnSelectorStrategyClass>[] getDimensionInfo(
      ColumnSelectorStrategyFactory<ColumnSelectorStrategyClass> strategyFactory,
      List<DimensionSpec> dimensionSpecs,
      StorageAdapter adapter,
      ColumnSelectorFactory cursor
  )
  {
    int dimCount = dimensionSpecs.size();
    ColumnSelectorPlus<ColumnSelectorStrategyClass>[] dims = new ColumnSelectorPlus[dimCount];
    for (int i = 0; i < dimCount; i++) {
      final DimensionSpec dimSpec = dimensionSpecs.get(i);
      final String dimName = dimSpec.getDimension();
      ColumnSelectorStrategyClass strategy = makeStrategy(
          strategyFactory,
          dimName,
          cursor.getColumnCapabilities(dimSpec.getDimension()),
          adapter == null ? null : Lists.newArrayList(adapter.getAvailableDimensions())
      );
      final ColumnValueSelector selector = getColumnValueSelectorFromDimensionSpec(
          dimSpec,
          cursor,
          adapter == null ? null : Lists.newArrayList(adapter.getAvailableDimensions())
      );
      final ColumnSelectorPlus<ColumnSelectorStrategyClass> selectorPlus = new ColumnSelectorPlus<>(
          dimName,
          dimSpec.getOutputName(),
          strategy,
          selector
      );
      dims[i] = selectorPlus;
    }
    return dims;
  }

  // When determining the capabilites of a column during query processing, this function
  // adjusts the capabilities for columns that cannot be handled as-is to manageable defaults
  // (e.g., treating missing columns as empty String columns)
  public static ColumnCapabilities getEffectiveCapabilities(
      String dimName,
      ColumnCapabilities capabilities,
      List<String> availableDimensions
  )
  {
    if (capabilities == null) {
      capabilities = DEFAULT_STRING_CAPABILITIES;
    }

    // treat metrics as null for now
    if (availableDimensions != null) {
      if (!availableDimensions.contains(dimName)) {
        capabilities = DEFAULT_STRING_CAPABILITIES;
      }
    }

    // non-Strings aren't actually supported yet
    if (capabilities.getType() != ValueType.STRING) {
      capabilities = DEFAULT_STRING_CAPABILITIES;
    }

    return capabilities;
  }

  public static ColumnValueSelector getColumnValueSelectorFromDimensionSpec(
      DimensionSpec dimSpec,
      ColumnSelectorFactory columnSelectorFactory,
      List<String> availableDimensions
  )
  {
    String dimName = dimSpec.getOutputName();
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(dimName);
    capabilities = getEffectiveCapabilities(dimName, capabilities, availableDimensions);
    switch (capabilities.getType()) {
      case STRING:
        return columnSelectorFactory.makeDimensionSelector(dimSpec);
      default:
        return null;
    }
  }
}
