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
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.dimension.ColumnSelectorStrategyFactory;
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

  /**
   * Creates an array of ColumnSelectorPlus objects, selectors that handle type-specific operations within
   * query processing engines, using a strategy factory provided by the query engine. One ColumnSelectorPlus
   * will be created for each column specified in dimensionSpecs.
   *
   * The ColumnSelectorPlus provides access to a type strategy (e.g., how to group on a float column)
   * and a value selector for a single column.
   *
   * A caller should define a strategy factory that provides an interface for type-specific operations
   * in a query engine. See GroupByStrategyFactory for a reference.
   *
   * @param <ColumnSelectorStrategyClass> The strategy type created by the provided strategy factory.
   * @param strategyFactory A factory provided by query engines that generates type-handling strategies
   * @param dimensionSpecs The set of columns to generate ColumnSelectorPlus objects for
   * @param cursor Used to create value selectors for columns.
   * @return An array of ColumnSelectorPlus objects, in the order of the columns specified in dimensionSpecs
   */
  public static <ColumnSelectorStrategyClass extends ColumnSelectorStrategy> ColumnSelectorPlus<ColumnSelectorStrategyClass>[] createColumnSelectorPluses(
      ColumnSelectorStrategyFactory<ColumnSelectorStrategyClass> strategyFactory,
      List<DimensionSpec> dimensionSpecs,
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
          cursor.getColumnCapabilities(dimSpec.getDimension())
      );
      final ColumnValueSelector selector = getColumnValueSelectorFromDimensionSpec(
          dimSpec,
          cursor
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
  private static ColumnCapabilities getEffectiveCapabilities(
      String dimName,
      ColumnCapabilities capabilities
  )
  {
    if (capabilities == null) {
      capabilities = DEFAULT_STRING_CAPABILITIES;
    }

    // non-Strings aren't actually supported yet
    if (capabilities.getType() != ValueType.STRING) {
      capabilities = DEFAULT_STRING_CAPABILITIES;
    }

    return capabilities;
  }

  private static ColumnValueSelector getColumnValueSelectorFromDimensionSpec(
      DimensionSpec dimSpec,
      ColumnSelectorFactory columnSelectorFactory
  )
  {
    String dimName = dimSpec.getDimension();
    ColumnCapabilities capabilities = columnSelectorFactory.getColumnCapabilities(dimName);
    capabilities = getEffectiveCapabilities(dimName, capabilities);
    switch (capabilities.getType()) {
      case STRING:
        return columnSelectorFactory.makeDimensionSelector(dimSpec);
      default:
        return null;
    }
  }

  private static <ColumnSelectorStrategyClass extends ColumnSelectorStrategy> ColumnSelectorStrategyClass makeStrategy(
      ColumnSelectorStrategyFactory<ColumnSelectorStrategyClass> strategyFactory,
      String dimName,
      ColumnCapabilities capabilities
  )
  {
    capabilities = getEffectiveCapabilities(dimName, capabilities);
    return strategyFactory.makeColumnSelectorStrategy(capabilities);
  }
}
