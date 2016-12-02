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

import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.ValueMatcher;

/**
 * Query related interface.
 *
 * Contains a collection of query processing methods for functionality that is dependent on
 * the type of a dimension.
 *
 * The methods within this interface are general methods that are not tied to a specific query type.
 *
 * Each DimensionQueryHelper is associated with a single dimension.
 *
 * @param <RowValuesType> The type of the row values object for this dimension
 * @param <ValueSelectorType> The type of the row value selector (e.g. DimensionSelector) for this dimension
 */
public interface DimensionQueryHelper<RowValuesType, ValueSelectorType extends ColumnValueSelector>
{
  /**
   * Get a typed column value selector (DimensionSelector, LongColumnSelector, etc.) from a ColumnSelectorFactory.
   * @param dimensionSpec The dimension of the selector
   * @param columnSelectorFactory Column value selector provider
   * @return Column value selector for the dimension specified by dimensionSpec.
   */
  ValueSelectorType getColumnValueSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory columnSelectorFactory);

  /**
   * Retrieve the current row from a dimension value selector.
   *
   * @param dimSelector Dimension value selector
   * @return Current row
   */
  RowValuesType getRowFromDimSelector(ValueSelectorType dimSelector);

  /**
   * Get the size of a row object.
   *
   * The type of the row object will depend on the dimension type, e.g.:
   *
   * String type -> IndexedInts row object
   * Long type -> IndexedLongs row object
   *
   * @param rowValues The row object to return the size of
   * @return size of the row object
   */
  int getRowSize(RowValuesType rowValues);


  /**
   * Get the cardinality, if possible, from a dimension value selector object.
   *
   * The class of the row object will depend on the dimension type, e.g:
   *
   * String type -> DimensionSelector
   * Long type -> LongColumnSelector
   *
   * @param valueSelector The dimension value selector object
   * @return Cardinality of the dimension value selector object, -1 if cardinality is not available.
   */
  int getCardinality(ValueSelectorType valueSelector);


  // Functions for QueryableIndexStorageAdapter, FilteredAggregatorFactory
  /**
   * Create a single value ValueMatcher, used for filtering by QueryableIndexStorageAdapter and FilteredAggregatorFactory.
   *
   * @param cursor ColumnSelectorFactory for creating dimension value selectors
   * @param value Value to match against
   * @return ValueMatcher that matches on 'value'
   */
  ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, String value);


  /**
   * Create a predicate-based ValueMatcher, used for filtering by QueryableIndexStorageAdapter and FilteredAggregatorFactory.
   *
   * @param cursor ColumnSelectorFactory for creating dimension value selectors
   * @param predicateFactory A DruidPredicateFactory that provides the filter predicates to be matched
   * @return A ValueMatcher that applies the predicate for this DimensionQueryHelper's value type from the predicateFactory
   */
  ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, final DruidPredicateFactory predicateFactory);
}
