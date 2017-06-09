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

package io.druid.query.topn.types;

import com.google.common.base.Function;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.dimension.ColumnSelectorStrategy;
import io.druid.query.topn.TopNParams;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNResultBuilder;
import io.druid.segment.Capabilities;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.column.ValueType;

import javax.annotation.Nullable;
import java.util.Map;

public interface TopNColumnSelectorStrategy
    <ValueSelectorType extends ColumnValueSelector, DimExtractionAggregateStoreType extends Map>
    extends ColumnSelectorStrategy
{
  int CARDINALITY_UNKNOWN = -1;

  int getCardinality(ValueSelectorType selector);

  ValueType getValueType();

  /**
   * Used by DimExtractionTopNAlgorithm.
   *
   * Create an Aggregator[][] using BaseTopNAlgorithm.AggregatorArrayProvider and the given parameters.
   *
   * As the Aggregator[][] is used as an integer-based lookup, this method is only applicable for dimension types
   * that use integer row values.
   *
   * A dimension type that does not have integer values should return null.
   *
   * @param query The TopN query being served
   * @param params Parameters for the TopN query being served
   * @param capabilities Object indicating if dimension values are sorted
   * @return an Aggregator[][] for integer-valued dimensions, null otherwise
   */
  Aggregator[][] getDimExtractionRowSelector(TopNQuery query, TopNParams params, Capabilities capabilities);

  /**
   * Used by DimExtractionTopNAlgorithm.
   *
   * Creates an aggregate store map suitable for this strategy's type that will be
   * passed to dimExtractionScanAndAggregate() and updateDimExtractionResults().
   *
   * @return Aggregate store map
   */
  DimExtractionAggregateStoreType makeDimExtractionAggregateStore();

  /**
   * Used by DimExtractionTopNAlgorithm.
   *
   * Iterate through the cursor, reading the current row from a dimension value selector, and for each row value:
   * 1. Retrieve the Aggregator[] for the row value from rowSelector (fast integer lookup) or from
   *    aggregatesStore (slower map).
   *
   * 2. If the rowSelector and/or aggregatesStore did not have an entry for a particular row value,
   *    this function should retrieve the current Aggregator[] using BaseTopNAlgorithm.makeAggregators() and the
   *    provided cursor and query, storing them in rowSelector and aggregatesStore
   *
   * 3. Call aggregate() on each of the aggregators.
   *
   * If a dimension type doesn't have integer values, it should ignore rowSelector and use the aggregatesStore map only.
   *
   * @param query The TopN query being served.
   * @param selector Dimension value selector
   * @param cursor Cursor for the segment being queried
   * @param rowSelector Integer lookup containing aggregators
   * @param aggregatesStore Map containing aggregators
   * @return the number of processed rows (after postFilters are applied inside the cursor being processed)
   */
  long dimExtractionScanAndAggregate(
      final TopNQuery query,
      ValueSelectorType selector,
      Cursor cursor,
      Aggregator[][] rowSelector,
      DimExtractionAggregateStoreType aggregatesStore
  );

  /**
   * Used by DimExtractionTopNAlgorithm.
   *
   * Read entries from the aggregates store, adding the keys and associated values to the resultBuilder, applying the
   * valueTransformer to the keys if present
   *
   * @param aggregatesStore Map created by makeDimExtractionAggregateStore()
   * @param valueTransformer Converts keys to different types, if null no conversion is needed
   * @param resultBuilder TopN result builder
   */
  void updateDimExtractionResults(
      DimExtractionAggregateStoreType aggregatesStore,
      @Nullable Function<Object, Object> valueTransformer,
      TopNResultBuilder resultBuilder
  );
}
