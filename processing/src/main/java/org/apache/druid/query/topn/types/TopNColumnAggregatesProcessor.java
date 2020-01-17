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

package org.apache.druid.query.topn.types;

import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.dimension.ColumnSelectorStrategy;
import org.apache.druid.query.topn.HeapBasedTopNAlgorithm;
import org.apache.druid.query.topn.TopNParams;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNResultBuilder;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.StorageAdapter;

import javax.annotation.Nullable;

/**
 * This {@link ColumnSelectorStrategy} is used by all {@link org.apache.druid.query.topn.TopNAlgorithm} to provide
 * selector value cardinality to {@link TopNParams} (perhaps unecessarily, but that is another matter), but is primarily
 * used by {@link HeapBasedTopNAlgorithm} to serve as its value aggregates store.
 *
 * Given a query, column value selector, and cursor to process, the aggregates store is populated by calling
 * {@link #scanAndAggregate} and can be applied to {@link TopNResultBuilder} through {@link #updateResults}.
 */
public interface TopNColumnAggregatesProcessor<ValueSelectorType> extends ColumnSelectorStrategy
{
  /**
   * Get value cardinality of underlying {@link ColumnValueSelector}
   */
  int getCardinality(ValueSelectorType selector);

  /**
   * Used by {@link HeapBasedTopNAlgorithm}.
   *
   * Create an Aggregator[][] using {@link org.apache.druid.query.topn.BaseTopNAlgorithm.AggregatorArrayProvider} and
   * the given parameters.
   *
   * As the Aggregator[][] is used as an integer-based lookup, this method is only applicable for dimension types
   * that use integer row values, e.g. string columns where the value cardinality is known.
   *
   * A dimension type that does not have integer values should return null.
   *
   * @param query          The TopN query being served
   * @param params         Parameters for the TopN query being served
   * @param storageAdapter Column storage adapter, to provide information about the column that can be used for
   *                       query optimization, e.g. whether dimension values are sorted or not
   *
   * @return an Aggregator[][] for integer-valued dimensions, null otherwise
   */
  @Nullable
  Aggregator[][] getRowSelector(TopNQuery query, TopNParams params, StorageAdapter storageAdapter);

  /**
   * Used by {@link HeapBasedTopNAlgorithm}. The contract of this method requires calling {@link #initAggregateStore()}
   * prior to calling this method.
   *
   * Iterate through the {@link Cursor}, reading the current row from a dimension value selector, and for each row
   * value:
   *  1. Retrieve the Aggregator[] for the row value from rowSelector (fast integer lookup), usable if value cardinality
   *     is known, or from aggregatesStore (slower map).
   *
   *  2. If the rowSelector/aggregatesStore did not have an entry for a particular row value, this function
   *     should retrieve the current Aggregator[] using
   *     {@link org.apache.druid.query.topn.BaseTopNAlgorithm#makeAggregators} and the provided cursor and query,
   *     storing them in rowSelector/aggregatesStore
   *
   * 3. Call {@link Aggregator#aggregate()} on each of the aggregators.
   *
   * If a dimension type doesn't have integer values, it should ignore rowSelector and use the aggregatesStore map only.
   *
   * @param query           The TopN query being served.
   * @param selector        Dimension value selector
   * @param cursor          Cursor for the segment being queried
   * @param rowSelector     Integer lookup containing aggregators
   *
   * @return the number of processed rows (after postFilters are applied inside the cursor being processed)
   */
  long scanAndAggregate(
      TopNQuery query,
      ValueSelectorType selector,
      Cursor cursor,
      Aggregator[][] rowSelector
  );

  /**
   * Used by {@link HeapBasedTopNAlgorithm}.
   *
   * Read entries from the aggregates store, adding the keys and associated values to the resultBuilder, applying the
   * valueTransformer to the keys if present
   *
   * @param resultBuilder   TopN result builder
   */
  void updateResults(TopNResultBuilder resultBuilder);

  /**
   * Initializes the underlying aggregates store to something nice and seleector type appropriate
   */
  void initAggregateStore();

  /**
   * Closes all on heap {@link Aggregator} associated withe the aggregates processor
   */
  void closeAggregators();
}
