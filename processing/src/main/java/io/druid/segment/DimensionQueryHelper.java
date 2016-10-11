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

import com.google.common.base.Function;
import com.google.common.hash.Hasher;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.QueryDimensionInfo;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.topn.TopNParams;
import io.druid.query.topn.TopNQuery;
import org.apache.commons.lang.mutable.MutableInt;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public interface DimensionQueryHelper<EncodedType extends Comparable<EncodedType>, EncodedTypeArray, ActualType extends Comparable<ActualType>>
{
  /**
   * Get a typed column value selector (DimensionSelector, LongColumnSelector, etc.) from a ColumnSelectorFactory.
   * @param dimensionSpec The dimension of the selector
   * @param columnSelectorFactory Column value selector provider
   * @return Column value selector for the dimension specified by dimensionSpec.
   */
  public Object getColumnValueSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory columnSelectorFactory);


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
  public int getRowSize(Object rowValues);


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
  public int getCardinality(Object valueSelector);


  /** Functions for QueryableIndexStorageAdapter, FilteredAggregatorFactory **/
  /**
   * Create a single value ValueMatcher, used for filtering by QueryableIndexStorageAdapter and FilteredAggregatorFactory.
   *
   * @param cursor ColumnSelectorFactory for creating dimension value selectors
   * @param value Value to match against
   * @return ValueMatcher that matches on 'value'
   */
  public ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, Comparable value);


  /**
   * Create a predicate-based ValueMatcher, used for filtering by QueryableIndexStorageAdapter and FilteredAggregatorFactory.
   *
   * @param cursor ColumnSelectorFactory for creating dimension value selectors
   * @param predicateFactory A DruidPredicateFactory that provides the filter predicates to be matched
   * @return A ValueMatcher that applies the predicate for this DimensionQueryHelper's value type from the predicateFactory
   */
  public ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, final DruidPredicateFactory predicateFactory);


  /**
   * Used by CardinalityAggregator.
   *
   * Retrieve the current row from dimSelector and add the row values to the hasher.
   *
   * @param dimSelector Dimension value selector
   * @param hasher Hasher used for cardinality aggregator calculations
   */
  public void hashRow(Object dimSelector, Hasher hasher);


  /**
   * Used by CardinalityAggregator.
   *
   * Retrieve the current row from dimSelector and add the row values to the hasher.
   * @param dimSelector Dimension value selector
   * @param collector HLL collector used for cardinality aggregator calculations
   */
  public void hashValues(Object dimSelector, HyperLogLogCollector collector);


  /**
   * Used by GroupByEngine.
   *
   * Return the size, in bytes, of this dimension's values in the grouping key.
   *
   * For example, a String implementation would return 4, the size of an int.
   *
   * @return size, in bytes, of this dimension's values in the grouping key.
   */
  public int getGroupingKeySize();


  /**
   * Used by GroupByEngine.
   *
   * A grouping key contains a concatenation of byte[] representations of dimension values.
   *
   * When comparing two grouping keys, the individual dimension values will be compared with comparators
   * provided by the query helper.
   *
   * @return A comparator suitable for comparing byte representations of this dimension's type of values.
   */
  public Comparator<byte[]> getGroupingKeyByteComparator();


  /**
   * Used by GroupByEngine.
   *
   * Perform a relative read on a grouping key ByteBuffer to retrieve a single dimension value, and
   * add the retrieved value to a GroupBy result map.
   *
   * An implementation may choose to not add anything to the result map
   * (e.g., as the String implementation does for empty rows)
   *
   * @param theEvent Result map for the GroupBy query being served
   * @param outputName The output name of this dimension for the GroupBy query being served, as specified in the DimensionSpec
   * @param dimSelector Dimension value selector, used for value lookups if needed
   * @param keyBuffer Grouping key, already positioned at this dimension's offset
   */
  public void readDimValueFromGroupingKey(
      Map<String, Object> theEvent,
      String outputName,
      Object dimSelector,
      ByteBuffer keyBuffer
  );


  /**
   * Used by GroupByEngine.
   *
   * Read the current row from a dimension value selector and add the row values to the grouping key.
   *
   * This is called by GroupByEngine's updateValues() function, which uses recursion to traverse the dimensions in the grouping set.
   *
   * Before adding a dimension value to the grouping key, this function should duplicate() the provided key buffer and
   * add the value to the duplicate key.
   *
   * After adding a dimension value to the duplicate grouping key, an implementation of this function should call
   * updateValuesFn on the new key to perform the recursion.
   *
   * For multi-value rows, this function should duplicate the original grouping key before adding each value, and
   * call updateValuesFn on each new key.
   *
   * See StringDimensionQueryHelper for a reference implementation.
   *
   * @param dimSelector Dimension value selector
   * @param key ByteBuffer for the grouping key
   * @param updateValuesFn Function provided by GroupByEngine for updateValues() recursion
   * @return Return the result of calling updateValuesFn on the updated grouping key
   */
  public List<ByteBuffer> addDimValuesToGroupingKey(
      Object dimSelector,
      ByteBuffer key,
      Function<ByteBuffer, List<ByteBuffer>> updateValuesFn
  );


  /**
   * Retrieve the current row from a dimension value selector.
   *
   * @param dimSelector Dimension value selector
   * @return Current row
   */
  public Object getRowFromDimSelector(Object dimSelector);


  /**
   * Used by GroupByEngineV2.
   *
   * Read the first value within a row values object (IndexedInts, IndexedLongs, etc.) and add that value
   * to the keyBuffer at keyBufferPosition, and return the size of the row values object.
   *
   * @param valuesObj row values object
   * @param keyBuffer grouping key
   * @param keyBufferPosition offset within grouping key
   * @return size of the row values object
   */
  public int initializeGroupingKeyV2Dimension(
      final Object valuesObj,
      final ByteBuffer keyBuffer,
      final int keyBufferPosition
  );


  /**
   * Used by GroupByEngineV2.
   *
   * Read the value at rowValueIdx from a row values object and add that value to the keyBuffer at keyBufferPosition.
   *
   * @param values row values object
   * @param rowValueIdx index of the value to read
   * @param keyBuffer grouping key
   * @param keyBufferPosition offset within grouping key
   */
  public void addValueToGroupingKeyV2(
      Object values,
      int rowValueIdx,
      ByteBuffer keyBuffer,
      final int keyBufferPosition
  );


  /**
   * Used by GroupByEngineV2.
   *
   * Read a value from a grouping key and add it to the group by query result map, using the output name specified
   * in a DimensionSpec.
   *
   * An implementation may choose to not add anything to the result map
   * (e.g., as the String implementation does for empty rows)
   *
   * dimInfo provides access to:
   * - the keyBufferPosition offset from which to read the value
   * - the dimension value selector
   * - the DimensionSpec for this dimension from the query
   *
   * @param dimInfo dimension info containing the key offset, value selector, and dimension spec
   * @param resultMap result map for the group by query being served
   * @param key grouping key
   */
  public void readValueFromGroupingKeyV2(
      QueryDimensionInfo dimInfo,
      Map<String, Object> resultMap,
      ByteBuffer key
  );


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
   * @param params Parameters for the TopN query being served
   * @param query The TopN query being served
   * @param capabilities Object indicating if dimension values are sorted
   * @return an Aggregator[][] for integer-valued dimensions, null otherwise
   */
  public Aggregator[][] getDimExtractionRowSelector(TopNParams params, TopNQuery query, Capabilities capabilities);


  /**
   * Used by DimExtractionTopNAlgorithm.
   *
   * Read the current row from a dimension value selector, and for each row value:
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
   * @param selector Dimension value selector
   * @param rowSelector Integer lookup containing aggregators
   * @param aggregatesStore Map containing aggregators
   * @param cursor Cursor for the segment being queried
   * @param query The TopN query being served.
   */
  public void dimExtractionScanAndAggregate(
      Object selector,
      Aggregator[][] rowSelector,
      Map<Comparable, Aggregator[]> aggregatesStore,
      Cursor cursor,
      TopNQuery query
  );


  /**
   * Used by the select query.
   *
   * Read the current row from dimSelector and add the row values to the result map.
   *
   * Multi-valued rows should be added to the result as a List, single value rows should be added as a single object.
   *
   * @param outputName Output name for this dimension in the select query being served
   * @param dimSelector Dimension value selector
   * @param resultMap Output map of the select query being served
   */
  public void addRowValuesToSelectResult(
      String outputName,
      Object dimSelector,
      Map<String, Object> resultMap
  );


  /**
   * Used by the search query.
   *
   * Read the current row from dimSelector and update the search result set.
   *
   * For each row value:
   * 1. Check if searchQuerySpec accept()s the value
   * 2. If so, add the value to the result set and increment the counter for that value
   * 3. If the size of the result set reaches the limit after adding a value, return early.
   *
   * @param outputName Output name for this dimension in the search query being served
   * @param dimSelector Dimension value selector
   * @param searchQuerySpec Spec for the search query
   * @param set The result set of the search query
   * @param limit The limit of the search query
   */
  public void updateSearchResultSet(
      String outputName,
      Object dimSelector,
      SearchQuerySpec searchQuerySpec,
      TreeMap<SearchHit, MutableInt> set,
      int limit
  );
}
