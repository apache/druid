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

package io.druid.query.topn;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import io.druid.query.Result;
import io.druid.query.QueryDimensionInfo;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.dimension.QueryTypeHelper;
import io.druid.query.dimension.QueryTypeHelperFactory;
import io.druid.segment.Capabilities;
import io.druid.segment.ColumnValueSelector;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.DimensionSelector;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;

import java.util.Map;

public class TopNMapFn implements Function<Cursor, Result<TopNResultValue>>
{
  private static final TopNTypeHelperFactory TYPE_HELPER_FACTORY = new TopNTypeHelperFactory();

  private static class TopNTypeHelperFactory implements QueryTypeHelperFactory<TopNTypeHelper>
  {
    @Override
    public TopNTypeHelper makeQueryTypeHelper(
        String dimName, ColumnCapabilities capabilities
    )
    {
      ValueType type = capabilities.getType();
      switch(type) {
        case STRING:
          return new StringTopNTypeHelper();
        default:
          return null;
      }
    }
  }

  public interface TopNTypeHelper<ValueSelectorType extends ColumnValueSelector> extends QueryTypeHelper
  {
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
    Aggregator[][] getDimExtractionRowSelector(TopNParams params, TopNQuery query, Capabilities capabilities);


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
    void dimExtractionScanAndAggregate(
        ValueSelectorType selector,
        Aggregator[][] rowSelector,
        Map<Comparable, Aggregator[]> aggregatesStore,
        Cursor cursor,
        TopNQuery query
    );
  }

  public static class StringTopNTypeHelper implements TopNTypeHelper<DimensionSelector>
  {
    @Override
    public Aggregator[][] getDimExtractionRowSelector(TopNParams params, TopNQuery query, Capabilities capabilities)
    {
      // This method is used for the DimExtractionTopNAlgorithm only.
      // Unlike regular topN we cannot rely on ordering to optimize.
      // Optimization possibly requires a reverse lookup from value to ID, which is
      // not possible when applying an extraction function

      final BaseTopNAlgorithm.AggregatorArrayProvider provider = new BaseTopNAlgorithm.AggregatorArrayProvider(
          (DimensionSelector) params.getDimSelector(),
          query,
          params.getCardinality(),
          capabilities
      );

      return provider.build();
    }

    @Override
    public void dimExtractionScanAndAggregate(
        DimensionSelector selector,
        Aggregator[][] rowSelector,
        Map<Comparable, Aggregator[]> aggregatesStore,
        Cursor cursor,
        TopNQuery query
    )
    {
      final IndexedInts dimValues = selector.getRow();

      for (int i = 0; i < dimValues.size(); ++i) {
        final int dimIndex = dimValues.get(i);
        Aggregator[] theAggregators = rowSelector[dimIndex];
        if (theAggregators == null) {
          final String key = selector.lookupName(dimIndex);
          theAggregators = aggregatesStore.get(key);
          if (theAggregators == null) {
            theAggregators = BaseTopNAlgorithm.makeAggregators(cursor, query.getAggregatorSpecs());
            aggregatesStore.put(key, theAggregators);
          }
          rowSelector[dimIndex] = theAggregators;
        }

        for (Aggregator aggregator : theAggregators) {
          aggregator.aggregate();
        }
      }
    }
  }


  private final TopNQuery query;
  private final TopNAlgorithm topNAlgorithm;

  public TopNMapFn(
      TopNQuery query,
      TopNAlgorithm topNAlgorithm
  )
  {
    this.query = query;
    this.topNAlgorithm = topNAlgorithm;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Result<TopNResultValue> apply(Cursor cursor)
  {
    final QueryDimensionInfo[] dimInfoArray = DimensionHandlerUtils.getDimensionInfo(
        TYPE_HELPER_FACTORY,
        Lists.newArrayList(query.getDimensionSpec()),
        null,
        cursor
    );

    if (dimInfoArray[0].getSelector() == null) {
      return null;
    }

    TopNParams params = null;
    try {
      params = topNAlgorithm.makeInitParams(dimInfoArray[0], cursor);

      TopNResultBuilder resultBuilder = BaseTopNAlgorithm.makeResultBuilder(params, query);

      topNAlgorithm.run(params, resultBuilder, null);

      return resultBuilder.build();
    }
    finally {
      topNAlgorithm.cleanup(params);
    }
  }
}
