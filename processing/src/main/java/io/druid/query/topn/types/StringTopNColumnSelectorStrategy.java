package io.druid.query.topn.types;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.topn.BaseTopNAlgorithm;
import io.druid.query.topn.TopNParams;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.Capabilities;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.data.IndexedInts;

import java.util.Map;

public class StringTopNColumnSelectorStrategy implements TopNColumnSelectorStrategy<DimensionSelector>
{
  @Override
  public int getCardinality(DimensionSelector selector)
  {
    return selector.getValueCardinality();
  }

  @Override
  public Aggregator[][] getDimExtractionRowSelector(TopNQuery query, TopNParams params, Capabilities capabilities)
  {
    // This method is used for the DimExtractionTopNAlgorithm only.
    // Unlike regular topN we cannot rely on ordering to optimize.
    // Optimization possibly requires a reverse lookup from value to ID, which is
    // not possible when applying an extraction function

    final BaseTopNAlgorithm.AggregatorArrayProvider provider = new BaseTopNAlgorithm.AggregatorArrayProvider(
        (DimensionSelector) params.getSelectorPlus().getSelector(),
        query,
        params.getCardinality(),
        capabilities
    );

    return provider.build();
  }

  @Override
  public void dimExtractionScanAndAggregate(
      final TopNQuery query,
      DimensionSelector selector,
      Cursor cursor,
      Aggregator[][] rowSelector,
      Map<Comparable, Aggregator[]> aggregatesStore
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
