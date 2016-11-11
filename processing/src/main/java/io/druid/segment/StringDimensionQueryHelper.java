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
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.hash.Hasher;
import com.google.common.primitives.Ints;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.QueryDimensionInfo;
import io.druid.query.aggregation.cardinality.CardinalityAggregator;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.DruidPredicateFactory;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuerySpec;
import io.druid.query.topn.BaseTopNAlgorithm;
import io.druid.query.topn.TopNParams;
import io.druid.query.topn.TopNQuery;
import io.druid.segment.data.EmptyIndexedInts;
import io.druid.segment.data.IndexedInts;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.objects.Object2IntRBTreeMap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class StringDimensionQueryHelper implements DimensionQueryHelper<String, IndexedInts, DimensionSelector>
{
  private static final int GROUP_BY_MISSING_VALUE = -1;
  public static final String CARDINALITY_AGG_NULL_STRING = "\u0000";
  public static final char CARDINALITY_AGG_SEPARATOR = '\u0001';
  private final String dimensionName;

  private static Comparator<byte[]> GROUPING_KEY_COMPARATOR = new Comparator<byte[]>()
  {
    @Override
    public int compare(byte[] o1, byte[] o2)
    {
      int intLhs = Ints.fromByteArray(o1);
      int intRhs = Ints.fromByteArray(o2);
      return Ints.compare(intLhs, intRhs);
    }
  };

  public StringDimensionQueryHelper(String dimensionName) {
    this.dimensionName = dimensionName;
  }

  @Override
  public DimensionSelector getColumnValueSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory columnSelectorFactory)
  {
    return columnSelectorFactory.makeDimensionSelector(dimensionSpec);
  }

  @Override
  public int getRowSize(IndexedInts rowValues)
  {
    return rowValues.size();
  }

  @Override
  public int getCardinality(DimensionSelector valueSelector)
  {
    return valueSelector.getValueCardinality();
  }

  @Override
  public ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, final String value)
  {
    final DimensionSelector selector = cursor.makeDimensionSelector(
        new DefaultDimensionSpec(dimensionName, dimensionName)
    );

    // if matching against null, rows with size 0 should also match
    final boolean matchNull = Strings.isNullOrEmpty(value);

    final int cardinality = selector.getValueCardinality();

    if (cardinality >= 0) {
      // Dictionary-encoded dimension. Compare by id instead of by value to save time.
      final int valueId = selector.lookupId(value);

      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 0) {
            // null should match empty rows in multi-value columns
            return matchNull;
          } else {
            for (int i = 0; i < size; ++i) {
              if (row.get(i) == valueId) {
                return true;
              }
            }
            return false;
          }
        }
      };
    } else {
      // Not dictionary-encoded. Skip the optimization.
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 0) {
            // null should match empty rows in multi-value columns
            return matchNull;
          } else {
            for (int i = 0; i < size; ++i) {
              if (Objects.equals(selector.lookupName(row.get(i)), value)) {
                return true;
              }
            }
            return false;
          }
        }
      };
    }
  }

  @Override
  public ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, final DruidPredicateFactory predicateFactory)
  {
    final DimensionSelector selector = cursor.makeDimensionSelector(
        new DefaultDimensionSpec(dimensionName, dimensionName)
    );

    final Predicate<String> predicate = predicateFactory.makeStringPredicate();
    final int cardinality = selector.getValueCardinality();
    final boolean matchNull = predicate.apply(null);

    if (cardinality >= 0) {
      // Dictionary-encoded dimension. Check every value; build a bitset of matching ids.
      final BitSet valueIds = new BitSet(cardinality);
      for (int i = 0; i < cardinality; i++) {
        if (predicate.apply(selector.lookupName(i))) {
          valueIds.set(i);
        }
      }

      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 0) {
            // null should match empty rows in multi-value columns
            return matchNull;
          } else {
            for (int i = 0; i < size; ++i) {
              if (valueIds.get(row.get(i))) {
                return true;
              }
            }
            return false;
          }
        }
      };
    } else {
      // Not dictionary-encoded. Skip the optimization.
      return new ValueMatcher()
      {
        @Override
        public boolean matches()
        {
          final IndexedInts row = selector.getRow();
          final int size = row.size();
          if (size == 0) {
            // null should match empty rows in multi-value columns
            return matchNull;
          } else {
            for (int i = 0; i < size; ++i) {
              if (predicate.apply(selector.lookupName(row.get(i)))) {
                return true;
              }
            }
            return false;
          }
        }
      };
    }
  }

  @Override
  public void hashRow(DimensionSelector dimSelector, Hasher hasher)
  {
    final IndexedInts row = dimSelector.getRow();
    final int size = row.size();
    // nothing to add to hasher if size == 0, only handle size == 1 and size != 0 cases.
    if (size == 1) {
      final String value = dimSelector.lookupName(row.get(0));
      hasher.putUnencodedChars(convertValueForCardinalityAggregator(value));
    } else if (size != 0) {
      final String[] values = new String[size];
      for (int i = 0; i < size; ++i) {
        final String value = dimSelector.lookupName(row.get(i));
        values[i] = convertValueForCardinalityAggregator(value);
      }
      // Values need to be sorted to ensure consistent multi-value ordering across different segments
      Arrays.sort(values);
      for (int i = 0; i < size; ++i) {
        if (i != 0) {
          hasher.putChar(CARDINALITY_AGG_SEPARATOR);
        }
        hasher.putUnencodedChars(values[i]);
      }
    }
  }

  @Override
  public void hashValues(DimensionSelector dimSelector, HyperLogLogCollector collector)
  {
    for (IntIterator rowIt = dimSelector.getRow().iterator(); rowIt.hasNext(); ) {
      int index = rowIt.nextInt();
      final String value = dimSelector.lookupName(index);
      collector.add(CardinalityAggregator.hashFn.hashUnencodedChars(convertValueForCardinalityAggregator(value)).asBytes());
    }
  }

  @Override
  public int getGroupingKeySize()
  {
    return Ints.BYTES;
  }

  @Override
  public int compareGroupingKeys(ByteBuffer b1, int pos1, ByteBuffer b2, int pos2)
  {
    final int v1 = b1.getInt(pos1);
    final int v2 = b2.getInt(pos2);
    return Ints.compare(v1, v2);
  }

  @Override
  public Comparator<byte[]> getGroupingKeyByteComparator()
  {
    return GROUPING_KEY_COMPARATOR;
  }

  @Override
  public void processDimValueFromGroupingKey(
      String outputName, DimensionSelector dimSelector, ByteBuffer keyBuffer, Map<String, Object> theEvent
  )
  {
    final int dimVal = keyBuffer.getInt();
    if (dimVal != GROUP_BY_MISSING_VALUE) {
      theEvent.put(outputName, dimSelector.lookupName(dimVal));
    }
  }

  @Override
  public List<ByteBuffer> addDimValuesToGroupingKey(
      DimensionSelector selector, ByteBuffer key, Function<ByteBuffer, List<ByteBuffer>> updateValuesFn
  )
  {
    List<ByteBuffer> unaggregatedBuffers = null;
    final IndexedInts row = selector.getRow();
    if (row == null || row.size() == 0) {
      ByteBuffer newKey = key.duplicate();
      newKey.putInt(GROUP_BY_MISSING_VALUE);
      unaggregatedBuffers = updateValuesFn.apply(newKey);
    } else {
      for (IntIterator rowIt = row.iterator(); rowIt.hasNext(); ) {
        ByteBuffer newKey = key.duplicate();
        int dimValue = rowIt.nextInt();
        newKey.putInt(dimValue);
        unaggregatedBuffers = updateValuesFn.apply(newKey);
      }
    }
    return unaggregatedBuffers;
  }

  @Override
  public IndexedInts getRowFromDimSelector(DimensionSelector selector) {
    return selector == null ? EmptyIndexedInts.EMPTY_INDEXED_INTS : selector.getRow();
  }

  @Override
  public void initializeGroupingKeyV2Dimension(
      final IndexedInts values,
      final ByteBuffer keyBuffer,
      final int keyBufferPosition
  )
  {
    int rowSize = values.size();
    if (rowSize == 0) {
      keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
    } else {
      keyBuffer.putInt(keyBufferPosition, values.get(0));
    }
  }

  @Override
  public void addValueToGroupingKeyV2(
      final IndexedInts values,
      final int rowValueIdx,
      final ByteBuffer keyBuffer,
      final int keyBufferPosition
  )
  {
    keyBuffer.putInt(
        keyBufferPosition,
        values.get(rowValueIdx)
    );
  }

  @Override
  public void processValueFromGroupingKeyV2(QueryDimensionInfo dimInfo, ByteBuffer key, Map<String, Object> resultMap)
  {
    final int id = key.getInt(dimInfo.keyBufferPosition);

    // GROUP_BY_MISSING_VALUE is used to indicate empty rows, which are omitted from the result map.
    if (id != GROUP_BY_MISSING_VALUE) {
      resultMap.put(
          dimInfo.spec.getOutputName(),
          ((DimensionSelector) dimInfo.selector).lookupName(id)
      );
    }
  }

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
  public void dimExtractionScanAndAggregate(DimensionSelector selector, Aggregator[][] rowSelector, Map<Comparable, Aggregator[]> aggregatesStore, Cursor cursor, TopNQuery query)
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


  @Override
  public void addRowValuesToSelectResult(String outputName, DimensionSelector selector, Map<String, Object> theEvent)
  {
    if (selector == null) {
      theEvent.put(outputName, null);
    } else {
      final IndexedInts vals = selector.getRow();

      if (vals.size() == 1) {
        final String dimVal = selector.lookupName(vals.get(0));
        theEvent.put(outputName, dimVal);
      } else {
        List<String> dimVals = new ArrayList<>(vals.size());
        for (int i = 0; i < vals.size(); ++i) {
          dimVals.add(selector.lookupName(vals.get(i)));
        }
        theEvent.put(outputName, dimVals);
      }
    }
  }

  @Override
  public void updateSearchResultSet(
      String outputName,
      DimensionSelector selector,
      SearchQuerySpec searchQuerySpec,
      int limit,
      final Object2IntRBTreeMap<SearchHit> set
  )
  {
    if (selector != null) {
      final IndexedInts vals = selector.getRow();
      for (int i = 0; i < vals.size(); ++i) {
        final String dimVal = selector.lookupName(vals.get(i));
        if (searchQuerySpec.accept(dimVal)) {
          set.addTo(new SearchHit(outputName, dimVal), 1);
          if (set.size() >= limit) {
            return;
          }
        }
      }
    }
  }

  // CardinalityAggregator has a special representation for nulls
  private String convertValueForCardinalityAggregator(String value)
  {
    return value == null ? CARDINALITY_AGG_NULL_STRING : value;
  }

}
