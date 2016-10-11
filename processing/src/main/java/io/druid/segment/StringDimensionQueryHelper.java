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
import com.google.common.collect.Lists;
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
import org.apache.commons.lang.mutable.MutableInt;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public class StringDimensionQueryHelper implements DimensionQueryHelper<Integer, int[], String>
{
  private static final int GROUP_BY_MISSING_VALUE = -1;
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

  private static boolean isComparableNullOrEmpty(final Comparable value)
  {
    if (value instanceof String) {
      return Strings.isNullOrEmpty((String) value);
    }
    return value == null;
  }

  public StringDimensionQueryHelper(String dimensionName) {
    this.dimensionName = dimensionName;
  }

  @Override
  public Object getColumnValueSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory columnSelectorFactory)
  {
    return columnSelectorFactory.makeDimensionSelector(dimensionSpec);
  }

  @Override
  public int getRowSize(Object rowValues)
  {
    return ((IndexedInts) rowValues).size();
  }

  @Override
  public int getCardinality(Object valueSelector)
  {
    return ((DimensionSelector) valueSelector).getValueCardinality();
  }

  public ValueMatcher getValueMatcher(ColumnSelectorFactory cursor, final Comparable value)
  {
    final DimensionSelector selector = cursor.makeDimensionSelector(
        new DefaultDimensionSpec(dimensionName, dimensionName)
    );

    // if matching against null, rows with size 0 should also match
    final boolean matchNull = isComparableNullOrEmpty(value);

    final int cardinality = selector.getValueCardinality();

    if (cardinality >= 0) {
      // Dictionary-encoded dimension. Compare by id instead of by value to save time.
      final int valueId = selector.lookupId((String) value);

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

  public void hashRow(Object dimSelector, Hasher hasher)
  {
    final DimensionSelector selector = (DimensionSelector) dimSelector;
    final IndexedInts row = selector.getRow();
    final int size = row.size();
    // nothing to add to hasher if size == 0, only handle size == 1 and size != 0 cases.
    if (size == 1) {
      final String value = selector.lookupName(row.get(0));
      hasher.putUnencodedChars(value != null ? value : CardinalityAggregator.NULL_STRING);
    } else if (size != 0) {
      final String[] values = new String[size];
      for (int i = 0; i < size; ++i) {
        final String value = selector.lookupName(row.get(i));
        values[i] = value != null ? value : CardinalityAggregator.NULL_STRING;
      }
      // Values need to be sorted to ensure consistent multi-value ordering across different segments
      Arrays.sort(values);
      for (int i = 0; i < size; ++i) {
        if (i != 0) {
          hasher.putChar(CardinalityAggregator.SEPARATOR);
        }
        hasher.putUnencodedChars(values[i]);
      }
    }
  }

  public void hashValues(Object dimSelector, HyperLogLogCollector collector)
  {
    final DimensionSelector selector = (DimensionSelector) dimSelector;
    for (final Integer index : selector.getRow()) {
      final String value = selector.lookupName(index);
      collector.add(CardinalityAggregator.hashFn.hashUnencodedChars(value == null ? CardinalityAggregator.NULL_STRING : value).asBytes());
    }
  }

  @Override
  public int getGroupingKeySize()
  {
    return Ints.BYTES;
  }

  @Override
  public Comparator<byte[]> getGroupingKeyByteComparator()
  {
    return GROUPING_KEY_COMPARATOR;
  }

  @Override
  public void readDimValueFromGroupingKey(
      Map<String, Object> theEvent, String outputName, Object dimSelector, ByteBuffer keyBuffer
  )
  {
    final DimensionSelector selector = (DimensionSelector) dimSelector;
    final int dimVal = keyBuffer.getInt();
    if (dimVal != GROUP_BY_MISSING_VALUE) {
      theEvent.put(outputName, selector.lookupName(dimVal));
    }
  }

  @Override
  public List<ByteBuffer> addDimValuesToGroupingKey(
      Object selector, ByteBuffer key, Function<ByteBuffer, List<ByteBuffer>> updateValuesFn
  )
  {
    List<ByteBuffer> unaggregatedBuffers = null;
    final DimensionSelector dimSelector = (DimensionSelector) selector;
    final IndexedInts row = dimSelector.getRow();
    if (row == null || row.size() == 0) {
      ByteBuffer newKey = key.duplicate();
      newKey.putInt(GROUP_BY_MISSING_VALUE);
      unaggregatedBuffers = updateValuesFn.apply(newKey);
    } else {
      for (Integer dimValue : row) {
        ByteBuffer newKey = key.duplicate();
        newKey.putInt(dimValue);
        unaggregatedBuffers = updateValuesFn.apply(newKey);
      }
    }
    return unaggregatedBuffers;
  }

  @Override
  public Object getRowFromDimSelector(Object dimSelector) {
    final DimensionSelector selector = (DimensionSelector) dimSelector;
    IndexedInts values = selector == null ? EmptyIndexedInts.EMPTY_INDEXED_INTS : selector.getRow();
    return values;
  }

  @Override
  public int initializeGroupingKeyV2Dimension(
      final Object valuesObj,
      final ByteBuffer keyBuffer,
      final int keyBufferPosition
  )
  {
    IndexedInts values = (IndexedInts) valuesObj;
    final int rowSize = values.size();
    if (rowSize == 0) {
      keyBuffer.putInt(keyBufferPosition, GROUP_BY_MISSING_VALUE);
    } else {
      keyBuffer.putInt(keyBufferPosition, values.get(0));
    }
    return rowSize;
  }

  @Override
  public void addValueToGroupingKeyV2(
      final Object values,
      final int rowValueIdx,
      final ByteBuffer keyBuffer,
      final int keyBufferPosition
  )
  {
    IndexedInts intValues = (IndexedInts) values;
    keyBuffer.putInt(
        keyBufferPosition,
        intValues.get(rowValueIdx)
    );
  }

  @Override
  public void readValueFromGroupingKeyV2(QueryDimensionInfo dimInfo, Map<String, Object> resultMap, ByteBuffer key)
  {
    final int id = key.getInt(dimInfo.keyBufferPosition);

    if (id >= 0) {
      resultMap.put(
          dimInfo.spec.getOutputName(),
          ((DimensionSelector) dimInfo.selector).lookupName(id)
      );
    }
  }

  @Override
  public Aggregator[][] getDimExtractionRowSelector(TopNParams params, TopNQuery query, Capabilities capabilities)
  {
    final BaseTopNAlgorithm.AggregatorArrayProvider provider = new BaseTopNAlgorithm.AggregatorArrayProvider(
        (DimensionSelector) params.getDimSelector(),
        query,
        params.getCardinality(),
        capabilities
    );

    // Unlike regular topN we cannot rely on ordering to optimize.
    // Optimization possibly requires a reverse lookup from value to ID, which is
    // not possible when applying an extraction function
    return provider.build();
  }

  @Override
  public void dimExtractionScanAndAggregate(Object selector, Aggregator[][] rowSelector, Map<Comparable, Aggregator[]> aggregatesStore, Cursor cursor, TopNQuery query)
  {
    final DimensionSelector dimSelector = (DimensionSelector) selector;
    final IndexedInts dimValues = dimSelector.getRow();

    for (int i = 0; i < dimValues.size(); ++i) {
      final int dimIndex = dimValues.get(i);
      Aggregator[] theAggregators = rowSelector[dimIndex];
      if (theAggregators == null) {
        final String key = dimSelector.lookupName(dimIndex);
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
  public void addRowValuesToSelectResult(String outputName, Object dimSelector, Map<String, Object> theEvent)
  {
    final DimensionSelector selector = (DimensionSelector) dimSelector;
    if (selector == null) {
      theEvent.put(outputName, null);
    } else {
      final IndexedInts vals = selector.getRow();

      if (vals.size() == 1) {
        final String dimVal = selector.lookupName(vals.get(0));
        theEvent.put(outputName, dimVal);
      } else {
        List<String> dimVals = Lists.newArrayList();
        for (int i = 0; i < vals.size(); ++i) {
          dimVals.add(selector.lookupName(vals.get(i)));
        }
        theEvent.put(outputName, dimVals);
      }
    }
  }

  @Override
  public void updateSearchResultSet(String outputName, Object dimSelector, SearchQuerySpec searchQuerySpec, TreeMap<SearchHit, MutableInt> set, int limit)
  {
    final DimensionSelector selector = (DimensionSelector) dimSelector;

    if (selector != null) {
      final IndexedInts vals = selector.getRow();
      for (int i = 0; i < vals.size(); ++i) {
        final String dimVal = selector.lookupName(vals.get(i));
        if (searchQuerySpec.accept(dimVal)) {
          MutableInt counter = new MutableInt(1);
          MutableInt prev = set.put(new SearchHit(outputName, dimVal), counter);
          if (prev != null) {
            counter.add(prev.intValue());
          }
          if (set.size() >= limit) {
            return;
          }
        }
      }
    }
  }
}
