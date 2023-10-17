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

package org.apache.druid.segment.join.table;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.netty.util.SuppressForbidden;
import it.unimi.dsi.fastutil.ints.Int2ObjectLinkedOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntAVLTreeSet;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSortedSet;
import it.unimi.dsi.fastutil.ints.IntSortedSets;
import org.apache.druid.collections.RangeIntSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.SimpleAscendingOffset;
import org.apache.druid.segment.SimpleDescendingOffset;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public class IndexedTableJoinMatcher implements JoinMatcher
{
  static final int NO_CONDITION_MATCH = -1;
  private static final int UNINITIALIZED_CURRENT_ROW = -1;

  // Key column type to use when the actual key type is unknown.
  static final ColumnType DEFAULT_KEY_TYPE = ColumnType.STRING;

  private final IndexedTable table;
  private final List<ConditionMatcher> conditionMatchers;
  private final boolean singleRowMatching;
  private final ColumnSelectorFactory selectorFactory;

  // matchedRows and matchingRemainder are used to implement matchRemainder().
  private final IntSet matchedRows;
  private boolean matchingRemainder = false;

  // currentIterator and currentRow are used to track iteration position through the currently-matched-rows.
  @Nullable
  private IntIterator currentIterator;
  private int currentRow;
  private final SimpleSettableOffset joinableOffset;

  IndexedTableJoinMatcher(
      final IndexedTable table,
      final ColumnSelectorFactory leftSelectorFactory,
      final JoinConditionAnalysis condition,
      final boolean remainderNeeded,
      final boolean descending,
      final Closer closer
  )
  {
    this.table = table;
    if (descending) {
      this.joinableOffset = new SimpleDescendingOffset(table.numRows());
    } else {
      this.joinableOffset = new SimpleAscendingOffset(table.numRows());
    }
    reset();

    if (condition.isAlwaysTrue()) {
      this.conditionMatchers = Collections.singletonList(() -> new RangeIntSet(0, table.numRows()));
      this.singleRowMatching = false;
    } else if (condition.isAlwaysFalse()) {
      this.conditionMatchers = Collections.singletonList(() -> IntSortedSets.EMPTY_SET);
      this.singleRowMatching = false;
    } else if (condition.getNonEquiConditions().isEmpty()) {
      final List<Pair<IndexedTable.Index, Equality>> indexes =
          condition.getEquiConditions()
                   .stream()
                   .map(eq -> Pair.of(getIndex(table, eq), eq))
                   .collect(Collectors.toCollection(ArrayList::new));

      this.conditionMatchers =
          indexes.stream()
                 .map(pair -> makeConditionMatcher(pair.lhs, leftSelectorFactory, pair.rhs))
                 .collect(Collectors.toList());

      this.singleRowMatching = indexes.stream().allMatch(pair -> pair.lhs.areKeysUnique(pair.rhs.isIncludeNull()));
    } else {
      throw new IAE(
          "Cannot build hash-join matcher on non-equi-join condition: %s",
          condition.getOriginalExpression()
      );
    }

    ColumnSelectorFactory selectorFactory = table.makeColumnSelectorFactory(joinableOffset, descending, closer);
    this.selectorFactory = selectorFactory != null
                           ? selectorFactory
                           : new IndexedTableColumnSelectorFactory(table, () -> currentRow, closer);

    if (remainderNeeded) {
      this.matchedRows = new IntRBTreeSet();
    } else {
      this.matchedRows = null;
    }

  }

  private static IndexedTable.Index getIndex(
      final IndexedTable table,
      final Equality condition
  )
  {
    if (!table.keyColumns().contains(condition.getRightColumn())) {
      throw new IAE("Cannot build hash-join matcher on non-key-based condition: %s", condition);
    }

    final int keyColumnNumber = table.rowSignature().indexOf(condition.getRightColumn());

    return table.columnIndex(keyColumnNumber);
  }

  private static ConditionMatcher makeConditionMatcher(
      final IndexedTable.Index index,
      final ColumnSelectorFactory selectorFactory,
      final Equality condition
  )
  {
    return ColumnProcessors.makeProcessor(
        condition.getLeftExpr(),
        index.keyType(),
        new ConditionMatcherFactory(index, condition.isIncludeNull()),
        selectorFactory
    );
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return selectorFactory;
  }

  @Override
  public void matchCondition()
  {
    reset();

    if (singleRowMatching) {
      if (conditionMatchers.size() == 1) {
        currentRow = conditionMatchers.get(0).matchSingleRow();
      } else {
        currentRow = conditionMatchers.get(0).matchSingleRow();

        for (int i = 1; i < conditionMatchers.size(); i++) {
          if (currentRow != conditionMatchers.get(i).matchSingleRow()) {
            currentRow = UNINITIALIZED_CURRENT_ROW;
            break;
          }
        }
      }
    } else {
      if (conditionMatchers.size() == 1) {
        currentIterator = conditionMatchers.get(0).match().iterator();
      } else {
        final IntSortedSet[] matchingSets = new IntSortedSet[conditionMatchers.size()];
        int smallestMatchingSet = -1;

        for (int i = 0; i < conditionMatchers.size(); i++) {
          matchingSets[i] = conditionMatchers.get(i).match();
          if (i == 0 || matchingSets[i].size() < matchingSets[smallestMatchingSet].size()) {
            smallestMatchingSet = i;
          }
        }

        // Start intersection using the smallest matching set.
        IntSortedSet intersection = matchingSets[smallestMatchingSet];

        // Remember if we copied matchingSets[smallestMatchingSet] or not. Avoids unnecessary copies.
        boolean copied = false;

        for (int i = 0; i < conditionMatchers.size(); i++) {
          final int numIntersectionElements = intersection.size();

          if (numIntersectionElements > 0 && i != smallestMatchingSet) {
            final IntBidirectionalIterator it = intersection.iterator();
            while (it.hasNext()) {
              final int rowNumber = it.nextInt();
              if (!matchingSets[i].contains(rowNumber)) {
                // Remove from intersection.
                if (numIntersectionElements == 1) {
                  intersection = IntSortedSets.EMPTY_SET;
                  break;
                } else {
                  if (!copied) {
                    intersection = new IntAVLTreeSet(intersection);
                    copied = true;
                  }

                  intersection.remove(rowNumber);
                }
              }
            }
          }
        }

        currentIterator = intersection.iterator();
      }

      advanceCurrentRow();
    }

    addCurrentRowToMatchedRows();
  }

  @Override
  public void matchRemainder()
  {
    Preconditions.checkState(matchedRows != null, "matchedRows != null");

    currentIterator = new IntIterator()
    {
      int current = -1;

      {
        advanceRemainderIterator();
      }

      @Override
      public int nextInt()
      {
        if (current >= table.numRows()) {
          throw new NoSuchElementException();
        }

        final int retVal = current;
        advanceRemainderIterator();
        return retVal;
      }

      @Override
      public boolean hasNext()
      {
        return current < table.numRows();
      }

      private void advanceRemainderIterator()
      {
        do {
          current++;
        } while (current < table.numRows() && matchedRows.contains(current));
      }
    };

    matchingRemainder = true;
    advanceCurrentRow();
  }

  @Override
  public boolean matchingRemainder()
  {
    return matchingRemainder;
  }

  @Override
  public boolean hasMatch()
  {
    return currentRow >= 0;
  }

  @Override
  public void nextMatch()
  {
    advanceCurrentRow();
    addCurrentRowToMatchedRows();
  }

  @Override
  public void reset()
  {
    // Do not reset matchedRows; we want to remember it across reset() calls so the 'remainder' is anything
    // that was unmatched across _all_ cursor walks.
    currentIterator = null;
    currentRow = UNINITIALIZED_CURRENT_ROW;
    matchingRemainder = false;
    joinableOffset.reset();
  }

  private void advanceCurrentRow()
  {
    if (currentIterator != null && currentIterator.hasNext()) {
      currentRow = currentIterator.nextInt();
    } else {
      currentIterator = null;
      currentRow = UNINITIALIZED_CURRENT_ROW;
      joinableOffset.setCurrentOffset(currentRow);
    }
  }

  private void addCurrentRowToMatchedRows()
  {
    if (!matchingRemainder && matchedRows != null && hasMatch()) {
      matchedRows.add(currentRow);
    }
  }

  interface ConditionMatcher
  {
    /**
     * Returns the first row that matches the current cursor position, or {@link #NO_CONDITION_MATCH} if nothing
     * matches.
     */
    default int matchSingleRow()
    {
      final IntSortedSet rows = match();
      return rows.isEmpty() ? NO_CONDITION_MATCH : rows.firstInt();
    }

    /**
     * Returns row numbers that match the current cursor position.
     */
    IntSortedSet match();
  }

  /**
   * Makes suppliers that returns the list of IndexedTable rows that match the values from selectors.
   */
  @VisibleForTesting
  static class ConditionMatcherFactory implements ColumnProcessorFactory<ConditionMatcher>
  {
    @VisibleForTesting
    static final int CACHE_MAX_SIZE = 1000;

    private static final int MAX_NUM_CACHE = 10;

    private final ColumnType keyType;
    private final IndexedTable.Index index;
    private final boolean includeNull;

    // DimensionSelector -> (int) dimension id -> (IntSortedSet) row numbers
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")  // updated via computeIfAbsent
    private final LruLoadingHashMap<DimensionSelector, Int2IntSortedSetMap> dimensionCaches;

    ConditionMatcherFactory(IndexedTable.Index index, boolean includeNull)
    {
      this.keyType = index.keyType();
      this.index = index;
      this.includeNull = includeNull;

      this.dimensionCaches = new LruLoadingHashMap<>(
          MAX_NUM_CACHE,
          selector -> {
            int cardinality = selector.getValueCardinality();
            IntFunction<IntSortedSet> loader = dimensionId -> getRowNumbers(selector.lookupName(dimensionId));
            return cardinality <= CACHE_MAX_SIZE
                   ? new Int2IntSortedSetLookupTable(cardinality, loader)
                   : new Int2IntSortedSetLruCache(CACHE_MAX_SIZE, loader);
          }
      );
    }

    private IntSortedSet getRowNumbers(@Nullable String key)
    {
      if (includeNull || !NullHandling.isNullOrEquivalent(key)) {
        return index.find(key);
      } else {
        return IntSortedSets.EMPTY_SET;
      }
    }

    private IntSortedSet getAndCacheRowNumbers(DimensionSelector selector, int dimensionId)
    {
      return dimensionCaches.getAndLoadIfAbsent(selector).getAndLoadIfAbsent(dimensionId);
    }

    @Override
    public ColumnType defaultType()
    {
      return keyType;
    }

    @Override
    public ConditionMatcher makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
    {
      // NOTE: The slow (cardinality unknown) and fast (cardinality known) code paths below only differ in the calls to
      // getRowNumbers() and getAndCacheRowNumbers(), respectively. The majority of the code path is duplicated to avoid
      // adding indirection to fetch getRowNumbers()/getAndCacheRowNumbers() from a local BiFunction variable that is
      // set outside of the supplier. Minimizing overhead is desirable since the supplier is called from a hot loop for
      // joins.

      if (selector.getValueCardinality() == DimensionDictionarySelector.CARDINALITY_UNKNOWN) {
        // If the cardinality is unknown, then the selector does not have a "real" dictionary and the dimension id
        // is not valid outside the context of a specific row. This means we cannot use a cache and must fall
        // back to this slow code path.
        return () -> {
          final IndexedInts row = selector.getRow();

          if (row.size() == 1) {
            int dimensionId = row.get(0);
            return getRowNumbers(selector.lookupName(dimensionId));
          } else if (row.size() == 0) {
            return getRowNumbers(null);
          } else {
            // Multi-valued rows are not handled by the join system right now
            // TODO: Remove when https://github.com/apache/druid/issues/9924 is done
            throw new QueryUnsupportedException("Joining against a multi-value dimension is not supported.");
          }
        };
      } else {
        // If the cardinality is known, then the dimension id is still valid outside the context of a specific row and
        // its mapping to row numbers can be cached.
        return () -> {
          final IndexedInts row = selector.getRow();

          if (row.size() == 1) {
            int dimensionId = row.get(0);
            return getAndCacheRowNumbers(selector, dimensionId);
          } else if (row.size() == 0) {
            return getRowNumbers(null);
          } else {
            // Multi-valued rows are not handled by the join system right now
            // TODO: Remove when https://github.com/apache/druid/issues/9924 is done
            throw new QueryUnsupportedException("Joining against a multi-value dimension is not supported.");
          }
        };
      }
    }

    @Override
    public ConditionMatcher makeFloatProcessor(BaseFloatColumnValueSelector selector)
    {
      if (NullHandling.replaceWithDefault()) {
        return () -> index.find(selector.getFloat());
      } else if (includeNull) {
        return () -> selector.isNull() ? index.find(null) : index.find(selector.getFloat());
      } else {
        return () -> selector.isNull() ? IntSortedSets.EMPTY_SET : index.find(selector.getFloat());
      }
    }

    @Override
    public ConditionMatcher makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
    {
      if (NullHandling.replaceWithDefault()) {
        return () -> index.find(selector.getDouble());
      } else if (includeNull) {
        return () -> selector.isNull() ? index.find(null) : index.find(selector.getDouble());
      } else {
        return () -> selector.isNull() ? IntSortedSets.EMPTY_SET : index.find(selector.getDouble());
      }
    }

    @Override
    public ConditionMatcher makeLongProcessor(BaseLongColumnValueSelector selector)
    {
      if (index.keyType().is(ValueType.LONG)) {
        return makePrimitiveLongMatcher(selector);
      } else if (NullHandling.replaceWithDefault()) {
        return () -> index.find(selector.getLong());
      } else if (includeNull) {
        return () -> selector.isNull() ? index.find(null) : index.find(selector.getLong());
      } else {
        return () -> selector.isNull() ? IntSortedSets.EMPTY_SET : index.find(selector.getLong());
      }
    }

    @Override
    public ConditionMatcher makeArrayProcessor(
        BaseObjectColumnValueSelector<?> selector,
        @Nullable ColumnCapabilities columnCapabilities
    )
    {
      return () -> {
        throw new QueryUnsupportedException("Joining against ARRAY columns is not supported.");
      };
    }

    @Override
    public ConditionMatcher makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
    {
      return new ConditionMatcher()
      {
        @Override
        public int matchSingleRow()
        {
          return NO_CONDITION_MATCH;
        }

        @Override
        public IntSortedSet match()
        {
          return IntSortedSets.EMPTY_SET;
        }
      };
    }

    /**
     * Makes matchers for LONG-typed selectors against LONG-typed indexes. Specialized to use findUniqueLong
     * when appropriate.
     */
    private ConditionMatcher makePrimitiveLongMatcher(BaseLongColumnValueSelector selector)
    {
      if (NullHandling.replaceWithDefault()) {
        return new ConditionMatcher()
        {
          @Override
          public int matchSingleRow()
          {
            return index.findUniqueLong(selector.getLong());
          }

          @Override
          public IntSortedSet match()
          {
            return index.find(selector.getLong());
          }
        };
      } else if (includeNull) {
        return new ConditionMatcher()
        {
          @Override
          public int matchSingleRow()
          {
            if (selector.isNull()) {
              final IntSortedSet rowNumbers = index.find(null);

              return rowNumbers == null ? NO_CONDITION_MATCH : rowNumbers.firstInt();
            } else {
              return index.findUniqueLong(selector.getLong());
            }
          }

          @Override
          public IntSortedSet match()
          {
            return selector.isNull() ? index.find(null) : index.find(selector.getLong());
          }
        };
      } else {
        return new ConditionMatcher()
        {
          @Override
          public int matchSingleRow()
          {
            return selector.isNull() ? NO_CONDITION_MATCH : index.findUniqueLong(selector.getLong());
          }

          @Override
          public IntSortedSet match()
          {
            return selector.isNull() ? IntSortedSets.EMPTY_SET : index.find(selector.getLong());
          }
        };
      }
    }
  }

  @VisibleForTesting
  static class LruLoadingHashMap<K, V> extends LinkedHashMap<K, V>
  {
    private final int maxSize;
    private final Function<K, V> loader;

    // Allowing use of LinkedHashMap constructor because implementation uses capacity() to properly size HashMap.
    @SuppressForbidden(reason = "java.util.LinkedHashMap#<init>(int)")
    LruLoadingHashMap(int maxSize, Function<K, V> loader)
    {
      super(capacity(maxSize));
      this.maxSize = maxSize;
      this.loader = loader;
    }

    V getAndLoadIfAbsent(K key)
    {
      return computeIfAbsent(key, loader);
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry eldest)
    {
      return size() > maxSize;
    }

    private static int capacity(int expectedSize)
    {
      // This is the calculation used in JDK8 to resize when a putAll happens; it seems to be the most conservative
      // calculation we can make. 0.75 is the default load factor.
      return (int) ((float) expectedSize / 0.75F + 1.0F);
    }
  }

  private interface Int2IntSortedSetMap
  {
    IntSortedSet getAndLoadIfAbsent(int key);
  }

  /**
   * Lookup table for keys in the range from 0 to maxSize - 1
   */
  @VisibleForTesting
  static class Int2IntSortedSetLookupTable implements Int2IntSortedSetMap
  {
    private final IntSortedSet[] lookup;
    private final IntFunction<IntSortedSet> loader;

    Int2IntSortedSetLookupTable(int maxSize, IntFunction<IntSortedSet> loader)
    {
      this.loader = loader;
      this.lookup = new IntSortedSet[maxSize];
    }

    @Override
    public IntSortedSet getAndLoadIfAbsent(int key)
    {
      IntSortedSet value = lookup[key];

      if (value == null) {
        value = loader.apply(key);
        lookup[key] = value;
      }

      return value;
    }
  }

  /**
   * LRU cache optimized for primitive int keys
   */
  @VisibleForTesting
  static class Int2IntSortedSetLruCache implements Int2IntSortedSetMap
  {
    private final Int2ObjectLinkedOpenHashMap<IntSortedSet> cache;
    private final int maxSize;
    private final IntFunction<IntSortedSet> loader;

    Int2IntSortedSetLruCache(int maxSize, IntFunction<IntSortedSet> loader)
    {
      this.cache = new Int2ObjectLinkedOpenHashMap<>(maxSize);
      this.maxSize = maxSize;
      this.loader = loader;
    }

    @Override
    public IntSortedSet getAndLoadIfAbsent(int key)
    {
      IntSortedSet value = cache.getAndMoveToFirst(key);

      if (value == null) {
        value = loader.apply(key);
        cache.putAndMoveToFirst(key, value);
      }

      if (cache.size() > maxSize) {
        cache.removeLast();
      }

      return value;
    }

    @VisibleForTesting
    IntSortedSet get(int key)
    {
      return cache.get(key);
    }
  }
}
