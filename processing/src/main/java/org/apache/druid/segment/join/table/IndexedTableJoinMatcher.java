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
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
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
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class IndexedTableJoinMatcher implements JoinMatcher
{
  private static final int UNINITIALIZED_CURRENT_ROW = -1;

  // Key column type to use when the actual key type is unknown.
  static final ValueType DEFAULT_KEY_TYPE = ValueType.STRING;

  private final IndexedTable table;
  private final List<Supplier<IntIterator>> conditionMatchers;
  private final IntIterator[] currentMatchedRows;
  private final ColumnSelectorFactory selectorFactory;

  // matchedRows and matchingRemainder are used to implement matchRemainder().
  private final IntSet matchedRows;
  private boolean matchingRemainder = false;

  // currentIterator and currentRow are used to track iteration position through the currently-matched-rows.
  @Nullable
  private IntIterator currentIterator;
  private int currentRow;

  IndexedTableJoinMatcher(
      final IndexedTable table,
      final ColumnSelectorFactory leftSelectorFactory,
      final JoinConditionAnalysis condition,
      final boolean remainderNeeded
  )
  {
    this.table = table;
    this.currentRow = UNINITIALIZED_CURRENT_ROW;

    if (condition.isAlwaysTrue()) {
      this.conditionMatchers = Collections.singletonList(() -> IntIterators.fromTo(0, table.numRows()));
    } else if (condition.isAlwaysFalse()) {
      this.conditionMatchers = Collections.singletonList(() -> IntIterators.EMPTY_ITERATOR);
    } else if (condition.getNonEquiConditions().isEmpty()) {
      this.conditionMatchers = condition.getEquiConditions()
                                        .stream()
                                        .map(eq -> makeConditionMatcher(table, leftSelectorFactory, eq))
                                        .collect(Collectors.toCollection(ArrayList::new));
    } else {
      throw new IAE(
          "Cannot build hash-join matcher on non-equi-join condition: %s",
          condition.getOriginalExpression()
      );
    }

    this.currentMatchedRows = new IntIterator[conditionMatchers.size()];
    this.selectorFactory = new IndexedTableColumnSelectorFactory(table, () -> currentRow);

    if (remainderNeeded) {
      this.matchedRows = new IntRBTreeSet();
    } else {
      this.matchedRows = null;
    }

  }

  private static Supplier<IntIterator> makeConditionMatcher(
      final IndexedTable table,
      final ColumnSelectorFactory selectorFactory,
      final Equality condition
  )
  {
    if (!table.keyColumns().contains(condition.getRightColumn())) {
      throw new IAE("Cannot build hash-join matcher on non-key-based condition: %s", condition);
    }

    final int keyColumnNumber = table.rowSignature().indexOf(condition.getRightColumn());

    final ValueType keyType =
        table.rowSignature().getColumnType(condition.getRightColumn()).orElse(DEFAULT_KEY_TYPE);

    final IndexedTable.Index index = table.columnIndex(keyColumnNumber);

    return ColumnProcessors.makeProcessor(
        condition.getLeftExpr(),
        keyType,
        new ConditionMatcherFactory(keyType, index),
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

    for (int i = 0; i < conditionMatchers.size(); i++) {
      final IntIterator rows = conditionMatchers.get(i).get();
      if (rows.hasNext()) {
        currentMatchedRows[i] = rows;
      } else {
        return;
      }
    }

    if (currentMatchedRows.length == 1) {
      currentIterator = currentMatchedRows[0];
    } else {
      currentIterator = new SortedIntIntersectionIterator(currentMatchedRows);
    }

    nextMatch();
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
    nextMatch();
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

    if (!matchingRemainder && matchedRows != null && hasMatch()) {
      matchedRows.add(currentRow);
    }
  }

  @Override
  public void reset()
  {
    // Do not reset matchedRows; we want to remember it across reset() calls so the 'remainder' is anything
    // that was unmatched across _all_ cursor walks.
    currentIterator = null;
    currentRow = UNINITIALIZED_CURRENT_ROW;
    matchingRemainder = false;
  }

  private void advanceCurrentRow()
  {
    if (currentIterator != null && currentIterator.hasNext()) {
      currentRow = currentIterator.nextInt();
    } else {
      currentIterator = null;
      currentRow = UNINITIALIZED_CURRENT_ROW;
    }
  }

  /**
   * Makes suppliers that returns the list of IndexedTable rows that match the values from selectors.
   */
  @VisibleForTesting
  static class ConditionMatcherFactory implements ColumnProcessorFactory<Supplier<IntIterator>>
  {
    @VisibleForTesting
    static final int CACHE_MAX_SIZE = 1000;

    private static final int MAX_NUM_CACHE = 10;

    private final ValueType keyType;
    private final IndexedTable.Index index;

    // DimensionSelector -> (int) dimension id -> (IntList) row numbers
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")  // updated via computeIfAbsent
    private final LruLoadingHashMap<DimensionSelector, Int2IntListMap> dimensionCaches;

    ConditionMatcherFactory(ValueType keyType, IndexedTable.Index index)
    {
      this.keyType = keyType;
      this.index = index;

      this.dimensionCaches = new LruLoadingHashMap<>(
          MAX_NUM_CACHE,
          selector -> {
            int cardinality = selector.getValueCardinality();
            IntFunction<IntList> loader = dimensionId -> getRowNumbers(selector, dimensionId);
            return cardinality <= CACHE_MAX_SIZE
                   ? new Int2IntListLookupTable(cardinality, loader)
                   : new Int2IntListLruCache(CACHE_MAX_SIZE, loader);
          }
      );
    }

    private IntList getRowNumbers(DimensionSelector selector, int dimensionId)
    {
      final String key = selector.lookupName(dimensionId);
      return index.find(key);
    }

    private IntList getAndCacheRowNumbers(DimensionSelector selector, int dimensionId)
    {
      return dimensionCaches.getAndLoadIfAbsent(selector).getAndLoadIfAbsent(dimensionId);
    }

    @Override
    public ValueType defaultType()
    {
      return keyType;
    }

    @Override
    public Supplier<IntIterator> makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
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
            IntList rowNumbers = getRowNumbers(selector, dimensionId);
            return rowNumbers.iterator();
          } else if (row.size() == 0) {
            return IntIterators.EMPTY_ITERATOR;
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
            IntList rowNumbers = getAndCacheRowNumbers(selector, dimensionId);
            return rowNumbers.iterator();
          } else if (row.size() == 0) {
            return IntIterators.EMPTY_ITERATOR;
          } else {
            // Multi-valued rows are not handled by the join system right now
            // TODO: Remove when https://github.com/apache/druid/issues/9924 is done
            throw new QueryUnsupportedException("Joining against a multi-value dimension is not supported.");
          }
        };
      }
    }

    @Override
    public Supplier<IntIterator> makeFloatProcessor(BaseFloatColumnValueSelector selector)
    {
      if (NullHandling.replaceWithDefault()) {
        return () -> index.find(selector.getFloat()).iterator();
      } else {
        return () -> selector.isNull() ? IntIterators.EMPTY_ITERATOR : index.find(selector.getFloat()).iterator();
      }
    }

    @Override
    public Supplier<IntIterator> makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
    {
      if (NullHandling.replaceWithDefault()) {
        return () -> index.find(selector.getDouble()).iterator();
      } else {
        return () -> selector.isNull() ? IntIterators.EMPTY_ITERATOR : index.find(selector.getDouble()).iterator();
      }
    }

    @Override
    public Supplier<IntIterator> makeLongProcessor(BaseLongColumnValueSelector selector)
    {
      if (NullHandling.replaceWithDefault()) {
        return () -> index.find(selector.getLong()).iterator();
      } else {
        return () -> selector.isNull() ? IntIterators.EMPTY_ITERATOR : index.find(selector.getLong()).iterator();
      }
    }

    @Override
    public Supplier<IntIterator> makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
    {
      return () -> IntIterators.EMPTY_ITERATOR;
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

  private interface Int2IntListMap
  {
    IntList getAndLoadIfAbsent(int key);
  }

  /**
   * Lookup table for keys in the range from 0 to maxSize - 1
   */
  @VisibleForTesting
  static class Int2IntListLookupTable implements Int2IntListMap
  {
    private final IntList[] lookup;
    private final IntFunction<IntList> loader;

    Int2IntListLookupTable(int maxSize, IntFunction<IntList> loader)
    {
      this.loader = loader;
      this.lookup = new IntList[maxSize];
    }

    @Override
    public IntList getAndLoadIfAbsent(int key)
    {
      IntList value = lookup[key];

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
  static class Int2IntListLruCache implements Int2IntListMap
  {
    private final Int2ObjectLinkedOpenHashMap<IntList> cache;
    private final int maxSize;
    private final IntFunction<IntList> loader;

    Int2IntListLruCache(int maxSize, IntFunction<IntList> loader)
    {
      this.cache = new Int2ObjectLinkedOpenHashMap<>(maxSize);
      this.maxSize = maxSize;
      this.loader = loader;
    }

    @Override
    public IntList getAndLoadIfAbsent(int key)
    {
      IntList value = cache.getAndMoveToFirst(key);

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
    IntList get(int key)
    {
      return cache.get(key);
    }
  }
}
