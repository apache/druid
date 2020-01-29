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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import it.unimi.dsi.fastutil.ints.IntRBTreeSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class IndexedTableJoinMatcher implements JoinMatcher
{
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

    final int keyColumnNumber = table.allColumns().indexOf(condition.getRightColumn());
    final ValueType keyColumnType = table.rowSignature().get(condition.getRightColumn());
    final IndexedTable.Index index = table.columnIndex(keyColumnNumber);

    return ColumnProcessors.makeProcessor(
        condition.getLeftExpr(),
        keyColumnType,
        new ConditionMatcherFactory(keyColumnType, index),
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
    currentRow = -1;
    matchingRemainder = false;
  }

  private void advanceCurrentRow()
  {
    if (currentIterator != null && currentIterator.hasNext()) {
      currentRow = currentIterator.nextInt();
    } else {
      currentIterator = null;
      currentRow = -1;
    }
  }

  /**
   * Makes suppliers that returns the list of IndexedTable rows that match the values from selectors.
   */
  private static class ConditionMatcherFactory implements ColumnProcessorFactory<Supplier<IntIterator>>
  {
    private final ValueType keyType;
    private final IndexedTable.Index index;

    ConditionMatcherFactory(ValueType keyType, IndexedTable.Index index)
    {
      this.keyType = keyType;
      this.index = index;
    }

    @Override
    public ValueType defaultType()
    {
      return keyType;
    }

    @Override
    public Supplier<IntIterator> makeDimensionProcessor(DimensionSelector selector)
    {
      return () -> {
        final IndexedInts row = selector.getRow();

        if (row.size() == 1) {
          final String key = selector.lookupName(row.get(0));
          return index.find(key).iterator();
        } else {
          // Multi-valued rows are not handled by the join system right now; treat them as nulls.
          return IntIterators.EMPTY_ITERATOR;
        }
      };
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
}
