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

package org.apache.druid.segment.join.lookup;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.math.expr.Expr;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.segment.BaseDoubleColumnValueSelector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.ColumnProcessorFactory;
import org.apache.druid.segment.ColumnProcessors;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.join.Equality;
import org.apache.druid.segment.join.JoinConditionAnalysis;
import org.apache.druid.segment.join.JoinMatcher;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LookupJoinMatcher implements JoinMatcher
{
  private static final ColumnProcessorFactory<Supplier<String>> LEFT_KEY_READER =
      new ColumnProcessorFactory<Supplier<String>>()
      {
        @Override
        public ColumnType defaultType()
        {
          return ColumnType.STRING;
        }

        @Override
        public Supplier<String> makeDimensionProcessor(DimensionSelector selector, boolean multiValue)
        {
          return () -> {
            final IndexedInts row = selector.getRow();

            if (row.size() == 1) {
              return selector.lookupName(row.get(0));
            } else if (row.size() == 0) {
              return null;
            } else {
              // Multi-valued rows are not handled by the join system right now
              // TODO: Remove when https://github.com/apache/druid/issues/9924 is done
              throw new QueryUnsupportedException("Joining against a multi-value dimension is not supported.");
            }
          };
        }

        @Override
        public Supplier<String> makeFloatProcessor(BaseFloatColumnValueSelector selector)
        {
          if (NullHandling.replaceWithDefault()) {
            return () -> DimensionHandlerUtils.convertObjectToString(selector.getFloat());
          } else {
            return () -> selector.isNull() ? null : DimensionHandlerUtils.convertObjectToString(selector.getFloat());
          }
        }

        @Override
        public Supplier<String> makeDoubleProcessor(BaseDoubleColumnValueSelector selector)
        {
          if (NullHandling.replaceWithDefault()) {
            return () -> DimensionHandlerUtils.convertObjectToString(selector.getDouble());
          } else {
            return () -> selector.isNull() ? null : DimensionHandlerUtils.convertObjectToString(selector.getDouble());
          }
        }

        @Override
        public Supplier<String> makeLongProcessor(BaseLongColumnValueSelector selector)
        {
          if (NullHandling.replaceWithDefault()) {
            return () -> DimensionHandlerUtils.convertObjectToString(selector.getLong());
          } else {
            return () -> selector.isNull() ? null : DimensionHandlerUtils.convertObjectToString(selector.getLong());
          }
        }

        @Override
        public Supplier<String> makeArrayProcessor(
            BaseObjectColumnValueSelector<?> selector,
            @Nullable ColumnCapabilities columnCapabilities
        )
        {
          throw new QueryUnsupportedException("Joining against a ARRAY columns is not supported.");
        }

        @Override
        public Supplier<String> makeComplexProcessor(BaseObjectColumnValueSelector<?> selector)
        {
          return () -> null;
        }
      };

  // currentIterator and currentEntry track iteration position through the currently-matched-rows.
  // 1) currentEntry is the entry that our column selector factory is looking at right now.
  // 2) currentIterator contains future matches that it _will_ be looking at after nextMatch() is called.
  @Nullable
  private Iterator<Map.Entry<String, String>> currentIterator = null;
  private final SettableSupplier<Pair<String, String>> currentEntry = new SettableSupplier<>();

  private final LookupExtractor extractor;
  private final JoinConditionAnalysis condition;
  private final List<Supplier<String>> keySuppliers;
  private final ColumnSelectorFactory selectorFactory = new LookupColumnSelectorFactory(currentEntry::get);

  // matchedKeys and matchingRemainder are used to implement matchRemainder().
  private boolean matchingRemainder = false;
  private final Set<String> matchedKeys;

  private LookupJoinMatcher(
      LookupExtractor extractor,
      ColumnSelectorFactory leftSelectorFactory,
      JoinConditionAnalysis condition,
      @Nullable List<Expr> keyExprs,
      boolean remainderNeeded
  )
  {
    this.extractor = extractor;
    this.matchedKeys = remainderNeeded && !condition.isAlwaysTrue() && !condition.isAlwaysFalse()
                       ? new HashSet<>()
                       : null;
    this.condition = condition;

    if (keyExprs != null) {
      this.keySuppliers = keyExprs.stream()
                                  .map(
                                      expr ->
                                          ColumnProcessors.makeProcessor(
                                              expr,
                                              ColumnType.STRING,
                                              LEFT_KEY_READER,
                                              leftSelectorFactory
                                          )
                                  )
                                  .collect(Collectors.toList());
    } else {
      // This check is to guard against bugs; users should never see it.
      Preconditions.checkState(
          condition.isAlwaysFalse() || condition.isAlwaysTrue(),
          "Condition must be always true or always false when keySuppliers == null"
      );

      this.keySuppliers = null;
    }

    // Verify that extractor can be iterated when needed.
    if (condition.isAlwaysTrue() || remainderNeeded) {
      Preconditions.checkState(
          extractor.supportsAsMap(),
          "Cannot read lookup as Map, which is required for this join"
      );
    }
  }

  public static LookupJoinMatcher create(
      LookupExtractor extractor,
      ColumnSelectorFactory leftSelectorFactory,
      JoinConditionAnalysis condition,
      boolean remainderNeeded
  )
  {
    final List<Expr> keyExprs;

    if (condition.isAlwaysTrue()) {
      keyExprs = null;
    } else if (condition.isAlwaysFalse()) {
      keyExprs = null;
    } else if (!condition.getNonEquiConditions().isEmpty()) {
      throw new IAE("Cannot join lookup with non-equi condition: %s", condition);
    } else if (!condition.getRightEquiConditionKeys()
                         .stream()
                         .allMatch(LookupColumnSelectorFactory.KEY_COLUMN::equals)) {
      throw new IAE("Cannot join lookup with condition referring to non-key column: %s", condition);
    } else {
      keyExprs = condition.getEquiConditions().stream().map(Equality::getLeftExpr).collect(Collectors.toList());
    }

    return new LookupJoinMatcher(extractor, leftSelectorFactory, condition, keyExprs, remainderNeeded);
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return selectorFactory;
  }

  @Override
  public void matchCondition()
  {
    currentIterator = null;
    matchingRemainder = false;

    if (condition.isAlwaysFalse()) {
      currentEntry.set(null);
    } else if (condition.isAlwaysTrue()) {
      currentIterator = extractor.asMap().entrySet().iterator();
      nextMatch();
    } else {
      // Not always true, not always false, it's a normal condition.

      if (keySuppliers.isEmpty()) {
        currentEntry.set(null);
        return;
      }

      Iterator<Supplier<String>> keySupplierIterator = keySuppliers.iterator();
      String theKey = keySupplierIterator.next().get();

      if (theKey == null) {
        currentEntry.set(null);
        return;
      }

      // In order to match, all keySuppliers must return the same string, which must be a key in the lookup.
      while (keySupplierIterator.hasNext()) {
        if (!theKey.equals(keySupplierIterator.next().get())) {
          currentEntry.set(null);
          return;
        }
      }

      // All keySuppliers matched. Check if they are actually in the lookup.
      checkInLookup(theKey);
    }
  }

  private void checkInLookup(String theKey)
  {
    // All keySuppliers matched. Check if they are actually in the lookup.
    final String theValue = extractor.apply(theKey);

    if (theValue != null) {
      assert theKey != null;
      currentEntry.set(Pair.of(theKey, theValue));

      if (matchedKeys != null) {
        matchedKeys.add(theKey);
      }
    } else {
      currentEntry.set(null);
    }
  }

  @Override
  public void matchRemainder()
  {
    matchingRemainder = true;

    if (condition.isAlwaysFalse()) {
      currentIterator = extractor.asMap().entrySet().iterator();
    } else if (condition.isAlwaysTrue()) {
      currentIterator = Collections.emptyIterator();
    } else {
      //noinspection ConstantConditions - entry can not be null because extractor.iterable() prevents this
      currentIterator = Iterators.filter(
          extractor.asMap().entrySet().iterator(),
          entry -> !matchedKeys.contains(entry.getKey())
      );
    }

    nextMatch();
  }

  @Override
  public boolean hasMatch()
  {
    return currentEntry.get() != null;
  }

  @Override
  public boolean matchingRemainder()
  {
    return matchingRemainder;
  }

  @Override
  public void nextMatch()
  {
    if (currentIterator != null && currentIterator.hasNext()) {
      final Map.Entry<String, String> entry = currentIterator.next();
      currentEntry.set(Pair.of(entry.getKey(), entry.getValue()));
    } else {
      currentIterator = null;
      currentEntry.set(null);
    }
  }

  @Override
  public void reset()
  {
    // Do not reset matchedKeys; we want to remember it across reset() calls so the 'remainder' is anything
    // that was unmatched across _all_ cursor walks.
    currentEntry.set(null);
    currentIterator = null;
    matchingRemainder = false;
  }
}
