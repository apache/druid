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

package org.apache.druid.sql.calcite.filtration;

import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.objects.ObjectIntPair;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.filter.InDimFilter;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class for collecting point comparisons that are children to an OR, such as equalities. Each set of
 * comparisons with the same {@link CollectionKey} can potentially become a single {@link CollectedType}.
 * For example: x = 'a', x = 'b' can become x IN ('a', 'b').
 */
public abstract class CollectComparisons<BaseType, ComparisonType extends BaseType, CollectedType extends BaseType, CollectionKey>
{
  /**
   * List of {@link BaseType} that were ORed together.
   */
  private final List<BaseType> orExprs;

  /**
   * Copy of {@link #orExprs} with point comparisons collected, or a reference to the same {@link #orExprs} if there
   * was nothing to collect.
   */
  private List<BaseType> retVal;

  protected CollectComparisons(final List<BaseType> orExprs)
  {
    this.orExprs = orExprs;
  }

  /**
   * Perform collection logic on the exprs provided to the constructor.
   */
  public List<BaseType> collect()
  {
    if (retVal != null) {
      return retVal;
    }

    IntSet orExprsToRemove = null;

    // Group comparisons together when they have the same collection key.
    // Map key: pair of collection key (from getCollectionKey), and any other expressions that may be ANDed in.
    // Map value: list of collectible comparisons paired with their original positions in "orExprs".
    final Map<Pair<CollectionKey, List<BaseType>>, List<ObjectIntPair<ComparisonType>>> collectMap = new LinkedHashMap<>();

    // Group all comparisons from the "orExprs" list into the "selectors" map.
    for (int orExprIndex = 0; orExprIndex < orExprs.size(); orExprIndex++) {
      final BaseType orExpr = orExprs.get(orExprIndex);
      final Pair<ComparisonType, List<BaseType>> selectorFound = getCollectibleComparison(orExpr);

      if (selectorFound != null) {
        final ComparisonType selector = selectorFound.lhs;
        final CollectionKey refKey = getCollectionKey(selector);
        if (refKey == null) {
          continue;
        }

        final List<ObjectIntPair<ComparisonType>> comparisonList = collectMap.computeIfAbsent(
            Pair.of(refKey, selectorFound.rhs),
            k -> new ArrayList<>()
        );
        comparisonList.add(ObjectIntPair.of(selector, orExprIndex));
      }
    }

    // Emit a collected comparison (e.g. IN filters) for each collection.
    for (Map.Entry<Pair<CollectionKey, List<BaseType>>, List<ObjectIntPair<ComparisonType>>> entry : collectMap.entrySet()) {
      final List<ObjectIntPair<ComparisonType>> comparisonList = entry.getValue();
      final InDimFilter.ValuesSet values = new InDimFilter.ValuesSet();

      for (ObjectIntPair<ComparisonType> subEntry : comparisonList) {
        final ComparisonType selector = subEntry.first();
        values.addAll(getMatchValues(selector));
      }

      final CollectedType collected = makeCollectedComparison(entry.getKey().lhs, values);
      if (collected != null) {
        // Remove the old comparisons, and add the collected one.
        for (ObjectIntPair<ComparisonType> subEntry : comparisonList) {
          final int originalIndex = subEntry.rightInt();

          if (orExprsToRemove == null) {
            orExprsToRemove = new IntOpenHashSet();
          }

          orExprsToRemove.add(originalIndex);
        }

        if (retVal == null) {
          retVal = new ArrayList<>();
        }

        // Other expressions that were ANDed with this collection.
        final List<BaseType> andExprs = entry.getKey().rhs;

        if (andExprs.isEmpty()) {
          retVal.add(collected);
        } else {
          final List<BaseType> allComparisons = new ArrayList<>(andExprs.size() + 1);
          allComparisons.addAll(andExprs);
          allComparisons.add(collected);
          retVal.add(makeAnd(allComparisons));
        }
      }
    }

    if (retVal == null) {
      retVal = orExprs;
    } else {
      for (int i = 0; i < orExprs.size(); i++) {
        if (orExprsToRemove == null || !orExprsToRemove.contains(i)) {
          retVal.add(orExprs.get(i));
        }
      }
    }

    return retVal;
  }

  /**
   * Given an expression, returns a collectible comparison (if the provided expression is collectible), or returns a
   * pair of collectible comparison and other expressions (if the provided expression is an AND of a collectible
   * comparison and those other expressions).
   */
  @Nullable
  protected abstract Pair<ComparisonType, List<BaseType>> getCollectibleComparison(BaseType expr);

  /**
   * Given a comparison, returns its collection key, which will be used to group it together with like comparisons.
   * This method will be called on objects returned by {@link #getCollectibleComparison(Object)}. If this method returns
   * null, the filter is considered non-collectible.
   */
  @Nullable
  protected abstract CollectionKey getCollectionKey(ComparisonType comparison);

  /**
   * Given a comparison, returns the strings that it matches.
   */
  protected abstract Set<String> getMatchValues(ComparisonType comparison);

  /**
   * Given a set of strings from {@link #getMatchValues(Object)} from various comparisons, returns a single collected
   * comparison that matches all those strings.
   */
  @Nullable
  protected abstract CollectedType makeCollectedComparison(CollectionKey key, InDimFilter.ValuesSet values);

  /**
   * Given a list of expressions, returns an AND expression with those exprs as children. Only called if
   * {@link #getCollectibleComparison(Object)} returns nonempty right-hand-sides.
   */
  protected abstract BaseType makeAnd(List<BaseType> exprs);
}
