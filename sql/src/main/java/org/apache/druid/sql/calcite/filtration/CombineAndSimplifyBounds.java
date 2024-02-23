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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import it.unimi.dsi.fastutil.Pair;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIntPair;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.FalseDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RangeFilter;
import org.apache.druid.segment.column.ColumnType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CombineAndSimplifyBounds extends BottomUpTransform
{
  private static final CombineAndSimplifyBounds INSTANCE = new CombineAndSimplifyBounds();

  private CombineAndSimplifyBounds()
  {
  }

  public static CombineAndSimplifyBounds instance()
  {
    return INSTANCE;
  }

  @Override
  public DimFilter process(DimFilter filter)
  {
    if (filter instanceof FalseDimFilter) {
      // we might sometimes come into here with just a false from optimizing impossible conditions
      return filter;
    } else if (filter instanceof AndDimFilter) {
      final List<DimFilter> children = getAndFilterChildren((AndDimFilter) filter);
      return doSimplifyAnd(children);
    } else if (filter instanceof OrDimFilter) {
      final List<DimFilter> children = getOrFilterChildren((OrDimFilter) filter);
      return doSimplifyOr(children);
    } else if (filter instanceof NotDimFilter) {
      final DimFilter field = ((NotDimFilter) filter).getField();
      return negate(field);
    } else {
      return filter;
    }
  }

  private List<DimFilter> getAndFilterChildren(final AndDimFilter filter)
  {
    final List<DimFilter> children = new ArrayList<>();
    for (final DimFilter field : filter.getFields()) {
      if (field instanceof AndDimFilter) {
        children.addAll(getAndFilterChildren((AndDimFilter) field));
      } else {
        children.add(field);
      }
    }
    return children;
  }

  private List<DimFilter> getOrFilterChildren(final OrDimFilter filter)
  {
    final List<DimFilter> children = new ArrayList<>();
    for (final DimFilter field : filter.getFields()) {
      if (field instanceof OrDimFilter) {
        children.addAll(getOrFilterChildren((OrDimFilter) field));
      } else {
        children.add(field);
      }
    }
    return children;
  }

  private static DimFilter doSimplifyAnd(final List<DimFilter> children)
  {
    return doSimplify(children, false);
  }

  private static DimFilter doSimplifyOr(final List<DimFilter> children)
  {
    return doSimplify(children, true);
  }

  /**
   * Simplify {@link BoundDimFilter} and {@link RangeFilter} that are children of an OR or an AND.
   *
   * @param children    the filters
   * @param disjunction true for OR, false for AND
   *
   * @return simplified filters
   */
  private static DimFilter doSimplify(final List<DimFilter> children, boolean disjunction)
  {
    // Copy the list of child filters. We'll modify the copy and eventually return it.
    // Filters we want to add and remove from "children".
    final List<DimFilter> childrenToAdd = new ArrayList<>();
    final IntOpenHashSet childrenToRemove = new IntOpenHashSet();

    // Group Bound filters by dimension, extractionFn, and comparator and compute a RangeSet for each one.
    // Each filter is paired with its position in the "children" array.
    final Map<BoundRefKey, List<ObjectIntPair<BoundDimFilter>>> bounds = new HashMap<>();
    // Group range filters by dimension, extractionFn, and matchValueType and compute a RangeSet for each one.
    // Each filter is paired with its position in the "children" array.
    final Map<RangeRefKey, List<ObjectIntPair<RangeFilter>>> ranges = new HashMap<>();
    final Map<String, ColumnType> leastRestrictiveNumericTypes = new HashMap<>();

    // all and/or filters have at least 1 child
    boolean allFalse = true;
    for (int childIndex = 0; childIndex < children.size(); childIndex++) {
      final DimFilter child = children.get(childIndex);
      if (child instanceof BoundDimFilter) {
        final BoundDimFilter bound = (BoundDimFilter) child;
        final BoundRefKey boundRefKey = BoundRefKey.from(bound);
        final List<ObjectIntPair<BoundDimFilter>> filterList =
            bounds.computeIfAbsent(boundRefKey, k -> new ArrayList<>());
        filterList.add(ObjectIntPair.of(bound, childIndex));
        allFalse = false;
      } else if (child instanceof RangeFilter) {
        final RangeFilter range = (RangeFilter) child;
        final RangeRefKey rangeRefKey = RangeRefKey.from(range);
        if (rangeRefKey.getMatchValueType().isNumeric()) {
          leastRestrictiveNumericTypes.compute(
              range.getColumn(),
              (c, existingType) -> ColumnType.leastRestrictiveType(existingType, range.getMatchValueType())
          );
        }

        final List<ObjectIntPair<RangeFilter>> filterList =
            ranges.computeIfAbsent(rangeRefKey, k -> new ArrayList<>());
        filterList.add(ObjectIntPair.of(range, childIndex));
        allFalse = false;
      } else {
        allFalse = allFalse && (child instanceof FalseDimFilter);
      }
    }

    // short circuit if can never be true
    if (allFalse) {
      return Filtration.matchNothing();
    }

    // Try to simplify "bound" filters within each group of "bounds".
    for (Map.Entry<BoundRefKey, List<ObjectIntPair<BoundDimFilter>>> entry : bounds.entrySet()) {
      final BoundRefKey boundRefKey = entry.getKey();
      final List<ObjectIntPair<BoundDimFilter>> filterList = entry.getValue();

      // Create a RangeSet for this group.
      final RangeSet<BoundValue> rangeSet =
          disjunction
          ? RangeSets.unionRanges(Bounds.toRanges(Lists.transform(filterList, Pair::left)))
          : RangeSets.intersectRanges(Bounds.toRanges(Lists.transform(filterList, Pair::left)));

      if (rangeSet.asRanges().size() < filterList.size()) {
        // We found a simplification. Remove the old filters and add new ones.
        for (final ObjectIntPair<BoundDimFilter> boundAndChildIndex : filterList) {
          childrenToRemove.add(boundAndChildIndex.rightInt());
        }

        if (rangeSet.asRanges().isEmpty()) {
          // range set matches nothing, equivalent to FALSE
          childrenToAdd.add(Filtration.matchNothing());
        }

        for (final Range<BoundValue> range : rangeSet.asRanges()) {
          if (!range.hasLowerBound() && !range.hasUpperBound()) {
            // range matches all, equivalent to TRUE
            childrenToAdd.add(Filtration.matchEverything());
          } else {
            childrenToAdd.add(Bounds.toFilter(boundRefKey, range));
          }
        }
      } else if (disjunction && Range.all().equals(rangeSet.span())) {
        // ranges in disjunction - spanning ALL
        // complementer must be a negated set of ranges
        for (final ObjectIntPair<BoundDimFilter> boundAndChildIndex : filterList) {
          childrenToRemove.add(boundAndChildIndex.rightInt());
        }
        Set<Range<BoundValue>> newRanges = rangeSet.complement().asRanges();
        List<DimFilter> newFilters = new ArrayList<>();
        for (Range<BoundValue> range : newRanges) {
          BoundDimFilter filter = Bounds.toFilter(boundRefKey, range);
          newFilters.add(filter);
        }
        childrenToAdd.add(new NotDimFilter(disjunction(newFilters)));
      }
    }

    // Consolidate groups of numeric ranges in "ranges", using the leastRestrictiveNumericTypes computed earlier.
    final Map<RangeRefKey, List<ObjectIntPair<RangeFilter>>> consolidatedRanges =
        Maps.newHashMapWithExpectedSize(ranges.size());
    for (Map.Entry<RangeRefKey, List<ObjectIntPair<RangeFilter>>> entry : ranges.entrySet()) {
      boolean refKeyChanged = false;
      RangeRefKey refKey = entry.getKey();
      if (entry.getKey().getMatchValueType().isNumeric()) {
        ColumnType numericTypeToUse = leastRestrictiveNumericTypes.get(refKey.getColumn());
        if (!numericTypeToUse.equals(refKey.getMatchValueType())) {
          refKeyChanged = true;
          refKey = new RangeRefKey(refKey.getColumn(), numericTypeToUse);
        }
      }
      final List<ObjectIntPair<RangeFilter>> consolidatedFilterList =
          consolidatedRanges.computeIfAbsent(refKey, k -> new ArrayList<>());

      if (refKeyChanged) {
        for (ObjectIntPair<RangeFilter> filterAndChildIndex : entry.getValue()) {
          final RangeFilter rewrite =
              Ranges.toFilter(refKey, Ranges.toRange(filterAndChildIndex.left(), refKey.getMatchValueType()));
          consolidatedFilterList.add(ObjectIntPair.of(rewrite, filterAndChildIndex.rightInt()));
        }
      } else {
        consolidatedFilterList.addAll(entry.getValue());
      }
    }

    // Try to simplify "range" filters within each group of "consolidatedRanges" (derived from "ranges").
    for (Map.Entry<RangeRefKey, List<ObjectIntPair<RangeFilter>>> entry : consolidatedRanges.entrySet()) {
      final RangeRefKey rangeRefKey = entry.getKey();
      final List<ObjectIntPair<RangeFilter>> filterList = entry.getValue();

      // Create a RangeSet for this group.
      final RangeSet<RangeValue> rangeSet =
          disjunction
          ? RangeSets.unionRanges(Ranges.toRanges(Lists.transform(filterList, Pair::left)))
          : RangeSets.intersectRanges(Ranges.toRanges(Lists.transform(filterList, Pair::left)));

      if (rangeSet.asRanges().size() < filterList.size()) {
        // We found a simplification. Remove the old filters and add new ones.
        for (final ObjectIntPair<RangeFilter> rangeAndChildIndex : filterList) {
          childrenToRemove.add(rangeAndChildIndex.rightInt());
        }

        if (rangeSet.asRanges().isEmpty()) {
          // range set matches nothing, equivalent to FALSE
          childrenToAdd.add(Filtration.matchNothing());
        }

        for (final Range<RangeValue> range : rangeSet.asRanges()) {
          if (!range.hasLowerBound() && !range.hasUpperBound()) {
            // range matches all, equivalent to TRUE
            childrenToAdd.add(Filtration.matchEverything());
          } else {
            childrenToAdd.add(Ranges.toFilter(rangeRefKey, range));
          }
        }
      } else if (disjunction && Range.all().equals(rangeSet.span())) {
        // ranges in disjunction - spanning ALL
        // complementer must be a negated set of ranges
        for (final ObjectIntPair<RangeFilter> boundAndChildIndex : filterList) {
          childrenToRemove.add(boundAndChildIndex.rightInt());
        }
        Set<Range<RangeValue>> newRanges = rangeSet.complement().asRanges();
        List<DimFilter> newFilters = new ArrayList<>();
        for (Range<RangeValue> range : newRanges) {
          RangeFilter filter = Ranges.toFilter(rangeRefKey, range);
          newFilters.add(filter);
        }
        childrenToAdd.add(new NotDimFilter(disjunction(newFilters)));
      }
    }

    // Create newChildren.
    final List<DimFilter> newChildren =
        new ArrayList<>(children.size() + childrenToAdd.size() - childrenToRemove.size());
    for (int i = 0; i < children.size(); i++) {
      if (!childrenToRemove.contains(i)) {
        newChildren.add(children.get(i));
      }
    }
    newChildren.addAll(childrenToAdd);

    // Finally: Go through newChildren, removing or potentially exiting early based on TRUE / FALSE marker filters.
    Preconditions.checkState(newChildren.size() > 0, "newChildren.size > 0");

    final Iterator<DimFilter> iterator = newChildren.iterator();
    while (iterator.hasNext()) {
      final DimFilter newChild = iterator.next();

      if (Filtration.matchNothing().equals(newChild)) {
        // Child matches nothing, equivalent to FALSE
        // OR with FALSE => ignore
        // AND with FALSE => always false, short circuit
        if (disjunction) {
          iterator.remove();
        } else {
          return Filtration.matchNothing();
        }
      } else if (Filtration.matchEverything().equals(newChild)) {
        // Child matches everything, equivalent to TRUE
        // OR with TRUE => always true, short circuit
        // AND with TRUE => ignore
        if (disjunction) {
          return Filtration.matchEverything();
        } else {
          iterator.remove();
        }
      }
    }

    if (newChildren.isEmpty()) {
      // If "newChildren" is empty at this point, it must have consisted entirely of TRUE / FALSE marker filters.
      if (disjunction) {
        // Must have been all FALSE filters (the only kind we would have removed above).
        return Filtration.matchNothing();
      } else {
        // Must have been all TRUE filters (the only kind we would have removed above).
        return Filtration.matchEverything();
      }
    } else if (newChildren.size() == 1) {
      return newChildren.get(0);
    } else {
      return disjunction ? new OrDimFilter(newChildren) : new AndDimFilter(newChildren);
    }
  }

  private static DimFilter disjunction(List<DimFilter> operands)
  {
    Preconditions.checkArgument(operands.size() > 0, "invalid number of operands");
    if (operands.size() == 1) {
      return operands.get(0);
    }
    return new OrDimFilter(operands);
  }

  private static DimFilter negate(final DimFilter filter)
  {
    if (Filtration.matchEverything().equals(filter)) {
      return Filtration.matchNothing();
    } else if (Filtration.matchNothing().equals(filter)) {
      return Filtration.matchEverything();
    } else if (filter instanceof NotDimFilter) {
      return ((NotDimFilter) filter).getField();
    } else if (filter instanceof BoundDimFilter) {
      final BoundDimFilter negated = Bounds.not((BoundDimFilter) filter);
      return negated != null ? negated : new NotDimFilter(filter);
    } else if (filter instanceof RangeFilter) {
      final RangeFilter negated = Ranges.not((RangeFilter) filter);
      return negated != null ? negated : new NotDimFilter(filter);
    } else {
      return new NotDimFilter(filter);
    }
  }
}
