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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
    if (filter instanceof AndDimFilter) {
      final List<DimFilter> children = getAndFilterChildren((AndDimFilter) filter);
      final DimFilter one = doSimplifyAnd(children);
      final DimFilter two = negate(doSimplifyOr(negateAll(children)));
      return computeCost(one) <= computeCost(two) ? one : two;
    } else if (filter instanceof OrDimFilter) {
      final List<DimFilter> children = getOrFilterChildren((OrDimFilter) filter);
      final DimFilter one = doSimplifyOr(children);
      final DimFilter two = negate(doSimplifyAnd(negateAll(children)));
      return computeCost(one) <= computeCost(two) ? one : two;
    } else if (filter instanceof NotDimFilter) {
      final DimFilter field = ((NotDimFilter) filter).getField();
      final DimFilter candidate;
      if (field instanceof OrDimFilter) {
        candidate = doSimplifyAnd(negateAll(getOrFilterChildren((OrDimFilter) field)));
      } else if (field instanceof AndDimFilter) {
        candidate = doSimplifyOr(negateAll(getAndFilterChildren((AndDimFilter) field)));
      } else {
        candidate = negate(field);
      }
      return computeCost(filter) <= computeCost(candidate) ? filter : candidate;
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
   * Simplify BoundDimFilters that are children of an OR or an AND.
   *
   * @param children    the filters
   * @param disjunction true for disjunction, false for conjunction
   *
   * @return simplified filters
   */
  private static DimFilter doSimplify(final List<DimFilter> children, boolean disjunction)
  {
    // Copy children list
    final List<DimFilter> newChildren = Lists.newArrayList(children);

    // Group Bound filters by dimension, extractionFn, and comparator and compute a RangeSet for each one.
    final Map<BoundRefKey, List<BoundDimFilter>> bounds = Maps.newHashMap();

    final Iterator<DimFilter> iterator = newChildren.iterator();
    while (iterator.hasNext()) {
      final DimFilter child = iterator.next();

      if (child.equals(Filtration.matchNothing())) {
        // Child matches nothing, equivalent to FALSE
        // OR with FALSE => ignore
        // AND with FALSE => always false, short circuit
        if (disjunction) {
          iterator.remove();
        } else {
          return Filtration.matchNothing();
        }
      } else if (child.equals(Filtration.matchEverything())) {
        // Child matches everything, equivalent to TRUE
        // OR with TRUE => always true, short circuit
        // AND with TRUE => ignore
        if (disjunction) {
          return Filtration.matchEverything();
        } else {
          iterator.remove();
        }
      } else if (child instanceof BoundDimFilter) {
        final BoundDimFilter bound = (BoundDimFilter) child;
        final BoundRefKey boundRefKey = BoundRefKey.from(bound);
        List<BoundDimFilter> filterList = bounds.get(boundRefKey);
        if (filterList == null) {
          filterList = Lists.newArrayList();
          bounds.put(boundRefKey, filterList);
        }
        filterList.add(bound);
      }
    }

    // Try to simplify filters within each group.
    for (Map.Entry<BoundRefKey, List<BoundDimFilter>> entry : bounds.entrySet()) {
      final BoundRefKey boundRefKey = entry.getKey();
      final List<BoundDimFilter> filterList = entry.getValue();

      // Create a RangeSet for this group.
      final RangeSet<BoundValue> rangeSet = disjunction
                                            ? RangeSets.unionRanges(Bounds.toRanges(filterList))
                                            : RangeSets.intersectRanges(Bounds.toRanges(filterList));

      if (rangeSet.asRanges().size() < filterList.size()) {
        // We found a simplification. Remove the old filters and add new ones.
        for (final BoundDimFilter bound : filterList) {
          if (!newChildren.remove(bound)) {
            throw new ISE("WTF?! Tried to remove bound but couldn't?");
          }
        }

        if (rangeSet.asRanges().isEmpty()) {
          // range set matches nothing, equivalent to FALSE
          // OR with FALSE => ignore
          // AND with FALSE => always false, short circuit
          if (disjunction) {
            newChildren.add(Filtration.matchNothing());
          } else {
            return Filtration.matchNothing();
          }
        }

        for (final Range<BoundValue> range : rangeSet.asRanges()) {
          if (!range.hasLowerBound() && !range.hasUpperBound()) {
            // range matches all, equivalent to TRUE
            // AND with TRUE => ignore
            // OR with TRUE => always true; short circuit
            if (disjunction) {
              return Filtration.matchEverything();
            } else {
              newChildren.add(Filtration.matchEverything());
            }
          } else {
            newChildren.add(Bounds.toFilter(boundRefKey, range));
          }
        }
      }
    }

    Preconditions.checkState(newChildren.size() > 0, "newChildren.size > 0");
    if (newChildren.size() == 1) {
      return newChildren.get(0);
    } else {
      return disjunction ? new OrDimFilter(newChildren) : new AndDimFilter(newChildren);
    }
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
    } else {
      return new NotDimFilter(filter);
    }
  }

  private static List<DimFilter> negateAll(final List<DimFilter> children)
  {
    final List<DimFilter> newChildren = Lists.newArrayListWithCapacity(children.size());
    for (final DimFilter child : children) {
      newChildren.add(negate(child));
    }
    return newChildren;
  }

  private static int computeCost(final DimFilter filter)
  {
    if (filter instanceof NotDimFilter) {
      return computeCost(((NotDimFilter) filter).getField());
    } else if (filter instanceof AndDimFilter) {
      int cost = 0;
      for (DimFilter field : ((AndDimFilter) filter).getFields()) {
        cost += computeCost(field);
      }
      return cost;
    } else if (filter instanceof OrDimFilter) {
      int cost = 0;
      for (DimFilter field : ((OrDimFilter) filter).getFields()) {
        cost += computeCost(field);
      }
      return cost;
    } else {
      return 1;
    }
  }
}
