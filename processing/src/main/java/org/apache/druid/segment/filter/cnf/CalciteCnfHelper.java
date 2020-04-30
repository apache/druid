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

package org.apache.druid.segment.filter.cnf;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.FalseFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.TrueFilter;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * All functions in this class were basically adopted from Apache Calcite and modified to use them in Druid.
 * See https://github.com/apache/calcite/blob/branch-1.21/core/src/main/java/org/apache/calcite/rex/RexUtil.java#L1615
 * for original implementation.
 */
public class CalciteCnfHelper
{
  public static Filter pull(Filter rex)
  {
    final Set<Filter> operands;
    if (rex instanceof AndFilter) {
      operands = ((AndFilter) rex).getFilters();
      return and(pullList(operands));
    } else if (rex instanceof OrFilter) {
      operands = ((OrFilter) rex).getFilters();
      final Map<Filter, Filter> factors = commonFactors(operands);
      if (factors.isEmpty()) {
        return or(operands);
      }
      final List<Filter> list = new ArrayList<>();
      for (Filter operand : operands) {
        Filter removed = removeFactor(factors, operand);
        list.add(removed);
      }
      if (list.isEmpty()) {
        return and(factors.values());
      } else if (list.size() == 1) {
        return and(Iterables.concat(factors.values(), ImmutableList.of(list.get(0))));
      } else {
        return and(Iterables.concat(factors.values(), ImmutableList.of(or(list))));
      }
    } else {
      return rex;
    }
  }

  private static List<Filter> pullList(Set<Filter> nodes)
  {
    final List<Filter> list = new ArrayList<>();
    for (Filter node : nodes) {
      Filter pulled = pull(node);
      if (pulled instanceof AndFilter) {
        list.addAll(((AndFilter) pulled).getFilters());
      } else {
        list.add(pulled);
      }
    }
    return list;
  }

  private static Map<Filter, Filter> commonFactors(Set<Filter> nodes)
  {
    final Map<Filter, Filter> map = new HashMap<>();
    int i = 0;
    for (Filter node : nodes) {
      if (i++ == 0) {
        for (Filter conjunction : conjunctions(node)) {
          map.put(conjunction, conjunction);
        }
      } else {
        map.keySet().retainAll(conjunctions(node));
      }
    }
    return map;
  }

  private static Filter removeFactor(Map<Filter, Filter> factors, Filter node)
  {
    List<Filter> list = new ArrayList<>();
    for (Filter operand : conjunctions(node)) {
      if (!factors.containsKey(operand)) {
        list.add(operand);
      }
    }
    return and(list);
  }

  private static Filter and(Iterable<? extends Filter> nodes)
  {
    return composeConjunction(nodes);
  }

  private static Filter or(Iterable<? extends Filter> nodes)
  {
    return composeDisjunction(nodes);
  }

  /** As {@link #composeConjunction(Iterable, boolean)} but never
   * returns null. */
  public static @Nonnull Filter composeConjunction(Iterable<? extends Filter> nodes)
  {
    final Filter e = composeConjunction(nodes, false);
    return Objects.requireNonNull(e);
  }

  /**
   * Converts a collection of expressions into an AND.
   * If there are zero expressions, returns TRUE.
   * If there is one expression, returns just that expression.
   * If any of the expressions are FALSE, returns FALSE.
   * Removes expressions that always evaluate to TRUE.
   * Returns null only if {@code nullOnEmpty} and expression is TRUE.
   */
  public static Filter composeConjunction(Iterable<? extends Filter> nodes, boolean nullOnEmpty)
  {
    ImmutableList<Filter> list = flattenAnd(nodes);
    switch (list.size()) {
      case 0:
        return nullOnEmpty
               ? null
               : TrueFilter.instance();
      case 1:
        return list.get(0);
      default:
        return new AndFilter(list);
    }
  }

  /** Flattens a list of AND nodes.
   *
   * <p>Treats null nodes as literal TRUE (i.e. ignores them). */
  public static ImmutableList<Filter> flattenAnd(Iterable<? extends Filter> nodes)
  {
    if (nodes instanceof Collection && ((Collection) nodes).isEmpty()) {
      // Optimize common case
      return ImmutableList.of();
    }
    final ImmutableList.Builder<Filter> builder = ImmutableList.builder();
    final Set<Filter> set = new HashSet<>(); // to eliminate duplicates
    for (Filter node : nodes) {
      if (node != null) {
        addAnd(builder, set, node);
      }
    }
    return builder.build();
  }

  private static void addAnd(ImmutableList.Builder<Filter> builder, Set<Filter> digests, Filter node)
  {
    if (node instanceof AndFilter) {
      for (Filter operand : ((AndFilter) node).getFilters()) {
        addAnd(builder, digests, operand);
      }
    } else {
      if (!(node instanceof TrueFilter) && digests.add(node)) {
        builder.add(node);
      }
    }
  }

  /**
   * Converts a collection of expressions into an OR.
   * If there are zero expressions, returns FALSE.
   * If there is one expression, returns just that expression.
   * If any of the expressions are TRUE, returns TRUE.
   * Removes expressions that always evaluate to FALSE.
   * Flattens expressions that are ORs.
   */
  @Nonnull public static Filter composeDisjunction(Iterable<? extends Filter> nodes)
  {
    final Filter e = composeDisjunction(nodes, false);
    return Objects.requireNonNull(e);
  }

  /**
   * Converts a collection of expressions into an OR,
   * optionally returning null if the list is empty.
   */
  public static Filter composeDisjunction(Iterable<? extends Filter> nodes, boolean nullOnEmpty)
  {
    ImmutableList<Filter> list = flattenOr(nodes);
    switch (list.size()) {
      case 0:
        return nullOnEmpty ? null : FalseFilter.instance();
      case 1:
        return list.get(0);
      default:
        if (containsTrue(list)) {
          return TrueFilter.instance();
        }
        return new OrFilter(list);
    }
  }

  /** Flattens a list of OR nodes. */
  public static ImmutableList<Filter> flattenOr(Iterable<? extends Filter> nodes)
  {
    if (nodes instanceof Collection && ((Collection) nodes).isEmpty()) {
      // Optimize common case
      return ImmutableList.of();
    }
    final ImmutableList.Builder<Filter> builder = ImmutableList.builder();
    final Set<Filter> set = new HashSet<>(); // to eliminate duplicates
    for (Filter node : nodes) {
      addOr(builder, set, node);
    }
    return builder.build();
  }

  private static void addOr(ImmutableList.Builder<Filter> builder, Set<Filter> set, Filter node)
  {
    if (node instanceof OrFilter) {
      for (Filter operand : ((OrFilter) node).getFilters()) {
        addOr(builder, set, operand);
      }
    } else {
      if (set.add(node)) {
        builder.add(node);
      }
    }
  }

  /**
   * Returns a condition decomposed by AND.
   *
   * <p>For example, {@code conjunctions(TRUE)} returns the empty list;
   * {@code conjunctions(FALSE)} returns list {@code {FALSE}}.</p>
   */
  public static List<Filter> conjunctions(Filter rexPredicate)
  {
    final List<Filter> list = new ArrayList<>();
    decomposeConjunction(rexPredicate, list);
    return list;
  }

  /**
   * Decomposes a predicate into a list of expressions that are AND'ed
   * together.
   *
   * @param rexPredicate predicate to be analyzed
   * @param rexList      list of decomposed RexNodes
   */
  public static void decomposeConjunction(Filter rexPredicate, List<Filter> rexList)
  {
    if (rexPredicate == null || rexPredicate instanceof TrueFilter) {
      return;
    }
    if (rexPredicate instanceof AndFilter) {
      for (Filter operand : ((AndFilter) rexPredicate).getFilters()) {
        decomposeConjunction(operand, rexList);
      }
    } else {
      rexList.add(rexPredicate);
    }
  }

  private static boolean containsTrue(Iterable<Filter> nodes)
  {
    for (Filter node : nodes) {
      if (node instanceof TrueFilter) {
        return true;
      }
    }
    return false;
  }

  private CalciteCnfHelper()
  {
  }
}
