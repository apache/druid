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

package org.apache.druid.segment.filter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.druid.query.filter.BooleanFilter;
import org.apache.druid.query.filter.Filter;

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
 * A helper class to convert a filter to CNF.
 *
 * The methods in this class are mainly adopted from Apache Hive and Apache Calcite.
 */
public class CnfHelper
{
  public static Filter toCnf(Filter current)
  {
    current = pushDownNot(current);
    current = flatten(current);
    current = pull(current);
    current = convertToCNFInternal(current);
    current = flatten(current);
    return current;
  }

  // A helper function adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  @VisibleForTesting
  static Filter pushDownNot(Filter current)
  {
    if (current instanceof NotFilter) {
      Filter child = ((NotFilter) current).getBaseFilter();
      if (child instanceof NotFilter) {
        return pushDownNot(((NotFilter) child).getBaseFilter());
      }
      if (child instanceof AndFilter) {
        Set<Filter> children = new HashSet<>();
        for (Filter grandChild : ((AndFilter) child).getFilters()) {
          children.add(pushDownNot(new NotFilter(grandChild)));
        }
        return new OrFilter(children);
      }
      if (child instanceof OrFilter) {
        Set<Filter> children = new HashSet<>();
        for (Filter grandChild : ((OrFilter) child).getFilters()) {
          children.add(pushDownNot(new NotFilter(grandChild)));
        }
        return new AndFilter(children);
      }
    }

    if (current instanceof AndFilter) {
      Set<Filter> children = new HashSet<>();
      for (Filter child : ((AndFilter) current).getFilters()) {
        children.add(pushDownNot(child));
      }
      return new AndFilter(children);
    }

    if (current instanceof OrFilter) {
      Set<Filter> children = new HashSet<>();
      for (Filter child : ((OrFilter) current).getFilters()) {
        children.add(pushDownNot(child));
      }
      return new OrFilter(children);
    }
    return current;
  }

  // A helper function adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static Filter convertToCNFInternal(Filter current)
  {
    if (current instanceof NotFilter) {
      return new NotFilter(convertToCNFInternal(((NotFilter) current).getBaseFilter()));
    }
    if (current instanceof AndFilter) {
      Set<Filter> children = new HashSet<>();
      for (Filter child : ((AndFilter) current).getFilters()) {
        children.add(convertToCNFInternal(child));
      }
      return new AndFilter(children);
    }
    if (current instanceof OrFilter) {
      // a list of leaves that weren't under AND expressions
      List<Filter> nonAndList = new ArrayList<Filter>();
      // a list of AND expressions that we need to distribute
      List<Filter> andList = new ArrayList<Filter>();
      for (Filter child : ((OrFilter) current).getFilters()) {
        if (child instanceof AndFilter) {
          andList.add(child);
        } else if (child instanceof OrFilter) {
          // pull apart the kids of the OR expression
          nonAndList.addAll(((OrFilter) child).getFilters());
        } else {
          nonAndList.add(child);
        }
      }
      if (!andList.isEmpty()) {
        Set<Filter> result = new HashSet<>();
        generateAllCombinations(result, andList, nonAndList);
        return new AndFilter(result);
      }
    }
    return current;
  }

  // A helper function adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  @VisibleForTesting
  static Filter flatten(Filter root)
  {
    if (root instanceof BooleanFilter) {
      List<Filter> children = new ArrayList<>(((BooleanFilter) root).getFilters());
      // iterate through the index, so that if we add more children,
      // they don't get re-visited
      for (int i = 0; i < children.size(); ++i) {
        Filter child = flatten(children.get(i));
        // do we need to flatten?
        if (child.getClass() == root.getClass() && !(child instanceof NotFilter)) {
          boolean first = true;
          Set<Filter> grandKids = ((BooleanFilter) child).getFilters();
          for (Filter grandkid : grandKids) {
            // for the first grandkid replace the original parent
            if (first) {
              first = false;
              children.set(i, grandkid);
            } else {
              children.add(++i, grandkid);
            }
          }
        } else {
          children.set(i, child);
        }
      }
      // if we have a singleton AND or OR, just return the child
      if (children.size() == 1 && (root instanceof AndFilter || root instanceof OrFilter)) {
        return children.get(0);
      }

      if (root instanceof AndFilter) {
        return new AndFilter(children);
      } else if (root instanceof OrFilter) {
        return new OrFilter(children);
      }
    }
    return root;
  }

  // A helper function adapted from Apache Hive, see:
  // https://github.com/apache/hive/blob/branch-2.0/storage-api/src/java/org/apache/hadoop/hive/ql/io/sarg/SearchArgumentImpl.java
  private static void generateAllCombinations(
      Set<Filter> result,
      List<Filter> andList,
      List<Filter> nonAndList
  )
  {
    Set<Filter> children = ((AndFilter) andList.get(0)).getFilters();
    if (result.isEmpty()) {
      for (Filter child : children) {
        Set<Filter> a = new HashSet<>(nonAndList);
        a.add(child);
        result.add(new OrFilter(a));
      }
    } else {
      List<Filter> work = new ArrayList<>(result);
      result.clear();
      for (Filter child : children) {
        for (Filter or : work) {
          Set<Filter> a = new HashSet<>((((OrFilter) or).getFilters()));
          a.add(child);
          result.add(new OrFilter(a));
        }
      }
    }
    if (andList.size() > 1) {
      generateAllCombinations(result, andList.subList(1, andList.size()), nonAndList);
    }
  }

  // All functions below were basically adopted from Apache Calcite and modified to use them in Druid.
  // See https://github.com/apache/calcite/blob/branch-1.21/core/src/main/java/org/apache/calcite/rex/RexUtil.java#L1615
  // for original implementations.
  @VisibleForTesting
  static Filter pull(Filter rex) {
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
        if (removed != null) {
          list.add(removed);
        }
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

  private static List<Filter> pullList(Set<Filter> nodes) {
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

  private static Map<Filter, Filter> commonFactors(Set<Filter> nodes) {
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

  private static Filter removeFactor(Map<Filter, Filter> factors, Filter node) {
    List<Filter> list = new ArrayList<>();
    for (Filter operand : conjunctions(node)) {
      if (!factors.containsKey(operand)) {
        list.add(operand);
      }
    }
    return and(list);
  }

  private static Filter and(Iterable<? extends Filter> nodes) {
    return composeConjunction(nodes);
  }

  private static Filter or(Iterable<? extends Filter> nodes) {
    return composeDisjunction(nodes);
  }

  /** As {@link #composeConjunction(Iterable, boolean)} but never
   * returns null. */
  public static @Nonnull
  Filter composeConjunction(Iterable<? extends Filter> nodes) {
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
  public static Filter composeConjunction(Iterable<? extends Filter> nodes, boolean nullOnEmpty) {
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
  public static ImmutableList<Filter> flattenAnd(Iterable<? extends Filter> nodes) {
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

  private static void addAnd(ImmutableList.Builder<Filter> builder, Set<Filter> digests, Filter node) {
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
  @Nonnull public static Filter composeDisjunction(Iterable<? extends Filter> nodes) {
    final Filter e = composeDisjunction(nodes, false);
    return Objects.requireNonNull(e);
  }

  /**
   * Converts a collection of expressions into an OR,
   * optionally returning null if the list is empty.
   */
  public static Filter composeDisjunction(Iterable<? extends Filter> nodes, boolean nullOnEmpty) {
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
  public static ImmutableList<Filter> flattenOr(Iterable<? extends Filter> nodes) {
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

  private static void addOr(ImmutableList.Builder<Filter> builder, Set<Filter> set, Filter node) {
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
  public static List<Filter> conjunctions(Filter rexPredicate) {
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
  public static void decomposeConjunction(
      Filter rexPredicate,
      List<Filter> rexList) {
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

  private static boolean containsTrue(Iterable<Filter> nodes) {
    for (Filter node : nodes) {
      if (node instanceof TrueFilter) {
        return true;
      }
    }
    return false;
  }
}
