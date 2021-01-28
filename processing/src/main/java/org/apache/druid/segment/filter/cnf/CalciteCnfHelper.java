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
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.segment.filter.OrFilter;
import org.apache.druid.segment.filter.TrueFilter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * All functions in this class were basically adopted from Apache Calcite and modified to use them in Druid.
 * See https://github.com/apache/calcite/blob/branch-1.21/core/src/main/java/org/apache/calcite/rex/RexUtil.java#L1615
 * for original implementation.
 */
public class CalciteCnfHelper
{
  public static Filter pull(Filter rex)
  {
    final LinkedHashSet<Filter> operands;
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

  private static List<Filter> pullList(Collection<Filter> nodes)
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

  private static Map<Filter, Filter> commonFactors(Collection<Filter> nodes)
  {
    final Map<Filter, Filter> map = new LinkedHashMap<>();
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
    return Filters.maybeAnd(ImmutableList.copyOf(nodes)).orElse(TrueFilter.instance());
  }

  private static Filter or(Iterable<? extends Filter> nodes)
  {
    return Filters.maybeOr(ImmutableList.copyOf(nodes)).orElse(FalseFilter.instance());
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

  private CalciteCnfHelper()
  {
  }
}
