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

package org.apache.druid.sql.calcite.schema;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.Sarg;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Helpers for pushing a simple string-column predicate down from a {@code ProjectableFilterableTable}
 * system-table scan (e.g. {@code datasource} for sys.segments, {@code server}/{@code service_name} for
 * sys.server_properties) into that table's data source, so the scan materializes only the matching
 * rows instead of the whole cluster.
 *
 * <p>All extraction is best-effort. The filters are left in the planner's filter list, so Calcite
 * still applies them as a post-filter; correctness therefore holds even if extraction returns a
 * conservative (over-broad) set - the caller just does a bit of extra work - or {@code null} (no
 * constraint), which retains a full scan. The extractor never returns a set narrower than the true
 * constraint on the column, so pushing it down can never drop a matching row.
 */
final class SystemSchemaFilters
{
  private SystemSchemaFilters()
  {
  }

  /**
   * Extracts the finite set of exact string values that column {@code columnIndex} is constrained to
   * by the given filters (a top-level list is implicitly ANDed). Handles {@code col = 'x'},
   * {@code col IN (...)} (normalized by Calcite to SEARCH), OR-of-equalities, and arbitrarily nested
   * {@code AND}/{@code OR} - including a whole {@code WHERE} passed as a single {@code AND(...)}
   * RexCall (as Calcite's filter-scan rule may do), where any conjunct that does not constrain the
   * column (e.g. {@code is_active = 1}) is simply ignored.
   *
   * @return the bounded value set, or {@code null} when the column is not bounded to a finite set
   *         (no predicate, or a range/{@code LIKE}/{@code !=} predicate)
   */
  @Nullable
  static Set<String> extractColumnValues(List<RexNode> filters, int columnIndex)
  {
    return intersectConjuncts(filters, columnIndex);
  }

  /**
   * Multi-column variant of {@link #extractColumnValues(List, int)}: extracts a bounded value set for
   * each of {@code columnIndices} independently. A column with no usable constraint is absent from the
   * returned map (rather than mapped to {@code null}).
   */
  static Map<Integer, Set<String>> extractColumnValues(List<RexNode> filters, int... columnIndices)
  {
    final Map<Integer, Set<String>> result = new HashMap<>();
    for (final int columnIndex : columnIndices) {
      final Set<String> values = extractColumnValues(filters, columnIndex);
      if (values != null) {
        result.put(columnIndex, values);
      }
    }
    return result;
  }

  /**
   * AND semantics: intersect the value sets from the extractable conjuncts, ignoring conjuncts that
   * don't constrain the column. Returns {@code null} if no conjunct yields a value set.
   */
  @Nullable
  private static Set<String> intersectConjuncts(List<RexNode> conjuncts, int columnIndex)
  {
    Set<String> result = null;
    for (final RexNode conjunct : conjuncts) {
      final Set<String> values = extractColumnValues(conjunct, columnIndex);
      if (values != null) {
        result = (result == null) ? values : Sets.intersection(result, values).immutableCopy();
      }
    }
    return result;
  }

  @Nullable
  private static Set<String> extractColumnValues(RexNode node, int columnIndex)
  {
    if (!(node instanceof RexCall)) {
      return null;
    }
    final RexCall call = (RexCall) node;
    switch (call.getKind()) {
      case EQUALS:
        return equalsColumn(call, columnIndex);
      case AND:
        return intersectConjuncts(call.getOperands(), columnIndex);
      case OR:
        final Set<String> union = new HashSet<>();
        for (final RexNode operand : call.getOperands()) {
          final Set<String> values = extractColumnValues(operand, columnIndex);
          if (values == null) {
            // An un-extractable disjunct means we cannot bound the value set for this OR.
            return null;
          }
          union.addAll(values);
        }
        return union;
      case SEARCH:
        return searchColumn(call, columnIndex);
      default:
        return null;
    }
  }

  @Nullable
  private static Set<String> equalsColumn(RexCall call, int columnIndex)
  {
    final List<RexNode> ops = call.getOperands();
    if (ops.size() != 2) {
      return null;
    }
    final RexNode a = ops.get(0);
    final RexNode b = ops.get(1);
    final RexLiteral literal;
    if (isColumnRef(a, columnIndex) && b instanceof RexLiteral) {
      literal = (RexLiteral) b;
    } else if (isColumnRef(b, columnIndex) && a instanceof RexLiteral) {
      literal = (RexLiteral) a;
    } else {
      return null;
    }
    final String value = RexLiteral.stringValue(literal);
    return value == null ? null : ImmutableSet.of(value);
  }

  @Nullable
  private static Set<String> searchColumn(RexCall call, int columnIndex)
  {
    final List<RexNode> ops = call.getOperands();
    if (ops.size() != 2 || !isColumnRef(ops.get(0), columnIndex) || !(ops.get(1) instanceof RexLiteral)) {
      return null;
    }
    final Sarg<?> sarg = ((RexLiteral) ops.get(1)).getValueAs(Sarg.class);
    // Only exact-value sets (IN / OR-of-=) can bound the scan; ranges (>, <, LIKE) cannot.
    if (sarg == null || !sarg.isPoints()) {
      return null;
    }
    final Set<String> values = new HashSet<>();
    for (final Range<?> range : sarg.rangeSet.asRanges()) {
      final Object endpoint = range.lowerEndpoint();
      values.add(endpoint instanceof NlsString ? ((NlsString) endpoint).getValue() : String.valueOf(endpoint));
    }
    return values;
  }

  private static boolean isColumnRef(RexNode node, int columnIndex)
  {
    return node instanceof RexInputRef && ((RexInputRef) node).getIndex() == columnIndex;
  }
}
