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

package org.apache.druid.sql.calcite.rule;

import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import org.apache.calcite.interpreter.BindableConvention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Sarg;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.SegmentsRollupRel;
import org.apache.druid.sql.calcite.schema.DatasourceSegmentStats;
import org.apache.druid.sql.calcite.schema.SegmentsRollup;
import org.apache.druid.sql.calcite.schema.SystemSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema.SegmentsRollupSource;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Rewrites a per-datasource aggregate over {@code sys.segments} -
 * {@code SELECT datasource, <aggregates> ... GROUP BY datasource} - into a {@link SegmentsRollupRel}
 * that reads the precomputed {@link SegmentsRollup} status sub-cube, so the query is O(#datasources)
 * instead of a full segment scan.
 *
 * <p>It recognizes any aggregate that is:
 * <ul>
 *   <li>{@code COUNT(*)}, {@code SUM}/{@code AVG}({@code size} | {@code num_rows} |
 *       {@code size*num_replicas}), or {@code MIN}/{@code MAX}({@code num_rows}); and</li>
 *   <li>optionally {@code FILTER}ed by an arbitrary boolean expression over the low-cardinality
 *       <em>status</em> columns ({@code is_active, is_published, is_available, is_realtime,
 *       is_overshadowed}, and {@code replication_factor} compared against 0). The filter is turned
 *       into a mask of cube cells by evaluating it over the finite status domain.</li>
 * </ul>
 *
 * <p>Anything it does not fully recognize - a different grouping, a filter touching a non-status
 * column, an unsupported measure/operator, or a rollup that is not yet populated - leaves the tree
 * unchanged, so the query falls back to the normal scan and stays correct. It runs as a Hep pre-pass
 * (before {@code AVG} is reduced to {@code SUM/COUNT}), so {@code AVG} is still a single call here.
 */
public class SegmentsRollupRule extends RelOptRule
{
  // Column positions in SystemSchema.SEGMENTS_SIGNATURE, derived from the signature (not hardcoded) so
  // they track the column order. Valid because the rule only fires on a table marked SegmentsRollupSource.
  private static final int COL_DATASOURCE = SegmentsRollupSource.COL_DATASOURCE;
  private static final int COL_SIZE = SegmentsRollupSource.COL_SIZE;
  private static final int COL_NUM_REPLICAS = SegmentsRollupSource.COL_NUM_REPLICAS;
  private static final int COL_NUM_ROWS = SegmentsRollupSource.COL_NUM_ROWS;
  private static final int COL_IS_ACTIVE = SegmentsRollupSource.COL_IS_ACTIVE;
  private static final int COL_IS_PUBLISHED = SegmentsRollupSource.COL_IS_PUBLISHED;
  private static final int COL_IS_AVAILABLE = SegmentsRollupSource.COL_IS_AVAILABLE;
  private static final int COL_IS_REALTIME = SegmentsRollupSource.COL_IS_REALTIME;
  private static final int COL_IS_OVERSHADOWED = SegmentsRollupSource.COL_IS_OVERSHADOWED;
  private static final int COL_REPLICATION_FACTOR = SegmentsRollupSource.COL_REPLICATION_FACTOR;

  /** Mask selecting every cell - used for an aggregate with no FILTER. */
  private static final boolean[] ALL_CELLS = allCells();

  private final SegmentsRollup rollup;
  private final AuthorizerMapper authorizerMapper;
  private final PlannerContext plannerContext;

  public SegmentsRollupRule(
      final SegmentsRollup rollup,
      final AuthorizerMapper authorizerMapper,
      final PlannerContext plannerContext
  )
  {
    super(
        operand(Aggregate.class, operand(Project.class, operand(TableScan.class, none()))),
        "SegmentsRollupRule"
    );
    this.rollup = rollup;
    this.authorizerMapper = authorizerMapper;
    this.plannerContext = plannerContext;
  }

  @Override
  public void onMatch(final RelOptRuleCall call)
  {
    final Aggregate aggregate = call.rel(0);
    final Project project = call.rel(1);
    final TableScan scan = call.rel(2);

    // Only fire once the rollup has been populated; otherwise fall back to the scan.
    if (rollup.getStatsIfReady() == null) {
      return;
    }
    // Only the sys.segments table (so the hardcoded column positions are valid).
    if (scan.getTable().unwrap(SystemSchema.SegmentsRollupSource.class) == null) {
      return;
    }
    // Exactly GROUP BY datasource.
    if (aggregate.getGroupSets().size() != 1 || aggregate.getGroupSet().cardinality() != 1) {
      return;
    }
    final int groupInputRef = aggregate.getGroupSet().nth(0);
    if (scanColumnOf(project.getProjects().get(groupInputRef)) != COL_DATASOURCE) {
      return;
    }

    // Map every aggregate call to a rollup extractor; bail (fall back) if any is unrecognized.
    final List<Function<DatasourceSegmentStats, Object>> extractors = new ArrayList<>();
    for (final AggregateCall aggCall : aggregate.getAggCallList()) {
      final Function<DatasourceSegmentStats, Object> extractor = mapAggCall(aggCall, project);
      if (extractor == null) {
        return;
      }
      extractors.add(extractor);
    }

    call.transformTo(
        new SegmentsRollupRel(
            aggregate.getCluster(),
            aggregate.getCluster().traitSetOf(BindableConvention.INSTANCE),
            aggregate.getRowType(),
            rollup,
            authorizerMapper,
            plannerContext,
            extractors
        )
    );
  }

  /**
   * Returns an extractor that computes this aggregate call from a {@link DatasourceSegmentStats}, or
   * null if the call is not answerable from the cube (so the whole query falls back).
   */
  @Nullable
  private static Function<DatasourceSegmentStats, Object> mapAggCall(final AggregateCall aggCall, final Project project)
  {
    if (aggCall.isDistinct()) {
      return null;
    }
    final boolean[] mask =
        aggCall.filterArg < 0 ? ALL_CELLS : computeMask(project.getProjects().get(aggCall.filterArg));
    if (mask == null) {
      return null;
    }
    final SqlKind kind = aggCall.getAggregation().getKind();
    final List<Integer> args = aggCall.getArgList();

    if (kind == SqlKind.COUNT && args.isEmpty()) {
      return stats -> stats.count(mask);
    }
    if (args.size() != 1) {
      return null;
    }
    final Measure measure = measureOf(project.getProjects().get(args.get(0)));
    switch (kind) {
      case SUM:
        switch (measure) {
          case SIZE:
            return stats -> nullIfEmpty(stats.count(mask), stats.sumSize(mask));
          case NUM_ROWS:
            return stats -> nullIfEmpty(stats.count(mask), stats.sumNumRows(mask));
          case REPLICATED_SIZE:
            return stats -> nullIfEmpty(stats.count(mask), stats.sumReplicatedSize(mask));
          default:
            return null;
        }
      case MIN:
        return measure == Measure.NUM_ROWS ? stats -> stats.minNumRows(mask) : null;
      case MAX:
        return measure == Measure.NUM_ROWS ? stats -> stats.maxNumRows(mask) : null;
      case AVG:
        switch (measure) {
          case NUM_ROWS:
            return stats -> avg(stats.count(mask), stats.sumNumRows(mask));
          case SIZE:
            return stats -> avg(stats.count(mask), stats.sumSize(mask));
          default:
            return null;
        }
      default:
        return null;
    }
  }

  /** SQL SUM(...) FILTER(...) is NULL (not 0) when no rows match. */
  @Nullable
  private static Long nullIfEmpty(final long count, final long sum)
  {
    return count == 0 ? null : sum;
  }

  /**
   * SQL {@code AVG} of an integral column is DOUBLE in Druid (e.g. {@code AVG(num_rows)} yields a
   * double, not truncated integer division), and is NULL when no rows match. Return a {@link Double}
   * so the value and type match the row type of the un-reduced {@code AVG} call this rel replaces.
   */
  @Nullable
  private static Double avg(final long count, final long sum)
  {
    return count == 0 ? null : (double) sum / count;
  }

  // -- FILTER predicate -> cube-cell mask, by evaluating over the finite status domain --

  /**
   * Evaluates the FILTER predicate against every cube cell's concrete status values, producing the
   * mask of selected cells. Returns null if the predicate touches a non-status column or uses an
   * operator/form the evaluator can't prove exact (so the query falls back).
   */
  @Nullable
  private static boolean[] computeMask(final RexNode predicate)
  {
    final boolean[] mask = new boolean[DatasourceSegmentStats.NUM_CELLS];
    try {
      for (int key = 0; key < DatasourceSegmentStats.NUM_CELLS; key++) {
        mask[key] = evalCell(predicate, key);
      }
    }
    catch (UnsupportedPredicateException e) {
      return null;
    }
    return mask;
  }

  private static boolean evalCell(final RexNode node, final int key)
  {
    if (node instanceof RexLiteral) {
      final Boolean literal = ((RexLiteral) node).getValueAs(Boolean.class);
      if (literal == null) {
        throw UnsupportedPredicateException.INSTANCE;
      }
      return literal;
    }
    if (!(node instanceof RexCall)) {
      throw UnsupportedPredicateException.INSTANCE;
    }
    final RexCall call = (RexCall) node;
    switch (call.getKind()) {
      case AND: {
        boolean result = true;
        for (final RexNode operand : call.getOperands()) {
          final boolean value = evalCell(operand, key); // evaluate all operands to detect unsupported ones
          result = result && value;
        }
        return result;
      }
      case OR: {
        boolean result = false;
        for (final RexNode operand : call.getOperands()) {
          final boolean value = evalCell(operand, key);
          result = result || value;
        }
        return result;
      }
      case NOT:
        return !evalCell(call.getOperands().get(0), key);
      // SQL FILTER(WHERE p) wraps its condition in IS TRUE(p). The status columns are non-null, so the
      // three-valued IS_* tests reduce to the operand's boolean value (or its negation).
      case IS_TRUE:
      case IS_NOT_FALSE:
        return evalCell(call.getOperands().get(0), key);
      case IS_FALSE:
      case IS_NOT_TRUE:
        return !evalCell(call.getOperands().get(0), key);
      case EQUALS:
      case NOT_EQUALS:
      case LESS_THAN:
      case GREATER_THAN:
      case LESS_THAN_OR_EQUAL:
      case GREATER_THAN_OR_EQUAL:
        return evalComparison(call, key);
      case SEARCH:
        return evalSearch(call, key);
      default:
        throw UnsupportedPredicateException.INSTANCE;
    }
  }

  private static boolean evalComparison(final RexCall call, final int key)
  {
    if (call.getOperands().size() != 2) {
      throw UnsupportedPredicateException.INSTANCE;
    }
    int col = scanColumnOf(call.getOperands().get(0));
    RexLiteral literal = asLiteral(call.getOperands().get(1));
    SqlKind kind = call.getKind();
    if (col < 0 || literal == null) {
      // Try reversed operand order (col on the right), flipping ordered comparisons.
      col = scanColumnOf(call.getOperands().get(1));
      literal = asLiteral(call.getOperands().get(0));
      if (col < 0 || literal == null) {
        throw UnsupportedPredicateException.INSTANCE;
      }
      kind = reverse(kind);
    }
    final Long value = literal.getValueAs(Long.class);
    if (value == null) {
      throw UnsupportedPredicateException.INSTANCE;
    }
    if (isBooleanStatusColumn(col)) {
      return compareLongs(booleanCellValue(col, key) ? 1L : 0L, value, kind);
    }
    if (col == COL_REPLICATION_FACTOR) {
      return evalReplication(kind, value, DatasourceSegmentStats.cellReplBucket(key));
    }
    throw UnsupportedPredicateException.INSTANCE;
  }

  private static boolean evalSearch(final RexCall call, final int key)
  {
    if (call.getOperands().size() != 2) {
      throw UnsupportedPredicateException.INSTANCE;
    }
    final int col = scanColumnOf(call.getOperands().get(0));
    final RexLiteral literal = asLiteral(call.getOperands().get(1));
    if (col < 0 || literal == null) {
      throw UnsupportedPredicateException.INSTANCE;
    }
    // Reduction commonly rewrites a status FILTER (e.g. `is_active = 1`) into a SEARCH over a Sarg, so
    // this path must handle the same predicates evalComparison does.
    if (isBooleanStatusColumn(col)) {
      // A Sarg of exact points over the boolean's {0,1} domain: the cell matches if its 0/1 value is one.
      final Sarg<?> sarg = literal.getValueAs(Sarg.class);
      if (sarg == null || !sarg.isPoints()) {
        throw UnsupportedPredicateException.INSTANCE;
      }
      final long cellValue = booleanCellValue(col, key) ? 1L : 0L;
      for (final Range<?> range : sarg.rangeSet.asRanges()) {
        if (((Number) range.lowerEndpoint()).longValue() == cellValue) {
          return true;
        }
      }
      return false;
    }
    // Only replication_factor range SEARCHes are supported, and only when equivalent to `= 0` or `> 0`
    // (a boundary at 0), so bucketing stays exact.
    if (col == COL_REPLICATION_FACTOR) {
      final Atom atom = sargToAtom(literal.getValueAs(Sarg.class));
      if (atom != null && atom.value == 0 && atom.kind == SqlKind.EQUALS) {
        return DatasourceSegmentStats.cellReplBucket(key) == DatasourceSegmentStats.REPL_ZERO;
      }
      if (atom != null && atom.value == 0 && atom.kind == SqlKind.GREATER_THAN) {
        return DatasourceSegmentStats.cellReplBucket(key) == DatasourceSegmentStats.REPL_POSITIVE;
      }
    }
    throw UnsupportedPredicateException.INSTANCE;
  }

  /**
   * replication_factor bucket predicate. Only thresholds at the {@code 0 | positive} boundary are
   * exact given the 3-way bucket, so anything else is unsupported (fall back). Unknown = -1.
   */
  private static boolean evalReplication(final SqlKind kind, final long value, final int bucket)
  {
    switch (kind) {
      case EQUALS:
        if (value == 0) {
          return bucket == DatasourceSegmentStats.REPL_ZERO;
        }
        break;
      case NOT_EQUALS:
        if (value == 0) {
          return bucket != DatasourceSegmentStats.REPL_ZERO;
        }
        break;
      case GREATER_THAN:
        if (value == 0) {
          return bucket == DatasourceSegmentStats.REPL_POSITIVE;
        }
        break;
      case GREATER_THAN_OR_EQUAL:
        if (value == 1) {
          return bucket == DatasourceSegmentStats.REPL_POSITIVE;
        } else if (value == 0) {
          return bucket != DatasourceSegmentStats.REPL_UNKNOWN;
        }
        break;
      case LESS_THAN:
        if (value == 1) {
          return bucket != DatasourceSegmentStats.REPL_POSITIVE;
        } else if (value == 0) {
          return bucket == DatasourceSegmentStats.REPL_UNKNOWN;
        }
        break;
      case LESS_THAN_OR_EQUAL:
        if (value == 0) {
          return bucket != DatasourceSegmentStats.REPL_POSITIVE;
        }
        break;
      default:
        break;
    }
    throw UnsupportedPredicateException.INSTANCE;
  }

  private static boolean isBooleanStatusColumn(final int col)
  {
    return col == COL_IS_ACTIVE
           || col == COL_IS_PUBLISHED
           || col == COL_IS_AVAILABLE
           || col == COL_IS_REALTIME
           || col == COL_IS_OVERSHADOWED;
  }

  private static boolean booleanCellValue(final int col, final int key)
  {
    // if/else rather than switch: the COL_* indices are derived from the row signature, not compile-time constants.
    if (col == COL_IS_ACTIVE) {
      return DatasourceSegmentStats.cellIsActive(key);
    } else if (col == COL_IS_PUBLISHED) {
      return DatasourceSegmentStats.cellIsPublished(key);
    } else if (col == COL_IS_AVAILABLE) {
      return DatasourceSegmentStats.cellIsAvailable(key);
    } else if (col == COL_IS_REALTIME) {
      return DatasourceSegmentStats.cellIsRealtime(key);
    } else if (col == COL_IS_OVERSHADOWED) {
      return DatasourceSegmentStats.cellIsOvershadowed(key);
    } else {
      throw UnsupportedPredicateException.INSTANCE;
    }
  }

  private static boolean compareLongs(final long left, final long right, final SqlKind kind)
  {
    switch (kind) {
      case EQUALS:
        return left == right;
      case NOT_EQUALS:
        return left != right;
      case LESS_THAN:
        return left < right;
      case GREATER_THAN:
        return left > right;
      case LESS_THAN_OR_EQUAL:
        return left <= right;
      case GREATER_THAN_OR_EQUAL:
        return left >= right;
      default:
        throw UnsupportedPredicateException.INSTANCE;
    }
  }

  private static SqlKind reverse(final SqlKind kind)
  {
    switch (kind) {
      case EQUALS:
      case NOT_EQUALS:
        return kind;
      case LESS_THAN:
        return SqlKind.GREATER_THAN;
      case GREATER_THAN:
        return SqlKind.LESS_THAN;
      case LESS_THAN_OR_EQUAL:
        return SqlKind.GREATER_THAN_OR_EQUAL;
      case GREATER_THAN_OR_EQUAL:
        return SqlKind.LESS_THAN_OR_EQUAL;
      default:
        throw UnsupportedPredicateException.INSTANCE;
    }
  }

  private enum Measure
  {
    SIZE, NUM_ROWS, REPLICATED_SIZE, UNKNOWN
  }

  private static Measure measureOf(final RexNode rawNode)
  {
    final RexNode node = unwrapCast(rawNode);
    final int col = scanColumnOf(node);
    if (col == COL_SIZE) {
      return Measure.SIZE;
    }
    if (col == COL_NUM_ROWS) {
      return Measure.NUM_ROWS;
    }
    // size * num_replicas (either operand order)
    if (node instanceof RexCall) {
      final RexCall rexCall = (RexCall) node;
      if (rexCall.getKind() == SqlKind.TIMES && rexCall.getOperands().size() == 2) {
        final int a = scanColumnOf(rexCall.getOperands().get(0));
        final int b = scanColumnOf(rexCall.getOperands().get(1));
        if ((a == COL_SIZE && b == COL_NUM_REPLICAS) || (a == COL_NUM_REPLICAS && b == COL_SIZE)) {
          return Measure.REPLICATED_SIZE;
        }
      }
    }
    return Measure.UNKNOWN;
  }

  /**
   * Converts a single-range {@link Sarg} on an integral column into an equivalent atom: a point
   * {@code [v, v]} becomes {@code = v}; a lower-bounded, upper-unbounded range becomes {@code > n}
   * (canonicalizing {@code >= v} to {@code > v - 1}, valid because the column is integral).
   */
  @Nullable
  private static Atom sargToAtom(@Nullable final Sarg<?> sarg)
  {
    if (sarg == null || sarg.rangeSet.asRanges().size() != 1) {
      return null;
    }
    final Range<?> range = sarg.rangeSet.asRanges().iterator().next();
    if (!range.hasLowerBound()) {
      return null;
    }
    final long lower = ((Number) range.lowerEndpoint()).longValue();
    if (range.hasUpperBound()) {
      final long upper = ((Number) range.upperEndpoint()).longValue();
      if (lower == upper
          && range.lowerBoundType() == BoundType.CLOSED
          && range.upperBoundType() == BoundType.CLOSED) {
        return new Atom(SqlKind.EQUALS, lower);
      }
      return null;
    }
    final long exclusiveLower = range.lowerBoundType() == BoundType.OPEN ? lower : lower - 1;
    return new Atom(SqlKind.GREATER_THAN, exclusiveLower);
  }

  private static int scanColumnOf(final RexNode node)
  {
    final RexNode unwrapped = unwrapCast(node);
    return unwrapped instanceof RexInputRef ? ((RexInputRef) unwrapped).getIndex() : -1;
  }

  @Nullable
  private static RexLiteral asLiteral(final RexNode node)
  {
    final RexNode unwrapped = unwrapCast(node);
    return unwrapped instanceof RexLiteral ? (RexLiteral) unwrapped : null;
  }

  /** Sees through CAST wrappers that type coercion may add around input refs and literals. */
  private static RexNode unwrapCast(RexNode node)
  {
    while (node.getKind() == SqlKind.CAST && node instanceof RexCall) {
      node = ((RexCall) node).getOperands().get(0);
    }
    return node;
  }

  private static boolean[] allCells()
  {
    final boolean[] mask = new boolean[DatasourceSegmentStats.NUM_CELLS];
    Arrays.fill(mask, true);
    return mask;
  }

  /** A single {@code column <op> literal} comparison, used to recognize replication_factor SEARCHes. */
  private static final class Atom
  {
    private final SqlKind kind;
    private final long value;

    private Atom(final SqlKind kind, final long value)
    {
      this.kind = kind;
      this.value = value;
    }
  }

  /** Thrown when a FILTER predicate can't be evaluated exactly over the status cube; triggers fallback. */
  private static final class UnsupportedPredicateException extends RuntimeException
  {
    private static final long serialVersionUID = 1L;
    private static final UnsupportedPredicateException INSTANCE = new UnsupportedPredicateException();

    private UnsupportedPredicateException()
    {
      super(null, null, false, false);
    }
  }
}
