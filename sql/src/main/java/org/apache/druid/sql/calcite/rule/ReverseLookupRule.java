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

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.sql.calcite.expression.builtin.MultiValueStringOperatorConversions;
import org.apache.druid.sql.calcite.expression.builtin.QueryLookupOperatorConversion;
import org.apache.druid.sql.calcite.expression.builtin.SearchOperatorConversion;
import org.apache.druid.sql.calcite.filtration.CollectComparisons;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Eliminates calls to {@link QueryLookupOperatorConversion#SQL_FUNCTION} by doing reverse-lookups.
 *
 * Considers reversing {@link SqlStdOperatorTable#IS_NULL} and operators that match
 * {@link #isBinaryComparison(RexNode)}. However, reversal is not done in all cases, such as when it would require
 * embedding the entire lookup into the query, or when the number of keys exceeds
 * {@link QueryContext#getInSubQueryThreshold()}.
 *
 * The heart of the class, where the reversal actually happens, is
 * {@link ReverseLookupShuttle.CollectReverseLookups#reverseLookup}. The rest of the rule is mainly about grouping
 * together as many LOOKUP calls as possible prior to attempting to reverse, since reversing a lookup may require
 * iteration of the lookup. We don't want to do that more often than necessary.
 */
public class ReverseLookupRule extends RelOptRule implements SubstitutionRule
{
  /**
   * Context parameter for tests, to allow us to confirm that this rule doesn't do too many calls to
   * {@link InDimFilter#optimize}. This is important because certain lookup implementations, most
   * prominently {@link MapLookupExtractor}, do a full iteration of the map for each call to
   * {@link LookupExtractor#unapplyAll}, which may be called by {@link InDimFilter#optimize}.
   */
  public static final String CTX_MAX_OPTIMIZE_COUNT = "maxOptimizeCountForDruidReverseLookupRule";

  /**
   * Context parameter to prevent creating too-large IN filters as a result of reverse lookups.
   */
  public static final String CTX_THRESHOLD = "sqlReverseLookupThreshold";

  /**
   * Context parameter for tests, to allow us to force the case where we avoid creating a bunch of ORs.
   */
  public static final int DEFAULT_THRESHOLD = 10000;

  private final PlannerContext plannerContext;

  public ReverseLookupRule(final PlannerContext plannerContext)
  {
    super(operand(LogicalFilter.class, any()));
    this.plannerContext = plannerContext;
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Filter filter = call.rel(0);

    final int maxOptimizeCount = plannerContext.queryContext().getInt(CTX_MAX_OPTIMIZE_COUNT, Integer.MAX_VALUE);
    final int maxInSize =
        Math.min(
            plannerContext.queryContext().getInSubQueryThreshold(),
            plannerContext.queryContext().getInt(CTX_THRESHOLD, DEFAULT_THRESHOLD)
        );
    final ReverseLookupShuttle reverseLookupShuttle = new ReverseLookupShuttle(
        plannerContext,
        filter.getCluster().getRexBuilder(),
        maxOptimizeCount,
        maxInSize
    );
    final RexNode newCondition = filter.getCondition().accept(reverseLookupShuttle);

    //noinspection ObjectEquality
    if (newCondition != filter.getCondition()) {
      call.transformTo(call.builder()
                           .push(filter.getInput())
                           .filter(newCondition).build());
      call.getPlanner().prune(filter);
    }
  }

  static class ReverseLookupShuttle extends RexShuttle
  {
    private final PlannerContext plannerContext;
    private final RexBuilder rexBuilder;

    /**
     * Maximum number of calls to {@link InDimFilter#optimizeLookup(InDimFilter, boolean, int)}. If this limit
     * is exceeded, we throw an error. Used in tests.
     */
    private final int maxOptimizeCount;

    /**
     * Maximum number of reversed keys to consider passing to {@link SearchOperatorConversion#makeIn}. If this limit
     * is exceeded, we don't rewrite the LOOKUP call. This mainly comes up when lots of keys map to the same value.
     */
    private final int maxInSize;

    /**
     * Tracks LOOKUP nodes that we have considered for reversal when they appear as children of AND or OR, so we don't
     * revisit them as we continue down the tree if reverse-lookup turned out to be impossible.
     */
    private final Set<RexNode> consideredAsChild = new HashSet<>();

    /**
     * Flipped by each call to {@link #visitNot(RexCall)}. See {@link NullHandling#useThreeValueLogic()}.
     */
    private boolean includeUnknown = false;

    /**
     * Tracker towards the limit {@link #maxOptimizeCount}.
     */
    private int optimizeCount = 0;

    public ReverseLookupShuttle(
        final PlannerContext plannerContext,
        final RexBuilder rexBuilder,
        final int maxOptimizeCount,
        final int maxInSize
    )
    {
      this.plannerContext = plannerContext;
      this.rexBuilder = rexBuilder;
      this.maxOptimizeCount = maxOptimizeCount;
      this.maxInSize = maxInSize;
    }

    @Override
    public RexNode visitCall(RexCall call)
    {
      if (call.getKind() == SqlKind.NOT) {
        return visitNot(call);
      } else if (call.getKind() == SqlKind.AND) {
        return visitAnd(call);
      } else if (call.getKind() == SqlKind.OR) {
        return visitOr(call);
      } else if (call.isA(SqlKind.SEARCH)) {
        return visitSearch(call);
      } else if ((call.isA(SqlKind.IS_NULL) || isBinaryComparison(call)) && !consideredAsChild.contains(call)) {
        return visitComparison(call);
      } else {
        return super.visitCall(call);
      }
    }

    /**
     * When we encounter NOT, flip {@link #includeUnknown} to ensure filters are optimized correctly.
     */
    private RexNode visitNot(final RexCall call)
    {
      includeUnknown = NullHandling.useThreeValueLogic() && !includeUnknown;
      final RexNode retVal = super.visitCall(call);
      includeUnknown = NullHandling.useThreeValueLogic() && !includeUnknown;
      return retVal;
    }

    /**
     * When we encounter OR, collect and reverse all LOOKUP calls that appear as children.
     */
    private RexNode visitOr(final RexCall call)
    {
      consideredAsChild.addAll(call.getOperands());

      final List<RexNode> newOperands =
          new CollectReverseLookups(call.getOperands(), rexBuilder).collect();

      //noinspection ObjectEquality
      if (newOperands != call.getOperands()) {
        return RexUtil.composeDisjunction(rexBuilder, newOperands);
      } else {
        return super.visitCall(call);
      }
    }

    /**
     * When we encounter AND, transform [!a && !b && c && d] to [!(a || b) && c && c], then run
     * similar logic as {@link #visitOr(RexCall)} on the [(a || b)] part.
     */
    private RexNode visitAnd(final RexCall call)
    {
      // Transform [!a && !b && c && d] to [!(a || b) && c && c]
      final List<RexNode> notOrs = new ArrayList<>();
      final List<RexNode> remainder = new ArrayList<>();

      for (final RexNode operand : call.getOperands()) {
        if (operand.isA(SqlKind.NOT)) {
          // Need to add the node beneath the NOT to consideredAsChild, because that's the one that would be visited
          // by a later visitCall. This is unlike NOT_EQUALS and IS_NOT_NULL, where the NOT_EQUALS and IS_NOT_NULL
          // nodes are visited directly.
          final RexNode nodeBeneathNot = Iterables.getOnlyElement(((RexCall) operand).getOperands());
          consideredAsChild.add(nodeBeneathNot);
          notOrs.add(nodeBeneathNot);
        } else if (operand.isA(SqlKind.NOT_EQUALS)) {
          consideredAsChild.add(operand);
          notOrs.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ((RexCall) operand).getOperands()));
        } else if (operand.isA(SqlKind.IS_NOT_NULL)) {
          consideredAsChild.add(operand);
          notOrs.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ((RexCall) operand).getOperands()));
        } else {
          remainder.add(operand);
        }
      }

      if (!notOrs.isEmpty()) {
        includeUnknown = !includeUnknown;
        final List<RexNode> newNotOrs =
            new CollectReverseLookups(notOrs, rexBuilder).collect();
        includeUnknown = !includeUnknown;

        //noinspection ObjectEquality
        if (newNotOrs != notOrs) {
          RexNode retVal =
              rexBuilder.makeCall(SqlStdOperatorTable.NOT, RexUtil.composeDisjunction(rexBuilder, newNotOrs));

          if (!remainder.isEmpty()) {
            remainder.add(retVal);
            retVal = rexBuilder.makeCall(SqlStdOperatorTable.AND, remainder);
          }

          return retVal;
        }
      }

      return super.visitCall(call);
    }

    /**
     * When we encounter SEARCH, expand it using {@link SearchOperatorConversion#expandSearch(RexCall, RexBuilder)}
     * and continue processing what lies beneath.
     */
    private RexNode visitSearch(final RexCall call)
    {
      final RexNode expanded = SearchOperatorConversion.expandSearch(call, rexBuilder);

      if (expanded instanceof RexCall) {
        final RexNode converted = visitCall((RexCall) expanded);
        //noinspection ObjectEquality
        if (converted != expanded) {
          return converted;
        }
      }

      // If no change, return the original SEARCH call, not the expanded one.
      return call;
    }

    /**
     * When we encounter a comparison, check if it is of the form {@code LOOKUP(...) = <literal>}, and if so, attempt
     * to do a reverse lookup to eliminate the {@code LOOKUP} call.
     */
    private RexNode visitComparison(final RexCall call)
    {
      return CollectionUtils.getOnlyElement(
          new CollectReverseLookups(Collections.singletonList(call), rexBuilder).collect(),
          ret -> new ISE("Expected to collect single node, got[%s]", ret)
      );
    }

    /**
     * Collect and reverse a set of lookups that appear as children to OR.
     */
    private class CollectReverseLookups
        extends CollectComparisons<RexNode, RexCall, RexNode, ReverseLookupKey>
    {
      private final RexBuilder rexBuilder;

      private CollectReverseLookups(
          final List<RexNode> orExprs,
          final RexBuilder rexBuilder
      )
      {
        super(orExprs);
        this.rexBuilder = rexBuilder;
      }

      @Nullable
      @Override
      protected Pair<RexCall, List<RexNode>> getCollectibleComparison(RexNode expr)
      {
        final RexCall asLookupComparison = getAsLookupComparison(expr);
        if (asLookupComparison != null) {
          return Pair.of(asLookupComparison, Collections.emptyList());
        } else {
          return null;
        }
      }

      @Nullable
      @Override
      protected ReverseLookupKey getCollectionKey(RexCall call)
      {
        final RexCall lookupCall = (RexCall) call.getOperands().get(0);
        final List<RexNode> lookupOperands = lookupCall.getOperands();
        final RexNode argument = lookupOperands.get(0);
        final String lookupName = RexLiteral.stringValue(lookupOperands.get(1));
        final String replaceMissingValueWith;

        if (lookupOperands.size() >= 3) {
          replaceMissingValueWith = NullHandling.emptyToNullIfNeeded(RexLiteral.stringValue(lookupOperands.get(2)));
        } else {
          replaceMissingValueWith = null;
        }

        final LookupExtractor lookup = plannerContext.getLookup(lookupName);

        if (lookup == null) {
          return null;
        }

        if (!lookup.isOneToOne()) {
          // For non-injective lookups, we can't reverse comparisons to the "replaceMissingValueWith" value.
          // If we detect these, ignore them by returning a null collection key. This isn't strictly required for
          // correctness, because InDimFilter#optimize won't optimize this case, and so our "reverseLookup" won't do
          // anything. But it's helpful to encourage grouping together filters that *can* be reversed when they
          // are peers to an irreversible one, and all are children of OR and AND.
          final boolean isComparisonAgainstReplaceMissingValueWith;

          if (replaceMissingValueWith == null) {
            // Optimization: avoid calling getMatchValues(call) when it's IS NULL.
            // Equivalent to the "else" case logically, but involves less work.
            isComparisonAgainstReplaceMissingValueWith = call.isA(SqlKind.IS_NULL);
          } else {
            isComparisonAgainstReplaceMissingValueWith = getMatchValues(call).contains(replaceMissingValueWith);
          }

          if (isComparisonAgainstReplaceMissingValueWith) {
            return null;
          }
        }

        final boolean multiValue =
            call.getOperator().equals(MultiValueStringOperatorConversions.CONTAINS.calciteOperator())
            || call.getOperator().equals(MultiValueStringOperatorConversions.OVERLAP.calciteOperator());
        final boolean negate = call.getKind() == SqlKind.NOT_EQUALS;
        return new ReverseLookupKey(argument, lookupName, replaceMissingValueWith, multiValue, negate);
      }

      @Override
      protected Set<String> getMatchValues(RexCall call)
      {
        if (call.isA(SqlKind.IS_NULL)) {
          return Collections.singleton(null);
        } else {
          // Compute the set of values that this comparison operator matches.
          // Note that MV_CONTAINS and MV_OVERLAP match nulls, but other comparison operators do not.
          // See "isBinaryComparison" for the set of operators we might encounter here.
          final RexNode matchLiteral = call.getOperands().get(1);
          final boolean matchNulls =
              call.getOperator().equals(MultiValueStringOperatorConversions.CONTAINS.calciteOperator())
              || call.getOperator().equals(MultiValueStringOperatorConversions.OVERLAP.calciteOperator());
          return toStringSet(matchLiteral, matchNulls);
        }
      }

      @Nullable
      @Override
      protected RexNode makeCollectedComparison(ReverseLookupKey reverseLookupKey, InDimFilter.ValuesSet matchValues)
      {
        final LookupExtractor lookupExtractor = plannerContext.getLookup(reverseLookupKey.lookupName);

        if (lookupExtractor != null) {
          final Set<String> reversedMatchValues = reverseLookup(
              lookupExtractor,
              reverseLookupKey.replaceMissingValueWith,
              matchValues,
              includeUnknown ^ reverseLookupKey.negate
          );

          if (reversedMatchValues != null) {
            return makeMatchCondition(reverseLookupKey, reversedMatchValues, rexBuilder);
          }
        }

        return null;
      }

      @Override
      protected RexNode makeAnd(List<RexNode> exprs)
      {
        throw new UnsupportedOperationException();
      }


      /**
       * Return expr as a lookup comparison, where the lookup operator is the left-hand side, a literal is
       * on the right-hand side, and the comparison is either {@link SqlStdOperatorTable#IS_NULL} or one of
       * the operators recognized by {@link #isBinaryComparison(RexNode)}.
       *
       * Returns null if expr does not actually match the above pattern.
       */
      @Nullable
      private RexCall getAsLookupComparison(final RexNode expr)
      {
        if (expr.isA(SqlKind.IS_NULL) && isLookupCall(((RexCall) expr).getOperands().get(0))) {
          return (RexCall) expr;
        }

        if (!isBinaryComparison(expr)) {
          return null;
        }

        final RexCall call = (RexCall) expr;

        RexNode lookupCall = call.getOperands().get(0);
        RexNode literal = call.getOperands().get(1);

        // Possibly swap arguments.
        if (literal instanceof RexCall && Calcites.isLiteral(lookupCall, true, true)) {
          // MV_CONTAINS doesn't allow swapping of arguments, since they aren't commutative.
          // See "isBinaryComparison" for the set of operators we might encounter here.
          if (call.getOperator().equals(MultiValueStringOperatorConversions.CONTAINS.calciteOperator())) {
            return null;
          }

          // Swap lookupCall, literal.
          RexNode tmp = lookupCall;
          lookupCall = literal;
          literal = tmp;
        }

        lookupCall = RexUtil.removeNullabilityCast(rexBuilder.getTypeFactory(), lookupCall);
        literal = RexUtil.removeNullabilityCast(rexBuilder.getTypeFactory(), literal);

        // Check that the call is of the form: LOOKUP(...) <op> <literal>
        if (isLookupCall(lookupCall) && Calcites.isLiteral(literal, true, true)) {
          return (RexCall) rexBuilder.makeCall(call.getOperator(), lookupCall, literal);
        } else {
          return null;
        }
      }

      /**
       * Perform a reverse lookup, leveraging logic from {@link InDimFilter#optimize(boolean)}.
       *
       * @param lookupExtractor         lookup object
       * @param replaceMissingValueWith third argument to LOOKUP function-- missing values are replaced with this.
       *                                By default, this is null.
       * @param matchValues             values to reverse-lookup
       * @param mayIncludeUnknown       whether the reverse-lookup should be done in a context where unknown matches
       *                                may be considered 'true' rather than 'false'.
       *
       * @return reverse lookup of "matchValues", or null if a reverse lookup can't be performed
       */
      @Nullable
      private Set<String> reverseLookup(
          final LookupExtractor lookupExtractor,
          @Nullable final String replaceMissingValueWith,
          final InDimFilter.ValuesSet matchValues,
          final boolean mayIncludeUnknown
      )
      {
        optimizeCount++;
        if (optimizeCount > maxOptimizeCount) {
          throw new ISE("Too many optimize calls[%s]", optimizeCount);
        }

        final InDimFilter filterToOptimize = new InDimFilter(
            "__dummy__",
            matchValues,
            new LookupExtractionFn(lookupExtractor, false, replaceMissingValueWith, null, true)
        );

        return InDimFilter.optimizeLookup(
            filterToOptimize,
            mayIncludeUnknown && NullHandling.useThreeValueLogic(),
            maxInSize
        );
      }

      private RexNode makeMatchCondition(
          final ReverseLookupKey reverseLookupKey,
          final Set<String> reversedMatchValues,
          final RexBuilder rexBuilder
      )
      {
        if (reversedMatchValues.isEmpty()) {
          return rexBuilder.makeLiteral(reverseLookupKey.negate);
        } else if (reverseLookupKey.multiValue) {
          // Use MV_CONTAINS or MV_OVERLAP.
          RexNode condition;
          if (reversedMatchValues.size() == 1) {
            condition = rexBuilder.makeCall(
                MultiValueStringOperatorConversions.CONTAINS.calciteOperator(),
                reverseLookupKey.arg,
                Iterables.getOnlyElement(stringsToRexNodes(reversedMatchValues, rexBuilder))
            );
          } else {
            condition = rexBuilder.makeCall(
                MultiValueStringOperatorConversions.OVERLAP.calciteOperator(),
                reverseLookupKey.arg,
                rexBuilder.makeCall(
                    SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR,
                    stringsToRexNodes(reversedMatchValues, rexBuilder)
                )
            );
          }

          if (reverseLookupKey.negate) {
            condition = rexBuilder.makeCall(SqlStdOperatorTable.NOT, condition);
          }

          return condition;
        } else {
          return SearchOperatorConversion.makeIn(
              reverseLookupKey.arg,
              stringsToRexNodes(reversedMatchValues, rexBuilder),
              reverseLookupKey.negate,
              rexBuilder
          );
        }
      }
    }
  }

  /**
   * Return a list of {@link RexNode} corresponding to the provided strings.
   */
  private static List<RexNode> stringsToRexNodes(final Iterable<String> strings, final RexBuilder rexBuilder)
  {
    return Lists.newArrayList(
        Iterables.transform(
            strings,
            s -> {
              if (s == null) {
                return rexBuilder.makeNullLiteral(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR));
              } else {
                return rexBuilder.makeLiteral(s);
              }
            }
        )
    );
  }

  /**
   * Whether a call is a binary (two-argument) comparison of the kind that
   * {@link ReverseLookupShuttle#visitComparison(RexCall)} can possibly handle.
   */
  private static boolean isBinaryComparison(final RexNode rexNode)
  {
    if (rexNode instanceof RexCall) {
      final RexCall call = (RexCall) rexNode;
      return call.getKind() == SqlKind.EQUALS
             || call.getKind() == SqlKind.NOT_EQUALS
             || call.getOperator().equals(MultiValueStringOperatorConversions.CONTAINS.calciteOperator())
             || call.getOperator().equals(MultiValueStringOperatorConversions.OVERLAP.calciteOperator());
    } else {
      return false;
    }
  }

  /**
   * Whether a given expression is a LOOKUP call.
   */
  static boolean isLookupCall(final RexNode expr)
  {
    return expr.isA(SqlKind.OTHER_FUNCTION)
           && ((RexCall) expr).getOperator().equals(QueryLookupOperatorConversion.SQL_FUNCTION);
  }

  /**
   * Convert a literal that we are doing an equality (or MV contains, overlaps) operation with into a set of values
   * to match. Returns null if the literal is not a string, string array, or null type.
   */
  @Nullable
  private static Set<String> toStringSet(final RexNode literal, final boolean matchNulls)
  {
    if (RexUtil.isNullLiteral(literal, true)) {
      return matchNulls ? Collections.singleton(null) : Collections.emptySet();
    } else if (SqlTypeFamily.STRING.contains(literal.getType())) {
      final String s = RexLiteral.stringValue(literal);
      return s != null || matchNulls ? Collections.singleton(s) : Collections.emptySet();
    } else if (literal.getType().getSqlTypeName() == SqlTypeName.ARRAY
               && SqlTypeFamily.STRING.contains(literal.getType().getComponentType())) {
      final Set<String> elements = new HashSet<>();
      for (final RexNode element : ((RexCall) literal).getOperands()) {
        final String s = RexLiteral.stringValue(element);
        if (s != null || matchNulls) {
          elements.add(s);
        }
      }
      return elements;
    } else {
      return null;
    }
  }

  /**
   * Collection key for {@link ReverseLookupShuttle.CollectReverseLookups}.
   */
  private static class ReverseLookupKey
  {
    private final RexNode arg;
    private final String lookupName;
    private final String replaceMissingValueWith;
    private final boolean multiValue;
    private final boolean negate;

    private ReverseLookupKey(
        final RexNode arg,
        final String lookupName,
        final String replaceMissingValueWith,
        final boolean multiValue,
        final boolean negate
    )
    {
      this.arg = arg;
      this.lookupName = lookupName;
      this.replaceMissingValueWith = replaceMissingValueWith;
      this.multiValue = multiValue;
      this.negate = negate;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ReverseLookupKey that = (ReverseLookupKey) o;
      return multiValue == that.multiValue
             && negate == that.negate
             && Objects.equals(arg, that.arg)
             && Objects.equals(lookupName, that.lookupName)
             && Objects.equals(replaceMissingValueWith, that.replaceMissingValueWith);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(arg, lookupName, replaceMissingValueWith, multiValue, negate);
    }
  }
}
