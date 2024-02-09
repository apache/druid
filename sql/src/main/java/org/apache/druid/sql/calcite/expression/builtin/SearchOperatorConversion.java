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

package org.apache.druid.sql.calcite.expression.builtin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;
import com.google.common.collect.TreeRangeSet;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.Expressions;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.rel.VirtualColumnRegistry;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Operator that tests whether its left operand is included in the range of
 * values covered by search arguments.
 * This is the Druid version support for https://issues.apache.org/jira/browse/CALCITE-4173
 *
 * SEARCH operator tests whether an operand belongs to the range set.
 * A RexCall to SEARCH is converted back to SQL, typically an IN or OR.
 */
public class SearchOperatorConversion implements SqlOperatorConversion
{
  private static final RexBuilder REX_BUILDER = new RexBuilder(DruidTypeSystem.TYPE_FACTORY);

  @Override
  public SqlOperator calciteOperator()
  {
    return SqlStdOperatorTable.SEARCH;
  }

  @Nullable
  @Override
  public DimFilter toDruidFilter(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      @Nullable final VirtualColumnRegistry virtualColumnRegistry,
      final RexNode rexNode
  )
  {
    return Expressions.toFilter(
        plannerContext,
        rowSignature,
        virtualColumnRegistry,
        expandSearch((RexCall) rexNode, REX_BUILDER)
    );
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(
      final PlannerContext plannerContext,
      final RowSignature rowSignature,
      final RexNode rexNode
  )
  {
    return Expressions.toDruidExpression(
        plannerContext,
        rowSignature,
        expandSearch((RexCall) rexNode, REX_BUILDER)
    );
  }

  /**
   * Like {@link RexUtil#expandSearch(RexBuilder, RexProgram, RexNode)}, but with some enhancements:
   *
   * 1) Expands NOT IN (a.k.a. {@link Sarg#isComplementedPoints()} as !(a || b || c) rather than (!a && !b && !c),
   * which helps us convert it to an {@link InDimFilter} later.
   *
   * 2) Can generate nice conversions for complement-points even if the range is not *entirely* complement-points.
   */
  public static RexNode expandSearch(
      final RexCall call,
      final RexBuilder rexBuilder
  )
  {
    final RexNode arg = call.operands.get(0);
    final RexLiteral sargRex = (RexLiteral) call.operands.get(1);
    final Sarg<?> sarg = sargRex.getValueAs(Sarg.class);

    if (sarg.isAll() || sarg.isNone()) {
      return RexUtil.expandSearch(rexBuilder, null, call);
    }

    // RexNodes that represent ranges *including* the notInPoints. Later, the notInRexNode will be ANDed in so
    // those notInPoints can be removed.
    final List<RexNode> rangeRexNodes = new ArrayList<>();

    // Compute points that occur in the complement of the range set. These are "NOT IN" points.
    final List<Comparable> notInPoints;
    final RexNode notInRexNode;

    if (sarg.isPoints()) {
      // Short-circuit. Equivalent to the "else" branch logically, but involves less work.
      notInRexNode = null;
      notInPoints = Collections.emptyList();
    } else {
      final RangeSet<Comparable> complement = (RangeSet<Comparable>) sarg.rangeSet.complement();
      notInPoints = getPoints(complement);
      notInRexNode = makeIn(
          arg,
          ImmutableList.copyOf(
              Iterables.transform(
                  notInPoints,
                  point -> rexBuilder.makeLiteral(point, sargRex.getType(), true, true)
              )
          ),
          true,
          rexBuilder
      );
    }

    // Compute points that occur in the range set. These are the "IN" points.
    final List<Comparable> inPoints =
        sarg.pointCount == 0 ? Collections.emptyList() : (List<Comparable>) getPoints(sarg.rangeSet);
    final RexNode inRexNode = makeIn(
        arg,
        ImmutableList.copyOf(
            Iterables.transform(
                inPoints,
                point -> rexBuilder.makeLiteral(point, sargRex.getType(), true, true)
            )
        ),
        false,
        rexBuilder
    );
    if (inRexNode != null) {
      rangeRexNodes.add(inRexNode);
    }

    // Use RexUtil.sargRef to expand the rest of the sarg, if any.
    if (!sarg.isPoints() && !sarg.isComplementedPoints()) {
      // Remaining ranges, after separating out the "IN" and "NOT IN" points.
      // The "IN" points are excluded, and the "NOT IN" points are added back in.
      final RangeSet<Comparable> remainderRanges = TreeRangeSet.create();
      for (final Range<?> range : sarg.rangeSet.asRanges()) {
        if (!RangeSets.isPoint(range)) {
          remainderRanges.add((Range<Comparable>) range);
        }
      }

      for (final Comparable notInPoint : notInPoints) {
        remainderRanges.add(Range.singleton(notInPoint));
      }

      // Skip Range.all() and empty set, because they both mean we handled all existing points in the prior logic.
      // (If the range set started out as all or empty, we wouldn't have got this far, because of the check for
      // sarg.isAll() || sarg.isNone().)
      if (!remainderRanges.encloses(Range.all()) && !remainderRanges.isEmpty()) {
        final Sarg<?> remainderSarg = Sarg.of(RexUnknownAs.UNKNOWN, remainderRanges);
        final RexNode remainderRexNode =
            RexUtil.sargRef(rexBuilder, arg, remainderSarg, sargRex.getType(), RexUnknownAs.UNKNOWN);
        if (remainderRexNode.isA(SqlKind.OR)) {
          rangeRexNodes.addAll(((RexCall) remainderRexNode).getOperands());
        } else {
          rangeRexNodes.add(remainderRexNode);
        }
      }
    }

    RexNode retVal = null;

    if (!rangeRexNodes.isEmpty()) {
      retVal = RexUtil.composeDisjunction(rexBuilder, rangeRexNodes);
    }

    if (notInRexNode != null) {
      retVal = retVal == null ? notInRexNode : rexBuilder.makeCall(SqlStdOperatorTable.AND, retVal, notInRexNode);
    }

    if (sarg.nullAs == RexUnknownAs.TRUE) {
      retVal = rexBuilder.makeCall(
          SqlStdOperatorTable.OR,
          rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, arg),
          retVal
      );
    } else if (sarg.nullAs == RexUnknownAs.FALSE) {
      retVal = rexBuilder.makeCall(
          SqlStdOperatorTable.AND,
          rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, arg),
          retVal
      );
    }

    return retVal;
  }

  @Nullable
  public static RexNode makeIn(
      final RexNode arg,
      final List<RexNode> points,
      final boolean negate,
      final RexBuilder rexBuilder
  )
  {
    if (points.isEmpty()) {
      return null;
    } else if (points.size() == 1) {
      // x = a or X IS NULL
      final RexNode point = Iterables.getOnlyElement(points);
      if (RexUtil.isNullLiteral(point, true)) {
        return rexBuilder.makeCall(negate ? SqlStdOperatorTable.IS_NOT_NULL : SqlStdOperatorTable.IS_NULL, arg);
      } else {
        return rexBuilder.makeCall(negate ? SqlStdOperatorTable.NOT_EQUALS : SqlStdOperatorTable.EQUALS, arg, point);
      }
    } else {
      // x = a || x = b || x = c ...
      RexNode retVal = rexBuilder.makeCall(
          SqlStdOperatorTable.OR,
          ImmutableList.copyOf(
              Iterables.transform(
                  points,
                  point -> {
                    if (RexUtil.isNullLiteral(point, true)) {
                      return rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, arg);
                    } else {
                      return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, arg, point);
                    }
                  }
              )
          )
      );

      if (negate) {
        retVal = rexBuilder.makeCall(SqlStdOperatorTable.NOT, retVal);
      }

      return retVal;
    }
  }

  private static <T extends Comparable<T>> List<T> getPoints(final RangeSet<T> rangeSet)
  {
    final List<T> points = new ArrayList<>();
    for (final Range<T> range : rangeSet.asRanges()) {
      if (RangeSets.isPoint(range)) {
        points.add(range.lowerEndpoint());
      }
    }
    return points;
  }
}
