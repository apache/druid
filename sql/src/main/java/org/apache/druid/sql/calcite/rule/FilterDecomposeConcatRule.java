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
import com.google.common.collect.LinkedHashMultiset;
import com.google.common.collect.Multiset;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
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

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transform calls like [CONCAT(x, '-', y) = 'a-b'] => [x = 'a' AND y = 'b'].
 */
public class FilterDecomposeConcatRule extends RelOptRule implements SubstitutionRule
{
  public FilterDecomposeConcatRule()
  {
    super(operand(Filter.class, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call)
  {
    final Filter oldFilter = call.rel(0);
    final DecomposeConcatShuttle shuttle = new DecomposeConcatShuttle(
        oldFilter.getCluster().getRexBuilder());
    final RexNode newCondition = oldFilter.getCondition().accept(shuttle);

    //noinspection ObjectEquality
    if (newCondition != oldFilter.getCondition()) {
      call.transformTo(
          call.builder()
              .push(oldFilter.getInput())
              .filter(newCondition).build()
      );

      call.getPlanner().prune(oldFilter);
    }
  }

  /**
   * Shuttle that decomposes predicates on top of CONCAT calls.
   */
  static class DecomposeConcatShuttle extends RexShuttle
  {
    private final RexBuilder rexBuilder;

    DecomposeConcatShuttle(final RexBuilder rexBuilder)
    {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(final RexCall call)
    {
      final RexNode newCall;
      final boolean negate;

      if (call.isA(SqlKind.EQUALS) || call.isA(SqlKind.NOT_EQUALS)) {
        // Convert: [CONCAT(x, '-', y) = 'a-b']  => [x = 'a' AND y = 'b']
        // Convert: [CONCAT(x, '-', y) <> 'a-b'] => [NOT (x = 'a' AND y = 'b')]
        negate = call.isA(SqlKind.NOT_EQUALS);
        final RexNode lhs = call.getOperands().get(0);
        final RexNode rhs = call.getOperands().get(1);

        if (FlattenConcatRule.isNonTrivialStringConcat(lhs) && RexUtil.isLiteral(rhs, true)) {
          newCall = tryDecomposeConcatEquals((RexCall) lhs, rhs, rexBuilder);
        } else if (FlattenConcatRule.isNonTrivialStringConcat(rhs) && RexUtil.isLiteral(lhs, true)) {
          newCall = tryDecomposeConcatEquals((RexCall) rhs, lhs, rexBuilder);
        } else {
          newCall = null;
        }
      } else if ((call.isA(SqlKind.IS_NULL) || call.isA(SqlKind.IS_NOT_NULL))
                 && FlattenConcatRule.isNonTrivialStringConcat(Iterables.getOnlyElement(call.getOperands()))) {
        negate = call.isA(SqlKind.IS_NOT_NULL);
        final RexCall concatCall = (RexCall) Iterables.getOnlyElement(call.getOperands());
        if (NullHandling.sqlCompatible()) {
          // Convert: [CONCAT(x, '-', y) IS NULL]     => [x IS NULL OR y IS NULL]
          newCall = RexUtil.composeDisjunction(
              rexBuilder,
              Iterables.transform(
                  concatCall.getOperands(),
                  operand -> rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand)
              )
          );
        } else {
          // Treat [CONCAT(x, '-', y) IS NULL] as [CONCAT(x, '-', y) = '']
          newCall = tryDecomposeConcatEquals(concatCall, rexBuilder.makeLiteral(""), rexBuilder);
        }
      } else {
        negate = false;
        newCall = null;
      }

      if (newCall != null) {
        // Found a CONCAT comparison to decompose.
        return negate ? rexBuilder.makeCall(SqlStdOperatorTable.NOT, newCall) : newCall;
      } else {
        // Didn't find anything interesting. Visit children of original call.
        return super.visitCall(call);
      }
    }
  }

  /**
   * Convert [CONCAT(x, '-', y) = 'a-b'] => [x = 'a' AND y = 'b'].
   *
   * @param concatCall   the call to concat, i.e. CONCAT(x, '-', y)
   * @param matchRexNode the literal being matched, i.e. 'a-b'
   * @param rexBuilder   rex builder
   */
  @Nullable
  private static RexNode tryDecomposeConcatEquals(
      final RexCall concatCall,
      final RexNode matchRexNode,
      final RexBuilder rexBuilder
  )
  {
    final String matchValue = getAsString(matchRexNode);
    if (matchValue == null) {
      return null;
    }

    // We can decompose if all nonliterals are separated by literals, and if each literal appears in the matchValue
    // string exactly the number of times that it appears in the call to CONCAT. (In this case, the concatenation can
    // be unambiguously reversed.)
    final StringBuilder regexBuilder = new StringBuilder();
    final List<RexNode> nonLiterals = new ArrayList<>();

    // Order is important in literalCounter, since we look for later literals only after the first occurrences of
    // earlier literals. So, use LinkedHashMultiset to preserve order.
    final Multiset<String> literalCounter = LinkedHashMultiset.create();
    boolean expectLiteral = false; // If true, next operand must be a literal.
    for (int i = 0; i < concatCall.getOperands().size(); i++) {
      final RexNode operand = concatCall.getOperands().get(i);
      if (RexUtil.isLiteral(operand, true)) {
        final String operandValue = getAsString(operand);
        if (operandValue == null || operandValue.isEmpty()) {
          return null;
        }

        regexBuilder.append(Pattern.quote(operandValue));
        literalCounter.add(operandValue);
        expectLiteral = false;
      } else {
        if (expectLiteral) {
          return null;
        }

        nonLiterals.add(operand);
        regexBuilder.append("(.*)");
        expectLiteral = true;
      }
    }

    // Verify, using literalCounter, that each literal appears in the matchValue the correct number of times.
    int checkPos = 0;
    for (Multiset.Entry<String> entry : literalCounter.entrySet()) {
      final int occurrences = countOccurrences(matchValue.substring(checkPos), entry.getElement());
      if (occurrences > entry.getCount()) {
        // If occurrences > entry.getCount(), the match is ambiguous; consider concat(x, 'x', y) = '2x3x4'
        return null;
      } else if (occurrences < entry.getCount()) {
        return impossibleMatch(nonLiterals, rexBuilder);
      } else {
        // Literal N + 1 can be ignored if it appears before literal N, because it can't possibly match. Consider the
        // case where [CONCAT(a, ' (', b, 'x', ')') = 'xxx (2x4)']. This is unambiguous, because 'x' only appears once
        // after the first ' ('.
        checkPos = matchValue.indexOf(entry.getElement()) + 1;
      }
    }

    // Apply the regex to the matchValue to get the expected value of each non-literal.
    final Pattern regex = Pattern.compile(regexBuilder.toString(), Pattern.DOTALL);
    final Matcher matcher = regex.matcher(matchValue);
    if (matcher.matches()) {
      final List<RexNode> conditions = new ArrayList<>(nonLiterals.size());
      for (int i = 0; i < nonLiterals.size(); i++) {
        final RexNode operand = nonLiterals.get(i);
        conditions.add(
            rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS,
                operand,
                rexBuilder.makeLiteral(matcher.group(i + 1))
            )
        );
      }

      return RexUtil.composeConjunction(rexBuilder, conditions);
    } else {
      return impossibleMatch(nonLiterals, rexBuilder);
    }
  }

  /**
   * Generate an expression for the case where matching is impossible.
   *
   * This expression might be FALSE and might be UNKNOWN depending on whether any of the inputs are null. Use the
   * construct "x IS NULL AND UNKNOWN" for each arg x to CONCAT, which is FALSE if x is not null and UNKNOWN is x
   * is null. Then OR them all together, so the entire expression is FALSE if all args are not null, and UNKNOWN if any arg is null.
   *
   * @param nonLiterals non-literal arguments to CONCAT
   */
  private static RexNode impossibleMatch(final List<RexNode> nonLiterals, final RexBuilder rexBuilder)
  {
    if (NullHandling.sqlCompatible()) {
      // This expression might be FALSE and might be UNKNOWN depending on whether any of the inputs are null. Use the
      // construct "x IS NULL AND UNKNOWN" for each arg x to CONCAT, which is FALSE if x is not null and UNKNOWN if
      // x is null. Then OR them all together, so the entire expression is FALSE if all args are not null, and
      // UNKNOWN if any arg is null.
      final RexLiteral unknown =
          rexBuilder.makeNullLiteral(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN));
      return RexUtil.composeDisjunction(
          rexBuilder,
          Iterables.transform(
              nonLiterals,
              operand -> rexBuilder.makeCall(
                  SqlStdOperatorTable.AND,
                  rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, operand),
                  unknown
              )
          )
      );
    } else {
      return rexBuilder.makeLiteral(false);
    }
  }

  /**
   * Given a literal (which may be wrapped in a cast), remove the cast call (if any) and read it as a string.
   * Returns null if the rex can't be read as a string.
   */
  @Nullable
  private static String getAsString(final RexNode rexNode)
  {
    if (!SqlTypeFamily.STRING.contains(rexNode.getType())) {
      // We don't expect this to happen, since this method is used when reading from RexNodes that are expected
      // to be strings. But if it does (CONCAT operator that accepts non-strings?), return null so we skip the
      // optimization.
      return null;
    }

    // Get matchValue from the matchLiteral (remove cast call if any, then read as string).
    final RexNode matchLiteral = RexUtil.removeCast(rexNode);
    if (SqlTypeFamily.STRING.contains(matchLiteral.getType())) {
      return RexLiteral.stringValue(matchLiteral);
    } else if (SqlTypeFamily.NUMERIC.contains(matchLiteral.getType())) {
      return String.valueOf(RexLiteral.value(matchLiteral));
    } else {
      return null;
    }
  }

  /**
   * Count the number of occurrences of substring in string. Considers overlapping occurrences as multiple occurrences;
   * for example the string "--" is counted as appearing twice in "---".
   */
  private static int countOccurrences(final String string, final String substring)
  {
    int count = 0;
    int i = -1;

    while ((i = string.indexOf(substring, i + 1)) >= 0) {
      count++;
    }

    return count;
  }
}
